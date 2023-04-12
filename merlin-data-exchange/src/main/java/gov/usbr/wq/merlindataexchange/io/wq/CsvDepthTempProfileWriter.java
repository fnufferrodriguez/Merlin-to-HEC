package gov.usbr.wq.merlindataexchange.io.wq;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.json.JsonReadFeature;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SequenceWriter;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import gov.usbr.wq.dataaccess.model.MeasureWrapper;
import gov.usbr.wq.merlindataexchange.MerlinDataExchangeLogBody;
import gov.usbr.wq.merlindataexchange.MerlinExchangeCompletionTracker;
import gov.usbr.wq.merlindataexchange.configuration.DataStore;
import gov.usbr.wq.merlindataexchange.io.CloseableReentrantLock;
import gov.usbr.wq.merlindataexchange.io.DataExchangeWriter;
import gov.usbr.wq.merlindataexchange.io.ReadWriteLockManager;
import gov.usbr.wq.merlindataexchange.io.ReadWriteTimestampUtil;
import gov.usbr.wq.merlindataexchange.parameters.MerlinParameters;
import hec.ui.ProgressListener;
import rma.services.annotations.ServiceProvider;

import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.Logger;

@ServiceProvider(service = DataExchangeWriter.class, position = 200, path = DataExchangeWriter.LOOKUP_PATH
        + "/" + CsvDepthTempProfileWriter.CSV)
public final class CsvDepthTempProfileWriter implements DataExchangeWriter<ProfileSample>
{
    private static final Logger LOGGER = Logger.getLogger(CsvDepthTempProfileWriter.class.getName());
    public static final String CSV = "csv";
    public static final String MERLIN_TO_CSV_PROFILE_WRITE_SINGLE_THREAD_PROPERTY_KEY = "merlin.dataexchange.writer.csv.profile.singlethread";
    private final AtomicBoolean _loggedThreadProperty = new AtomicBoolean(false);

    @Override
    public void writeData(ProfileSample depthTempProfileSample, MeasureWrapper measure, MerlinParameters runtimeParameters,
                          DataStore destinationDataStore, MerlinExchangeCompletionTracker completionTracker, ProgressListener progressListener,
                          MerlinDataExchangeLogBody logFileLogger, AtomicBoolean isCancelled, AtomicReference<String> readDurationString, AtomicReference<List<String>> seriesIds)
    {
        Path csvWritePath = Paths.get(getDestinationPath(destinationDataStore, runtimeParameters));
        boolean useSingleThreading = isSingleThreaded();
        Instant writeStart;
        Instant writeEnd;
        if(useSingleThreading)
        {
            try(CloseableReentrantLock lock = ReadWriteLockManager.getInstance().getCloseableLock().lockIt())
            {
                writeStart = Instant.now();
                writeCsv(depthTempProfileSample, csvWritePath, measure, completionTracker, logFileLogger, progressListener, readDurationString, seriesIds);
                writeEnd = Instant.now();
            }
        }
        else
        {
            writeStart = Instant.now();
            writeCsv(depthTempProfileSample, csvWritePath, measure, completionTracker, logFileLogger, progressListener, readDurationString, seriesIds);
            writeEnd = Instant.now();
        }
        String successMsg = "Write to " + csvWritePath + " from " + measure.getSeriesString() + ReadWriteTimestampUtil.getDuration(writeStart, writeEnd);
        //two write tasks
        completionTracker.readWriteTaskCompleted();
        int percentCompleteAfterWrite = completionTracker.readWriteTaskCompleted();
        completionTracker.writeTaskCompleted();
        if(progressListener != null)
        {
            progressListener.progress(successMsg, ProgressListener.MessageType.GENERAL, percentCompleteAfterWrite);
        }
        logFileLogger.log(successMsg);
        LOGGER.config(() -> successMsg);

    }

    private void writeCsv(ProfileSample depthTempProfileSample, Path csvWritePath, MeasureWrapper measure, MerlinExchangeCompletionTracker completionTracker,
                          MerlinDataExchangeLogBody logFileLogger, ProgressListener progressListener, AtomicReference<String> readDurationString, AtomicReference<List<String>> seriesIds)
    {
        if(depthTempProfileSample == null)
        {
            return;
        }
        List<String> seriesIdList = seriesIds.get();
        String seriesIdsString = String.join(",\n", seriesIdList);
        try
        {
            int totalSize = depthTempProfileSample.getConstituentDataList().stream()
                    .mapToInt(cdl -> cdl.getDataValues().size())
                    .sum();
            String progressMsg = "Read " + seriesIdsString + " | Is processed: "
                    + measure.isProcessed() + " | Values read: " + totalSize + readDurationString;
            logFileLogger.log(progressMsg);
            for(int i=1; i < depthTempProfileSample.getConstituentDataList().size(); i++)
            {
                completionTracker.readWriteTaskCompleted(); //1 read for every profile measure
            }
            int percentComplete = completionTracker.readWriteTaskCompleted(); // do the last read outside loop to get back completion percentage
            logProgress(progressListener, progressMsg, percentComplete);
            serializeDataToCsvFile(csvWritePath, depthTempProfileSample);
        }
        catch (IOException e)
        {
            String failMsg = "Failed to write " +  seriesIdsString + " to CSV!" + " Error: " + e.getMessage();
            if(progressListener != null)
            {
                progressListener.progress(failMsg, ProgressListener.MessageType.ERROR);
            }
            logFileLogger.log(failMsg);
            LOGGER.config(() -> failMsg);
        }
    }

    //scoped for unit testing
    void serializeDataToCsvFile(Path csvWritePath, ProfileSample depthTempProfileSample) throws IOException
    {
        CsvMapper mapper = buildCsvMapper();
        Map<String, String> headerMapping = buildHeaderMapping(depthTempProfileSample);
        CsvSchema.Builder schemaBuilder = CsvSchema.builder();
        headerMapping.keySet().forEach(schemaBuilder::addColumn);
        CsvSchema headerSchema = schemaBuilder.build().withHeader();
        schemaBuilder.clearColumns();
        headerMapping.keySet().stream()
                .map(headerMapping::get)
                .forEach(schemaBuilder::addColumn);
        CsvSchema contentSchema = buildContentSchema(headerMapping);
        mapper.writerFor(List.class)
                .with(headerSchema)
                .writeValue(csvWritePath.toFile(), Collections.emptyList());
        List<CsvDepthTempProfileSampleMeasurement> csvList = buildCsvDepthTempMeasurementForSample(depthTempProfileSample);
        try(Writer fileWriter = new FileWriter(csvWritePath.toFile(), true);
            SequenceWriter sequenceWriter = mapper.writerFor(CsvDepthTempProfileSampleMeasurement.class)
                    .with(contentSchema)
                    .writeValues(fileWriter))
        {
            sequenceWriter.writeAll(csvList);
        }
    }

    Map<String, String> buildHeaderMapping(ProfileSample sample)
    {
        Map<String, String> retVal = new LinkedHashMap<>();
        retVal.put("Date", "Date");
        Comparator<ProfileConstituentData> headerSorter = Comparator.comparing(o -> o.getParameter().toLowerCase().contains("temp") ? 0 : 1);
        sample.getConstituentDataList().sort(headerSorter); //put temp before depth. We can easily update sorter here to manipulate header order
        for(ProfileConstituentData profileConstituentData : sample.getConstituentDataList())
        {
            retVal.put(profileConstituentData.getParameter() + "(" + profileConstituentData.getUnit() + ")",
                    getSerializableFieldForParam(profileConstituentData.getParameter()));
        }
        return retVal;
    }

    private List<CsvDepthTempProfileSampleMeasurement> buildCsvDepthTempMeasurementForSample(ProfileSample sample)
    {
        List<CsvDepthTempProfileSampleMeasurement> retVal = new ArrayList<>();
        ZonedDateTime sampleDateTime = sample.getDateTime();
        ProfileConstituentData depthData = sample.getConstituentDataList().get(0);
        ProfileConstituentData tempData = sample.getConstituentDataList().get(1);
        if(depthData.getParameter().toLowerCase().contains("temp"))
        {
            tempData = sample.getConstituentDataList().get(0);
            depthData = sample.getConstituentDataList().get(1);
        }
        List<Double> depths = depthData.getDataValues();
        List<Double> temps = tempData.getDataValues();
        for(int i=0; i < depths.size(); i++)
        {
            retVal.add(new CsvDepthTempProfileSampleMeasurement(sampleDateTime, temps.get(i), depths.get(i)));
        }
        return retVal;
    }

    private String getSerializableFieldForParam(String parameter)
    {
        String retVal = "";
        parameter = parameter.toLowerCase();
        if(parameter.contains("temp"))
        {
            retVal = "Temperature"; //this is field specified in Csv POJO
        }
        else if(parameter.contains("depth"))
        {
            retVal = "Depth"; //this is field specified in Csv POJO
        }
        //Add in other fields in csv pojo here to do mapping.
        return retVal;
    }

    CsvSchema buildContentSchema(Map<String, String> headerMapping)
    {
        CsvSchema.Builder contentSchemaBuilder = CsvSchema.builder().setUseHeader(false);
        headerMapping.keySet().stream()
                .map(headerMapping::get)
                .forEach(contentSchemaBuilder::addColumn);
        return contentSchemaBuilder.build();
    }

    //scoped for testing
    CsvMapper buildCsvMapper()
    {
        CsvMapper mapper = new CsvMapper();
        mapper.registerModule(new JavaTimeModule());
        mapper.findAndRegisterModules();
        SimpleModule simpleModule = new SimpleModule();
        simpleModule.addSerializer(ZonedDateTime.class, new ZonedDateTimeSerializer());
        simpleModule.addDeserializer(ZonedDateTime.class, new ZonedDateTimeDeserializer());
        simpleModule.addSerializer(Double.class, new DoubleSerializer());
        simpleModule.addSerializer(Double[].class, new JsonSerializer<Double[]>()
        {
            @Override
            public void serialize(Double[] value, JsonGenerator gen, SerializerProvider serializers)
                    throws IOException
            {
                for (Double d : value)
                {
                    gen.writeNumber(d);
                }
            }
        });
        mapper.registerModule(simpleModule);
        mapper.configure(JsonReadFeature.ALLOW_UNESCAPED_CONTROL_CHARS.mappedFeature(), true);
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        return mapper;
    }

    private boolean isSingleThreaded()
    {
        String useSingleThreadString = System.getProperty(MERLIN_TO_CSV_PROFILE_WRITE_SINGLE_THREAD_PROPERTY_KEY);

        boolean useSingleThreading = false;
        if(useSingleThreadString != null)
        {
            useSingleThreading = Boolean.parseBoolean(useSingleThreadString);
            if(!_loggedThreadProperty.getAndSet(true))
            {
                boolean actualValue = useSingleThreading;
                LOGGER.log(Level.CONFIG, () -> "Merlin to csv-profile write with single thread using System Property " + MERLIN_TO_CSV_PROFILE_WRITE_SINGLE_THREAD_PROPERTY_KEY
                        + " set to: " + useSingleThreadString + ". Parsed value: " + actualValue);
            }
        }
        else if(!_loggedThreadProperty.getAndSet(true))
        {
            LOGGER.log(Level.INFO, () -> "Merlin to csv-profile write with single thread using System Property " + MERLIN_TO_CSV_PROFILE_WRITE_SINGLE_THREAD_PROPERTY_KEY
                    + " is not set. Defaulting to : False");
        }
        return useSingleThreading;
    }

    private void logProgress(ProgressListener progressListener, String message, int percentComplete)
    {
        if(progressListener != null)
        {
            progressListener.progress(message, ProgressListener.MessageType.GENERAL, percentComplete);
        }
    }

    private static class ZonedDateTimeSerializer extends JsonSerializer<ZonedDateTime>
    {
        @Override
        public void serialize(ZonedDateTime value, JsonGenerator gen, SerializerProvider provider) throws IOException
        {
            gen.writeString(value.format(DateTimeFormatter.ISO_OFFSET_DATE_TIME));
        }
    }

    private static class ZonedDateTimeDeserializer extends JsonDeserializer<ZonedDateTime>
    {
        @Override
        public ZonedDateTime deserialize(JsonParser p, DeserializationContext ctxt) throws IOException
        {
            String dateStr = p.getText().trim();
            return ZonedDateTime.parse(dateStr, DateTimeFormatter.ISO_OFFSET_DATE_TIME);
        }

    }

    protected static class DoubleSerializer extends JsonSerializer<Double>
    {
        protected DoubleSerializer()
        {
        }

        public void serialize(Double value, JsonGenerator gen, SerializerProvider serializers) throws IOException
        {
            if (value == null)
            {
                gen.writeNull();
            }
            else
            {
                int intValue = value.intValue();
                if (intValue == value)
                {
                    gen.writeNumber(intValue);
                }
                else
                {
                    gen.writeNumber(value);
                }
            }

        }
    }

}
