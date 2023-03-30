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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.Logger;

import static java.util.stream.Collectors.toList;

@ServiceProvider(service = DataExchangeWriter.class, position = 200, path = DataExchangeWriter.LOOKUP_PATH
        + "/" + CsvDepthTempProfileWriter.CSV_PROFILE)
public final class CsvDepthTempProfileWriter implements DataExchangeWriter<DepthTempProfileSamples>
{
    private static final Logger LOGGER = Logger.getLogger(CsvDepthTempProfileWriter.class.getName());
    public static final String CSV_PROFILE = "csv-profile-depth-temp";
    public static final String MERLIN_TO_CSV_PROFILE_WRITE_SINGLE_THREAD_PROPERTY_KEY = "merlin.dataexchange.writer.csv.profile.singlethread";
    private final AtomicBoolean _loggedThreadProperty = new AtomicBoolean(false);

    @Override
    public void writeData(DepthTempProfileSamples depthTempProfileSamples, MeasureWrapper measure, MerlinParameters runtimeParameters,
                          DataStore destinationDataStore, MerlinExchangeCompletionTracker completionTracker, ProgressListener progressListener,
                          MerlinDataExchangeLogBody logFileLogger, AtomicBoolean isCancelled, AtomicReference<String> readDurationString)
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
                writeCsv(depthTempProfileSamples, csvWritePath, measure, completionTracker, logFileLogger, progressListener, readDurationString);
                writeEnd = Instant.now();
            }
        }
        else
        {
            writeStart = Instant.now();
            writeCsv(depthTempProfileSamples, csvWritePath, measure, completionTracker, logFileLogger, progressListener, readDurationString);
            writeEnd = Instant.now();
        }
        String successMsg = "Write to " + csvWritePath + " from " + measure.getSeriesString() + ReadWriteTimestampUtil.getDuration(writeStart, writeEnd);
        int percentCompleteAfterWrite = completionTracker.readWriteTaskCompleted();
        completionTracker.writeTaskCompleted();
        if(progressListener != null)
        {
            progressListener.progress(successMsg, ProgressListener.MessageType.GENERAL, percentCompleteAfterWrite);
        }
        logFileLogger.log(successMsg);
        LOGGER.config(() -> successMsg);

    }

    private void writeCsv(DepthTempProfileSamples depthTempProfileSamples, Path csvWritePath, MeasureWrapper measure, MerlinExchangeCompletionTracker completionTracker,
                          MerlinDataExchangeLogBody logFileLogger, ProgressListener progressListener, AtomicReference<String> readDurationString)
    {
        try
        {
            String progressMsg = "Read " + measure.getSeriesString() + " | Is processed: " + measure.isProcessed() + " | Values read: " + depthTempProfileSamples.getSamples().size()
                    + readDurationString;
            logFileLogger.log(progressMsg);
            int percentComplete = completionTracker.readWriteTaskCompleted();
            logProgress(progressListener, progressMsg, percentComplete);
            serializeDataToCsvFile(csvWritePath, depthTempProfileSamples);
        }
        catch (IOException e)
        {
            String failMsg = "Failed to write " +  measure.getSeriesString() + " to CSV!" + " Error: " + e.getMessage();
            if(progressListener != null)
            {
                progressListener.progress(failMsg, ProgressListener.MessageType.ERROR);
            }
            logFileLogger.log(failMsg);
            LOGGER.config(() -> failMsg);
        }
    }

    //scoped for unit testing
    void serializeDataToCsvFile(Path csvWritePath, DepthTempProfileSamples depthTempProfileSamples) throws IOException
    {
        CsvMapper mapper = buildCsvMapper();
        Map<String, String> headerMapping = depthTempProfileSamples.getHeadersMap();
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
        for(DepthTempProfileSample sample : depthTempProfileSamples.getSamples())
        {
            List<CsvDepthTempProfileSampleMeasurement> csvList = sample.getMeasurements().stream()
                    .map(CsvDepthTempProfileSampleMeasurement::new)
                    .collect(toList());
            try(Writer fileWriter = new FileWriter(csvWritePath.toFile(), true);
                SequenceWriter sequenceWriter = mapper.writerFor(CsvDepthTempProfileSampleMeasurement.class)
                    .with(contentSchema)
                    .writeValues(fileWriter))
            {
                sequenceWriter.writeAll(csvList);
            }
        }
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
        JavaTimeModule javaTimeModule = new JavaTimeModule();
        javaTimeModule.addSerializer(ZonedDateTime.class, new ZonedDateTimeSerializer());
        javaTimeModule.addDeserializer(ZonedDateTime.class, new ZonedDateTimeDeserializer());
        mapper.registerModule(javaTimeModule);
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
            gen.writeString(value.format(DateTimeFormatter.ISO_ZONED_DATE_TIME));
        }
    }

    private static class ZonedDateTimeDeserializer extends JsonDeserializer<ZonedDateTime>
    {
        @Override
        public ZonedDateTime deserialize(JsonParser p, DeserializationContext ctxt) throws IOException
        {
            String dateStr = p.getText().trim();
            return ZonedDateTime.parse(dateStr);
        }

    }

}
