package gov.usbr.wq.merlindataexchange.io.wq;

import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import gov.usbr.wq.dataaccess.model.DataWrapper;
import gov.usbr.wq.dataaccess.model.DataWrapperBuilder;
import gov.usbr.wq.dataaccess.model.EventWrapper;
import gov.usbr.wq.dataaccess.model.EventWrapperBuilder;
import gov.usbr.wq.merlindataexchange.configuration.DataStore;
import gov.usbr.wq.merlindataexchange.io.DataExchangeLookupException;
import gov.usbr.wq.merlindataexchange.io.DataExchangeWriterFactory;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.TreeSet;

import static org.junit.jupiter.api.Assertions.assertEquals;

final class TestMerlinProfileSampleMeasurement
{
    @Test
    void testSerialization() throws DataExchangeLookupException, IOException
    {
        Path fileToWriteTo = getTestDirectory().resolve("testProfileSerialization.csv");
        Files.deleteIfExists(fileToWriteTo);
        DataStore dataStore = new DataStore();
        dataStore.setDataStoreType("csv-profile-depth-temp");
        CsvDepthTempProfileWriter csvWriter = (CsvDepthTempProfileWriter) DataExchangeWriterFactory.lookupWriter(dataStore);
        ZonedDateTime dateTime = ZonedDateTime.parse("2023-04-10T00:00:00-08:00", DateTimeFormatter.ISO_OFFSET_DATE_TIME);
        ZonedDateTime dateTime2 = ZonedDateTime.parse("2023-05-10T00:00:00-08:00", DateTimeFormatter.ISO_OFFSET_DATE_TIME);
        EventWrapper depthEvent = new EventWrapperBuilder()
                .withDate(dateTime.toOffsetDateTime())
                .withValue(1.0)
                .build();
        EventWrapper depthEvent2 = new EventWrapperBuilder()
                .withDate(dateTime2.toOffsetDateTime())
                .withValue(2.0)
                .build();
        NavigableSet<EventWrapper> navDepthMap = new TreeSet<>();
        navDepthMap.add(depthEvent);
        navDepthMap.add(depthEvent2);
        EventWrapper tempEvent = new EventWrapperBuilder()
                .withDate(dateTime.toOffsetDateTime())
                .withValue(10.0)
                .build();
        EventWrapper tempEvent2 = new EventWrapperBuilder()
                .withDate(dateTime2.toOffsetDateTime())
                .withValue(11.0)
                .build();
        NavigableSet<EventWrapper> navTempMap = new TreeSet<>();
        navTempMap.add(tempEvent);
        navTempMap.add(tempEvent2);
        DataWrapper tempDataWrapper = new DataWrapperBuilder()
                .withParameter("Temp-Water")
                .withUnits("C")
                .withEvents(navTempMap)
                .build();
        DataWrapper depthDataWrapper = new DataWrapperBuilder()
                .withParameter("Depth")
                .withUnits("ft")
                .withEvents(navDepthMap)
                .build();
        ProfileSample sample = new ProfileSample(depthDataWrapper, tempDataWrapper);
        csvWriter.serializeDataToCsvFile(fileToWriteTo, sample);
        Map<String, String> headerMapper = csvWriter.buildHeaderMapping(sample);
        List<CsvDepthTempProfileSampleMeasurement> deserializedDataList = deserializeCsv(csvWriter, fileToWriteTo, headerMapper);
        assertEquals(deserializedDataList.size(), sample.getTempData().getEvents().size());
        DataWrapper expectedDepthData = sample.getDepthData();
        DataWrapper expectedTempData = sample.getTempData();
        NavigableSet<EventWrapper> depthEvents = expectedDepthData.getEvents();
        NavigableSet<EventWrapper> tempEvents = expectedTempData.getEvents();
        for(int i=0; i < deserializedDataList.size(); i++)
        {
            CsvDepthTempProfileSampleMeasurement deserializedData = deserializedDataList.get(i);
            Double expectedDepth = new ArrayList<>(depthEvents).get(i).getValue();
            assertEquals(deserializedData.getDepth(), expectedDepth);
            Double expectedTemp = new ArrayList<>(tempEvents).get(i).getValue();
            assertEquals(deserializedData.getTemperature(), expectedTemp);
            ZonedDateTime expectedDateTime = new ArrayList<>(depthEvents).get(i).getDate();
            assertEquals(deserializedData.getDateTime(), expectedDateTime);

        }
    }

    private List<CsvDepthTempProfileSampleMeasurement> deserializeCsv(CsvDepthTempProfileWriter writer, Path csvFile, Map<String, String> headerMapper) throws IOException
    {
        CsvMapper csvMapper = writer.buildCsvMapper();
        CsvSchema csvSchema = writer.buildContentSchema(headerMapper).withSkipFirstDataRow(true);
        try(MappingIterator<CsvDepthTempProfileSampleMeasurement> mappingIterator = csvMapper.readerFor(CsvDepthTempProfileSampleMeasurement.class)
                .with(csvSchema)
                .readValues(csvFile.toFile()))
        {
            return mappingIterator.readAll();
        }
    }

    private Path getTestDirectory()
    {
        return Paths.get(System.getProperty("user.dir")).resolve("build/tmp");
    }

}
