package gov.usbr.wq.merlindataexchange.io.wq;

import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import gov.usbr.wq.merlindataexchange.configuration.DataStore;
import gov.usbr.wq.merlindataexchange.io.DataExchangeLookupException;
import gov.usbr.wq.merlindataexchange.io.DataExchangeWriterFactory;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class TestMerlinDepthTempProfileSampleMeasurement
{
    @Test
    void testSerialization() throws DataExchangeLookupException, IOException
    {
        Path fileToWriteTo = getTestDirectory().resolve("testProfileSerialization.csv");
        Files.deleteIfExists(fileToWriteTo);
        DataStore dataStore = new DataStore();
        dataStore.setDataStoreType("csv-profile-depth-temp");
        CsvDepthTempProfileWriter csvWriter = (CsvDepthTempProfileWriter) DataExchangeWriterFactory.lookupWriter(dataStore);
        List<DepthTempProfileSampleMeasurement> data = new ArrayList<>();
        ZonedDateTime dateTime = ZonedDateTime.of(LocalDateTime.now(), ZoneId.of("GMT-08:00")).withNano(0);
        data.add(new DepthTempProfileSampleMeasurement(dateTime, 10.0, 1.0));
        data.add(new DepthTempProfileSampleMeasurement(dateTime, 11.0, null));
        Map<String, String> headerMapper = new LinkedHashMap<>();
        headerMapper.put("date", "date");
        headerMapper.put("Temp(C)", "temperature");
        headerMapper.put("Depth(ft)", "depth");
        DepthTempProfileSample sample = new DepthTempProfileSample(data);
        DepthTempProfileSamples samples = new DepthTempProfileSamples(100.0,
                Collections.singletonList(sample),
                headerMapper);
        csvWriter.serializeDataToCsvFile(fileToWriteTo, samples);
        List<CsvDepthTempProfileSampleMeasurement> deserializedDataList = deserializeCsv(csvWriter, fileToWriteTo, headerMapper);
        assertEquals(deserializedDataList.size(), data.size());
        for(int i=0; i < deserializedDataList.size(); i++)
        {
            CsvDepthTempProfileSampleMeasurement deserializedData = deserializedDataList.get(i);
            assertTrue(deserializedData.getDelegate().equals(data.get(i)));
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
