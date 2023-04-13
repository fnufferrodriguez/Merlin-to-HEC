package gov.usbr.wq.merlindataexchange.io.wq;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

final class TestMerlinProfileSampleMeasurement
{
    @Test
    void testSerialization() throws IOException
    {
        Path fileToWriteTo = getTestDirectory().resolve("testProfileSerialization.csv");
        Files.deleteIfExists(fileToWriteTo);
        ZonedDateTime dateTime = ZonedDateTime.parse("2023-04-10T00:00:00-08:00", DateTimeFormatter.ISO_OFFSET_DATE_TIME);
        List<Double> temps = Arrays.asList(10.0, 11.0);
        List<Double> depths = Arrays.asList(1.0, 2.0);
        String tempUnits = "C";
        String depthUnits = "ft";
        List<ProfileConstituent> profileConstituentData = new ArrayList<>();
        profileConstituentData.add(new ProfileConstituent("Depth", depths, depthUnits));
        profileConstituentData.add(new ProfileConstituent("Temp-Water", temps, tempUnits));
        ProfileSample sample = new ProfileSample(dateTime, profileConstituentData);
        CsvProfileObjectMapper.serializeDataToCsvFile(fileToWriteTo, Collections.singletonList(sample));
        ProfileSample returnedSample = CsvProfileObjectMapper.deserializeDataFromCsv(fileToWriteTo).get(0);
        assertEquals(sample, returnedSample);
    }

    private Path getTestDirectory()
    {
        return Paths.get(System.getProperty("user.dir")).resolve("build/tmp");
    }

}
