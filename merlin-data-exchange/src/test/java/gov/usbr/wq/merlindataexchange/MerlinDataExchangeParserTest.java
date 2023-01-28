package gov.usbr.wq.merlindataexchange;

import org.junit.jupiter.api.Test;

import javax.xml.stream.XMLStreamException;
import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

final class MerlinDataExchangeParserTest
{
    @Test
    void testParseXmlFile() throws IOException, XMLStreamException
    {
        Path mockXml = getMockXml();
        DataExchangeConfiguration dataExchangeConfig = MerlinDataExchangeParser.parseXmlFile(mockXml);
        assertNotNull(dataExchangeConfig);
        TimeSeriesDataExchangeSet tsDataExchangeSet = dataExchangeConfig.getTimeSeriesDataExchangeSet();
        assertNotNull(tsDataExchangeSet);
        DataStoreRef merlinRef = tsDataExchangeSet.getDataStoreRefMerlin();
        DataStoreRef localDssRef = tsDataExchangeSet.getDataStoreRefLocalDss();
        DataStoreMerlin dataStoreMerlin = (DataStoreMerlin) dataExchangeConfig.getDataStoreByRef(merlinRef);
        DataStoreLocalDss dataStoreLocalDss = (DataStoreLocalDss) dataExchangeConfig.getDataStoreByRef(localDssRef);
        assertEquals(dataExchangeConfig.getDataStoreMerlin(), dataStoreMerlin);
        assertEquals(dataExchangeConfig.getDataStoreLocalDss(), dataStoreLocalDss);
        assertEquals("www.grabdata2.com", dataStoreMerlin.getId());
        assertEquals("https://www.grabdata2.com/merlinwebservice", dataStoreMerlin.getUrl());
        assertEquals("wat", dataStoreLocalDss.getId());
        assertEquals("$WATERSHED/shared/filename.dss", dataStoreLocalDss.getFilepath());
        assertEquals(80, tsDataExchangeSet.getTemplateId());
        assertEquals(0, tsDataExchangeSet.getQualityVersionId());
        assertEquals("SI", tsDataExchangeSet.getUnitSystem());
        assertEquals(0.0, tsDataExchangeSet.getSortOrder());
    }

    private Path getMockXml() throws IOException
    {
        String resource = "gov/usbr/wq/merlindataexchange/merlin_mock_dx.xml";
        URL resourceUrl = getClass().getClassLoader().getResource(resource);
        if (resourceUrl == null)
        {
            throw new IOException("Failed to get resource: " + resource);
        }
        return new File(resourceUrl.getFile()).toPath();
    }
}
