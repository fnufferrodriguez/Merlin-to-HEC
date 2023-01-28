package gov.usbr.wq.merlindataexchange;

import com.fasterxml.jackson.dataformat.xml.XmlMapper;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

final class MerlinDataExchangeParser
{
    static DataExchangeConfiguration parseXmlFile(Path configFilepath) throws IOException, XMLStreamException
    {
        XMLInputFactory factory = XMLInputFactory.newFactory();
        XMLStreamReader streamReader = factory.createXMLStreamReader(Files.newInputStream(configFilepath));
        XmlMapper mapper = new XmlMapper();
        streamReader.next(); // to point to <root>
        return mapper.readValue(streamReader, DataExchangeConfiguration.class);
    }
}
