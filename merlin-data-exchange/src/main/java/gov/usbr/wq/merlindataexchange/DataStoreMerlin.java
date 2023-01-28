package gov.usbr.wq.merlindataexchange;

import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;

final class DataStoreMerlin extends DataStore
{
    @JacksonXmlProperty(isAttribute = true, localName = "url")
    private String _url;

    String getUrl()
    {
        return _url;
    }

    void setUrl(String url)
    {
        _url = url;
    }
}
