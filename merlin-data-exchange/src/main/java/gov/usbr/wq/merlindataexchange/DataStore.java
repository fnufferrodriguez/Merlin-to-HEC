package gov.usbr.wq.merlindataexchange;

import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;

class DataStore
{
    @JacksonXmlProperty(isAttribute = true, localName = "id")
    private String _id;

    String getId()
    {
        return _id;
    }

    void setId(String id)
    {
        this._id = id;
    }

}
