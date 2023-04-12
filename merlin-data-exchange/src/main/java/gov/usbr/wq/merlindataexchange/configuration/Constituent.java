package gov.usbr.wq.merlindataexchange.configuration;

import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;

public final class Constituent
{
    @JacksonXmlProperty(isAttribute = true, localName = "parameter")
    private String _parameter;

    @JacksonXmlProperty(isAttribute = true, localName = "unit")
    private String _unit;

    @JacksonXmlProperty(isAttribute = true, localName = "index")
    private Integer _index;

    public String getParameter()
    {
        return _parameter;
    }

    public void setParameter(String parameter)
    {
        _parameter = parameter;
    }

    public String getUnit()
    {
        return _unit;
    }

    public void setUnit(String unit)
    {
        _unit = unit;
    }

    public Integer getIndex()
    {
        return _index;
    }

    public void setIndex(Integer index)
    {
        _index = index;
    }
}
