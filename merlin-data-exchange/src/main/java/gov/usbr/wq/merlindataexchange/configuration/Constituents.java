package gov.usbr.wq.merlindataexchange.configuration;

import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;

import java.util.ArrayList;
import java.util.List;

public final class Constituents
{
    @JacksonXmlProperty(localName = "constituent")
    private List<Constituent> _constituents = new ArrayList<>();

    public List<Constituent> getConstituents()
    {
        return _constituents;
    }

    public void setConstituents(List<Constituent> constituents)
    {
        _constituents = constituents;
    }
}
