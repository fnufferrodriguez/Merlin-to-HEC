package gov.usbr.wq.merlindataexchange.configuration;

import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlRootElement;

import java.util.List;


@JacksonXmlRootElement(localName = "datastore-profile")
public final class DataStoreProfile extends DataStore
{
    @JacksonXmlProperty(localName = "constituents")
    private Constituents _constituents;

    @JacksonXmlProperty(isAttribute = true, localName = "depth-parameter-name")
    private String _depthParameterName;

    public List<Constituent> getConstituents()
    {
        return _constituents.getConstituents();
    }

    public void setConstituents(Constituents constituents)
    {
        _constituents = constituents;
    }

    public String getDepthParameterName()
    {
        return _depthParameterName;
    }

    public void setDepthParameterName(String depthParameterName)
    {
        _depthParameterName = depthParameterName;
    }

    public Constituent getDepthConstituent()
    {
        Constituent retVal = null;
        for(Constituent constituent : getConstituents())
        {
            if(constituent.getParameter().equalsIgnoreCase(getDepthParameterName()))
            {
                retVal = constituent;
                break;
            }
        }
        return retVal;
    }
}
