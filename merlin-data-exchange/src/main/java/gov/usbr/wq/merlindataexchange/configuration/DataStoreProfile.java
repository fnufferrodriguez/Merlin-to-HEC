package gov.usbr.wq.merlindataexchange.configuration;

import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlRootElement;

import java.util.List;


@JacksonXmlRootElement(localName = "datastore-profile")
public final class DataStoreProfile extends DataStore
{
    public static final String DEPTH = "Depth";
    @JacksonXmlProperty(localName = "constituents")
    private Constituents _constituents = new Constituents();

    public List<Constituent> getConstituents()
    {
        return _constituents.getConstituents();
    }

    public void setConstituents(Constituents constituents)
    {
        _constituents = constituents;
    }

    public Constituent getConstituentByParameter(String parameter)
    {
        Constituent retVal = null;
        for(Constituent constituent : getConstituents())
        {
            if(constituent.getParameter().equalsIgnoreCase(parameter))
            {
                retVal = constituent;
                break;
            }
        }
        return retVal;
    }
}
