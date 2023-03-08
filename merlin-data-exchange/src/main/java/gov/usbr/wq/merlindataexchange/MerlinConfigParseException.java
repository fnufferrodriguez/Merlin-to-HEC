package gov.usbr.wq.merlindataexchange;

import com.fasterxml.jackson.databind.exc.UnrecognizedPropertyException;
import org.xml.sax.SAXParseException;

import java.nio.file.Path;

public final class MerlinConfigParseException extends Exception
{
    public MerlinConfigParseException(Path configFile, Exception e)
    {
        super(buildMessage(configFile, e), e);
    }

    public MerlinConfigParseException(Path configFile, String missingFieldMessage)
    {
        super("Failed to read configuration file: " + configFile + ":\n" + missingFieldMessage);
    }

    private static String buildMessage(Path configFile, Exception e)
    {
        String retVal = "Failed to read configuration file: " + configFile;
        if(e instanceof UnrecognizedPropertyException)
        {
            retVal += ":\n" + ((UnrecognizedPropertyException)e).getOriginalMessage()
                    + ".\nExpected one of the following fields: " +((UnrecognizedPropertyException) e).getKnownPropertyIds();
        }
        if(e instanceof SAXParseException)
        {
            SAXParseException saxException = (SAXParseException) e;
            retVal += ":\n" + saxException.getMessage() + "\n"
                    + "line: " + saxException.getLineNumber() + ", column: " + saxException.getColumnNumber();
        }
        else
        {
            retVal += ":\n" + e.getMessage();
        }
        return retVal;
    }
}
