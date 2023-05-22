package gov.usbr.wq.merlindataexchange;

public final class NoEventsException extends Exception
{
    public NoEventsException(String output, String seriesId)
    {
        super("No data to write to " + output + " from " + seriesId);
    }
}
