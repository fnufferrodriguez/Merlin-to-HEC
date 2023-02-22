package gov.usbr.wq.merlindataexchange;

public final class MerlinDataExchangeLogBody
{
    private final StringBuilder _bodyLog = new StringBuilder();

    public synchronized void log(String logMessage)
    {
        _bodyLog.append(logMessage);
        _bodyLog.append("\n");
    }

    public String getLog()
    {
        return _bodyLog.toString();
    }
}
