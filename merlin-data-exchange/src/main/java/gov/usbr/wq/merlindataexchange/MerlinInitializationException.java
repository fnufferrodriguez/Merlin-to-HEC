package gov.usbr.wq.merlindataexchange;

final class MerlinInitializationException extends Exception
{
    MerlinInitializationException(String errorMsg, Exception e)
    {
        super(errorMsg, e);
    }
}
