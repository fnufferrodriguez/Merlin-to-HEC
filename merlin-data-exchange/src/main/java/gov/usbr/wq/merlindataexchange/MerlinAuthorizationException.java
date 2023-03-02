package gov.usbr.wq.merlindataexchange;

import gov.usbr.wq.dataaccess.http.ApiConnectionInfo;
import gov.usbr.wq.dataaccess.http.HttpAccessException;

import java.net.UnknownHostException;

final class MerlinAuthorizationException extends Exception
{
    MerlinAuthorizationException(String errorMsg)
    {
        super(errorMsg);
    }
}
