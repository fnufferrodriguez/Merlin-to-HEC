package gov.usbr.wq.merlindataexchange.parameters;

import gov.usbr.wq.merlindataexchange.MerlinDataExchangeLogger;

import java.nio.file.Path;
import java.time.Instant;
import java.util.List;

public final class MerlinProfileParameters extends MerlinParameters
{
    MerlinProfileParameters(Path watershedDirectory, Path logFileDirectory, Instant start, Instant end, List<AuthenticationParameters> authenticationParameters)
    {
        super(watershedDirectory, logFileDirectory, start, end, authenticationParameters);
    }

    @Override
    public void logAdditionalParameters(MerlinDataExchangeLogger logBody)
    {
        //no additional parameters to log
    }
}
