package gov.usbr.wq.merlindataexchange.fluentbuilders.parameters.timeseries;

import java.nio.file.Path;

public interface FluentTimeSeriesLogDirectory
{
    FluentTimeSeriesAuthenticationParameters withLogFileDirectory(Path logDirectory);
}
