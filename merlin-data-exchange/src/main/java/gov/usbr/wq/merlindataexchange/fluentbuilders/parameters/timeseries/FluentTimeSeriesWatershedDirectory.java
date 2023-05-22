package gov.usbr.wq.merlindataexchange.fluentbuilders.parameters.timeseries;

import java.nio.file.Path;

public interface FluentTimeSeriesWatershedDirectory
{
    FluentTimeSeriesLogDirectory withWatershedDirectory(Path watershedDirectory);
}
