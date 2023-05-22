package gov.usbr.wq.merlindataexchange.fluentbuilders.parameters.timeseries;

import gov.usbr.wq.merlindataexchange.parameters.MerlinParameters;

public interface FluentTimeSeriesFromExistingParameters
{
    FluentTimeSeriesAlterParametersBuilder fromExistingParameters(MerlinParameters existingParameters);
}
