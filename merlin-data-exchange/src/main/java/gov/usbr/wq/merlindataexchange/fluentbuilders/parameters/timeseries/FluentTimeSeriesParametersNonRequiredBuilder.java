package gov.usbr.wq.merlindataexchange.fluentbuilders.parameters.timeseries;

import java.time.Instant;

public interface FluentTimeSeriesParametersNonRequiredBuilder extends FluentTimeSeriesParametersBuilder
{
    FluentTimeSeriesParametersNonRequiredBuilder withFPartOverride(String fPartOverride);

    FluentTimeSeriesParametersNonRequiredBuilder withStart(Instant start);

    FluentTimeSeriesParametersNonRequiredBuilder withEnd(Instant end);
}
