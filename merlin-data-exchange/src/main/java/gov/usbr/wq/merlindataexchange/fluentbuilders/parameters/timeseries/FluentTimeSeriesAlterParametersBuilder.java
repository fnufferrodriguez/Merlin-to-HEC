package gov.usbr.wq.merlindataexchange.fluentbuilders.parameters.timeseries;

import gov.usbr.wq.merlindataexchange.parameters.AuthenticationParameters;
import hec.io.StoreOption;

import java.nio.file.Path;
import java.time.Instant;
import java.util.List;

public interface FluentTimeSeriesAlterParametersBuilder extends FluentTimeSeriesParametersBuilder
{
    FluentTimeSeriesAlterParametersBuilder withStoreOption(StoreOption storeOption);
    FluentTimeSeriesAlterParametersBuilder withFPartOverride(String fPartOverride);
    FluentTimeSeriesAlterParametersBuilder withWatershedDirectory(Path watershedDirectory);
    FluentTimeSeriesAlterParametersBuilder withLogFileDirectory(Path logDirectory);
    FluentTimeSeriesAlterParametersBuilder withAuthenticationParameters(AuthenticationParameters parameters);
    FluentTimeSeriesAlterParametersBuilder withAuthenticationParametersList(List<AuthenticationParameters> parameters);
    FluentTimeSeriesAlterParametersBuilder withStart(Instant start);
    FluentTimeSeriesAlterParametersBuilder withEnd(Instant end);
}
