package gov.usbr.wq.merlindataexchange.fluentbuilders.parameters.timeseries;

import hec.io.StoreOption;

public interface FluentTimeSeriesStoreOption
{
    FluentTimeSeriesParametersNonRequiredBuilder withStoreOption(StoreOption storeOption);
}
