package gov.usbr.wq.merlindataexchange.fluentbuilders.parameters.timeseries;

import gov.usbr.wq.merlindataexchange.parameters.AuthenticationParameters;

import java.util.List;

public interface FluentTimeSeriesAuthenticationParameters
{
    FluentTimeSeriesStoreOption withAuthenticationParameters(AuthenticationParameters authenticationParameters);
    FluentTimeSeriesStoreOption withAuthenticationParametersList(List<AuthenticationParameters> authenticationParametersList);
}
