package gov.usbr.wq.merlindataexchange.fluentbuilders;

import gov.usbr.wq.merlindataexchange.parameters.AuthenticationParameters;

import java.util.List;

public interface FluentUpdatedAuthenticationParameters
{
    FluentParamatersBuilder withUpdatedAuthenticationParameters(AuthenticationParameters parameters);
    FluentParamatersBuilder withUpdatedAuthenticationParametersList(List<AuthenticationParameters> parameters);
}
