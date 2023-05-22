package gov.usbr.wq.merlindataexchange.fluentbuilders.parameters.profile;

import gov.usbr.wq.merlindataexchange.parameters.AuthenticationParameters;

import java.util.List;

public interface FluentProfileAuthenticationParameters
{
    FluentProfileParametersNonRequiredBuilder withAuthenticationParameters(AuthenticationParameters authenticationParameters);
    FluentProfileParametersNonRequiredBuilder withAuthenticationParametersList(List<AuthenticationParameters> authenticationParametersList);
}
