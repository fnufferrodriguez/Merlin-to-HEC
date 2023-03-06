package gov.usbr.wq.merlindataexchange.fluentbuilders;

import gov.usbr.wq.merlindataexchange.parameters.MerlinParameters;

public interface FluentFromExistingParameters
{
    FluentUpdatedAuthenticationParameters fromExistingParameters(MerlinParameters existingParameters);
}
