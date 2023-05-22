package gov.usbr.wq.merlindataexchange.fluentbuilders.parameters.profile;

import gov.usbr.wq.merlindataexchange.parameters.MerlinParameters;

public interface FluentProfileFromExistingParameters
{
    FluentProfileAlterParametersBuilder fromExistingParameters(MerlinParameters existingParameters);
}
