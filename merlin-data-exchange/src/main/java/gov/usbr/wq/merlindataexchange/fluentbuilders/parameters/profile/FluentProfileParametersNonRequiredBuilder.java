package gov.usbr.wq.merlindataexchange.fluentbuilders.parameters.profile;


import java.time.Instant;

public interface FluentProfileParametersNonRequiredBuilder extends FluentProfileParametersBuilder
{
    FluentProfileParametersNonRequiredBuilder withStart(Instant start);

    FluentProfileParametersNonRequiredBuilder withEnd(Instant end);

}
