package gov.usbr.wq.merlindataexchange.fluentbuilders.parameters.profile;

import gov.usbr.wq.merlindataexchange.parameters.AuthenticationParameters;

import java.nio.file.Path;
import java.time.Instant;
import java.util.List;

public interface FluentProfileAlterParametersBuilder extends FluentProfileParametersBuilder
{
    FluentProfileAlterParametersBuilder withWatershedDirectory(Path watershedDirectory);
    FluentProfileAlterParametersBuilder withLogFileDirectory(Path logDirectory);
    FluentProfileAlterParametersBuilder withAuthenticationParameters(AuthenticationParameters parameters);
    FluentProfileAlterParametersBuilder withAuthenticationParametersList(List<AuthenticationParameters> parameters);
    FluentProfileAlterParametersBuilder withStart(Instant start);
    FluentProfileAlterParametersBuilder withEnd(Instant end);

}
