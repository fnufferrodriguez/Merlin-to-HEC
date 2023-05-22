package gov.usbr.wq.merlindataexchange.parameters;

import gov.usbr.wq.merlindataexchange.fluentbuilders.parameters.profile.FluentProfileAlterParametersBuilder;
import gov.usbr.wq.merlindataexchange.fluentbuilders.parameters.profile.FluentProfileAuthenticationParameters;
import gov.usbr.wq.merlindataexchange.fluentbuilders.parameters.profile.FluentProfileFromExistingParameters;
import gov.usbr.wq.merlindataexchange.fluentbuilders.parameters.profile.FluentProfileLogDirectory;
import gov.usbr.wq.merlindataexchange.fluentbuilders.parameters.profile.FluentProfileParametersNonRequiredBuilder;
import gov.usbr.wq.merlindataexchange.fluentbuilders.parameters.profile.FluentProfileParametersBuilder;
import gov.usbr.wq.merlindataexchange.fluentbuilders.parameters.profile.FluentProfileWatershedDirectory;

import java.nio.file.Path;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class MerlinProfileParametersBuilder implements FluentProfileWatershedDirectory, FluentProfileFromExistingParameters
{
    private Path _logDirectory;
    private List<AuthenticationParameters> _authenticationParametersList;
    private Path _watershedDirectory;
    private Instant _start;
    private Instant _end;

    @Override
    public FluentProfileLogDirectory withWatershedDirectory(Path watershedDirectory)
    {
        _watershedDirectory = Objects.requireNonNull(watershedDirectory, "Watershed directory must be specified, not null");
        return new MerlinProfileLogDirectory();
    }

    @Override
    public FluentProfileAlterParametersBuilder fromExistingParameters(MerlinParameters existingParameters)
    {
        if(existingParameters == null)
        {
            throw new IllegalArgumentException("Missing required parameters to copy from");
        }
        _watershedDirectory = existingParameters.getWatershedDirectory();
        _logDirectory = existingParameters.getLogFileDirectory();
        _start = existingParameters.getStart();
        _end = existingParameters.getEnd();
        _authenticationParametersList = existingParameters.getAuthenticationParameters();
        return new MerlinProfileAlteredParameters();
    }

    private class MerlinProfileLogDirectory implements FluentProfileLogDirectory
    {

        @Override
        public FluentProfileAuthenticationParameters withLogFileDirectory(Path logDirectory)
        {
            _logDirectory = Objects.requireNonNull(logDirectory, "Log file directory must be specified, not null");
            return new MerlinProfileAuthenticationParameters();
        }
    }

    private class MerlinProfileAuthenticationParameters implements FluentProfileAuthenticationParameters
    {
        @Override
        public FluentProfileParametersNonRequiredBuilder withAuthenticationParameters(AuthenticationParameters authenticationParameters)
        {
            if(authenticationParameters == null)
            {
                throw new IllegalArgumentException("AuthenticationParameters must be specified, not null");
            }
            _authenticationParametersList = Collections.singletonList(authenticationParameters);
            return new MerlinProfileParametersNonRequiredBuilder();
        }

        @Override
        public FluentProfileParametersNonRequiredBuilder withAuthenticationParametersList(List<AuthenticationParameters> authenticationParametersList)
        {
            if(authenticationParametersList == null || authenticationParametersList.isEmpty())
            {
                throw new IllegalArgumentException("AuthenticationParameters must be specified, not null");
            }
            _authenticationParametersList = authenticationParametersList;
            return new MerlinProfileParametersNonRequiredBuilder();
        }
    }

    private class MerlinProfileParametersNonRequiredBuilder extends MerlinProfileParametersBuild implements FluentProfileParametersNonRequiredBuilder
    {
        @Override
        public FluentProfileParametersNonRequiredBuilder withStart(Instant start)
        {
            _start = start;
            return this;
        }

        @Override
        public FluentProfileParametersNonRequiredBuilder withEnd(Instant end)
        {
            _end = end;
            return this;
        }
    }

    private class MerlinProfileParametersBuild implements FluentProfileParametersBuilder
    {
        @Override
        public MerlinProfileParameters build()
        {
            return new MerlinProfileParameters(_watershedDirectory, _logDirectory, _start, _end, _authenticationParametersList);
        }
    }

    private class MerlinProfileAlteredParameters extends MerlinProfileParametersBuild implements FluentProfileAlterParametersBuilder
    {

        @Override
        public FluentProfileAlterParametersBuilder withWatershedDirectory(Path watershedDirectory)
        {
            _watershedDirectory = Objects.requireNonNull(watershedDirectory, "Watershed directory must be specified, not null");
            return this;
        }

        @Override
        public FluentProfileAlterParametersBuilder withLogFileDirectory(Path logDirectory)
        {
            _logDirectory = Objects.requireNonNull(logDirectory, "Log file directory must be specified, not null");
            return this;
        }

        @Override
        public FluentProfileAlterParametersBuilder withAuthenticationParameters(AuthenticationParameters authenticationParameters)
        {
            if(authenticationParameters == null)
            {
                throw new IllegalArgumentException("AuthenticationParameters must be specified, not null");
            }
            return withAuthenticationParametersList(Collections.singletonList(authenticationParameters));
        }

        @Override
        public FluentProfileAlterParametersBuilder withAuthenticationParametersList(List<AuthenticationParameters> authenticationParameters)
        {
            if(authenticationParameters == null)
            {
                throw new IllegalArgumentException("AuthenticationParameters must be specified, not null");
            }
            _authenticationParametersList = authenticationParameters;
            return this;
        }

        @Override
        public FluentProfileAlterParametersBuilder withStart(Instant start)
        {
            _start = start;
            return this;
        }

        @Override
        public FluentProfileAlterParametersBuilder withEnd(Instant end)
        {
            _end = end;
            return this;
        }
    }
}
