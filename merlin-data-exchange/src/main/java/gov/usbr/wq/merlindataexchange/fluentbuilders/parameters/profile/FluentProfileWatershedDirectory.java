package gov.usbr.wq.merlindataexchange.fluentbuilders.parameters.profile;

import java.nio.file.Path;

public interface FluentProfileWatershedDirectory
{
    FluentProfileLogDirectory withWatershedDirectory(Path watershedDirectory);
}
