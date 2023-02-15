package gov.usbr.wq.merlindataexchange;

import hec.io.StoreOption;

import java.nio.file.Path;
import java.time.Instant;
import java.util.Arrays;

public final class MerlinDataExchangeParameters
{
    private final String _username;
    private final char[] _password;
    private final Instant _start;
    private final Instant _end;
    private final StoreOption _storeOption;
    private final String _fPartOverride;
    private final Path _watershedDirectory;
    private final Path _logFileDirectory;

    public MerlinDataExchangeParameters(String username, char[] password, Path watershedDirectory, Path logFileDirectory, Instant start, Instant end,
                                        StoreOption storeOption, String fPartOverride)
    {
        _username = username;
        _password = password;
        _watershedDirectory = watershedDirectory;
        _logFileDirectory = logFileDirectory;
        _start = start;
        _end = end;
        _storeOption = storeOption;
        _fPartOverride = fPartOverride;
    }

    public String getUsername()
    {
        return _username;
    }

    public char[] getPassword()
    {
        return _password;
    }

    public Instant getStart()
    {
        return _start;
    }

    public Instant getEnd()
    {
        return _end;
    }

    public StoreOption getStoreOption()
    {
        return _storeOption;
    }

    public String getFPartOverride()
    {
        return _fPartOverride;
    }

    public Path getWatershedDirectory()
    {
        return _watershedDirectory;
    }

    public Path getLogFileDirectory()
    {
        return _logFileDirectory;
    }

    public void clearPassword()
    {
        Arrays.fill(_password, '\0');
    }
}
