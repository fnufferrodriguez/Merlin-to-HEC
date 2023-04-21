package gov.usbr.wq.merlindataexchange.parameters;

import gov.usbr.wq.merlindataexchange.MerlinDataExchangeLogger;
import hec.io.StoreOption;

import java.nio.file.Path;
import java.time.Instant;
import java.util.List;

public final class MerlinTimeSeriesParameters extends MerlinParameters
{

    private final StoreOption _storeOption;
    private final String _fPartOverride;
    MerlinTimeSeriesParameters(Path watershedDirectory, Path logFileDirectory, Instant start, Instant end,
                     StoreOption storeOption, String fPartOverride, List<AuthenticationParameters> authenticationParameters)
    {
        super(watershedDirectory, logFileDirectory, start, end, authenticationParameters);
        _storeOption = storeOption;
        _fPartOverride = fPartOverride;
    }

    public StoreOption getStoreOption()
    {
        return _storeOption;
    }

    public String getFPartOverride()
    {
        return _fPartOverride;
    }

    @Override
    public void logAdditionalParameters(MerlinDataExchangeLogger merlinDataExchangeLogger)
    {
        int regularStoreRule = getStoreOption().getRegular();
        String fPartOverride = getFPartOverride();
        String storeRuleMsg = "Regular store rule: " + regularStoreRule;
        String fPartOverrideMsg = "DSS f-part override: " + (fPartOverride == null ? "Not Overridden" : fPartOverride);
        merlinDataExchangeLogger.logToHeader(storeRuleMsg);
        merlinDataExchangeLogger.logToHeader(fPartOverrideMsg);
    }
}
