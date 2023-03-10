package gov.usbr.wq.merlindataexchange.io;

import gov.usbr.wq.dataaccess.model.QualityVersionWrapper;
import gov.usbr.wq.merlindataexchange.DataExchangeCache;
import gov.usbr.wq.merlindataexchange.configuration.DataExchangeSet;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

public final class QualityVersionFromSetUtil
{
    private QualityVersionFromSetUtil()
    {
        throw new AssertionError("Utility class");
    }

    public static Optional<QualityVersionWrapper> getQualityVersionIdFromDataExchangeSet(DataExchangeSet dataExchangeSet, DataExchangeCache cache)
    {
        String qualityVersionNameFromSet = dataExchangeSet.getQualityVersionName();
        Integer qualityVersionIdFromSet = dataExchangeSet.getQualityVersionId();
        List<QualityVersionWrapper> qualityVersions = cache.getCachedQualityVersions();
        Optional<QualityVersionWrapper> retVal = qualityVersions.stream()
                .filter(qualityVersion -> qualityVersion.getQualityVersionName().equalsIgnoreCase(qualityVersionNameFromSet))
                .findFirst();
        if(!retVal.isPresent())
        {
            retVal = qualityVersions.stream()
                    .filter(qualityVersion ->  Objects.equals(qualityVersion.getQualityVersionID(), qualityVersionIdFromSet))
                    .findFirst();
        }
        return retVal;
    }
}
