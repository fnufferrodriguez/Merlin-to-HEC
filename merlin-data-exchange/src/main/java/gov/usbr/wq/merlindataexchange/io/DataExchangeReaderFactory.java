package gov.usbr.wq.merlindataexchange.io;

import gov.usbr.wq.merlindataexchange.configuration.DataExchangeSet;
import gov.usbr.wq.merlindataexchange.configuration.DataStore;
import rma.util.lookup.Lookup;
import rma.util.lookup.Lookups;

public final class DataExchangeReaderFactory
{
    private DataExchangeReaderFactory()
    {
        throw new AssertionError("Factory Class");
    }

    public static DataExchangeReader<?> lookupReader(DataStore source, DataExchangeSet set) throws DataExchangeLookupException
    {
        String delimiter = "/";
        String lookupPath = DataExchangeReader.LOOKUP_PATH + delimiter + source.getDataStoreType() + delimiter + set.getDataType();
        Lookup lookup = Lookups.forPath(lookupPath);
        DataExchangeReader<?> retVal = lookup.lookup(DataExchangeReader.class);
        if(retVal == null)
        {
            throw new DataExchangeLookupException(source);
        }
        return retVal;
    }
}
