package gov.usbr.wq.merlindataexchange.io;

import gov.usbr.wq.merlindataexchange.configuration.DataStore;
import rma.util.lookup.Lookup;
import rma.util.lookup.Lookups;

import java.util.logging.Logger;

public final class DataExchangeDaoFactory
{
    public static DataExchangeDao lookupDao(DataStore source, DataStore destination, Logger logFileLogger)
    {
        String delimeter = "/";
        String lookupPath = DataExchangeDao.LOOKUP_PATH + delimeter;
        //if going from merlin to dss, use MerlinToDssDataExchangeDao
        if("merlin".equalsIgnoreCase(source.getDataStoreType()) && "dss".equalsIgnoreCase(destination.getDataStoreType()))
        {
            lookupPath += MerlinToDssDataExchangeDao.MERLIN_TO_DSS_DAO;
        }
        else if("dss".equalsIgnoreCase(source.getDataStoreType()) && "merlin".equalsIgnoreCase(destination.getDataStoreType()))
        {
            //lookupPath for dss to merlin dao would go here if it existed
            lookupPath += DssToMerlinDataExchangeDao.DSS_TO_MERLIN_DAO;
        }
        String lookupPathDetermined = lookupPath;
        logFileLogger.info(() -> "Looking for Dao at: " + lookupPathDetermined);
        Lookup lookup = Lookups.forPath(lookupPathDetermined);
        return lookup.lookup(DataExchangeDao.class);
    }
}
