package gov.usbr.wq.merlindataexchange.io;

import gov.usbr.wq.merlindataexchange.MerlinDataExchangeParameters;
import gov.usbr.wq.merlindataexchange.configuration.DataStore;
import rma.services.annotations.ServiceProvider;

@ServiceProvider(service = DataExchangeDao.class, position = 200, path = DataExchangeDao.LOOKUP_PATH
        + "/" + DssToMerlinDataExchangeDao.DSS_TO_MERLIN_DAO)
public final class DssToMerlinDataExchangeDao extends DataExchangeDao
{
    public static final String DSS_TO_MERLIN_DAO = "dsstomerlin/dao";
    @Override
    DataExchangeReader buildReader(DataStore dataStoreSource, MerlinDataExchangeParameters runtimeParameters)
    {
        return null;
    }

    @Override
    DataExchangeWriter buildWriter(DataStore dataStoreDestination, MerlinDataExchangeParameters runtimeParameters)
    {
        return null;
    }
}
