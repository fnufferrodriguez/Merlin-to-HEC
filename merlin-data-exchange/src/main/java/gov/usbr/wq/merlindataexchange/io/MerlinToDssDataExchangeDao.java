package gov.usbr.wq.merlindataexchange.io;

import com.rma.io.DssFileManager;
import com.rma.io.DssFileManagerImpl;
import gov.usbr.wq.merlindataexchange.MerlinDataExchangeParameters;
import gov.usbr.wq.merlindataexchange.configuration.DataStore;
import rma.services.annotations.ServiceProvider;

import java.nio.file.Path;
import java.nio.file.Paths;

@ServiceProvider(service = DataExchangeDao.class, position = 100, path = DataExchangeDao.LOOKUP_PATH
        + "/" + MerlinToDssDataExchangeDao.MERLIN_TO_DSS_DAO)
public final class MerlinToDssDataExchangeDao extends DataExchangeDao
{

    public static final String MERLIN_TO_DSS_DAO = "merlintodss/dao";

    @Override
    public DataExchangeReader buildReader(DataStore dataStoreSource, MerlinDataExchangeParameters parameters)
    {
        return new MerlinDataExchangeReader(dataStoreSource.getPath());
    }

    @Override
    public DataExchangeWriter buildWriter(DataStore dataStoreDestination, MerlinDataExchangeParameters parameters)
    {
        DssFileManager dssFileManager = DssFileManagerImpl.getDssFileManager();
        Path absolutePathToWriteTo = buildAbsoluteDssWritePath(dataStoreDestination.getPath(), parameters.getWatershedDirectory());
        return new DssDataExchangeWriter(dssFileManager, absolutePathToWriteTo);
    }

    public static Path buildAbsoluteDssWritePath(String filepath, Path watershedDir)
    {
        Path xmlFilePath = Paths.get(filepath);
        if(!xmlFilePath.isAbsolute() && filepath.contains("$WATERSHED"))
        {
            filepath = filepath.replace("$WATERSHED", watershedDir.toString());
            xmlFilePath = Paths.get(filepath);
        }
        return xmlFilePath;
    }
}
