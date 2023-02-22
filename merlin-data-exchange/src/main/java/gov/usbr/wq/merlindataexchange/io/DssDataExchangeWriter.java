package gov.usbr.wq.merlindataexchange.io;

import com.rma.io.DssFileManagerImpl;
import gov.usbr.wq.dataaccess.model.MeasureWrapper;
import gov.usbr.wq.merlindataexchange.MerlinDataExchangeLogBody;
import gov.usbr.wq.merlindataexchange.parameters.MerlinParameters;
import gov.usbr.wq.merlindataexchange.MerlinExchangeCompletionTracker;
import gov.usbr.wq.merlindataexchange.configuration.DataStore;
import hec.io.StoreOption;
import hec.io.TimeSeriesContainer;
import hec.ui.ProgressListener;
import rma.services.annotations.ServiceProvider;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Logger;

@ServiceProvider(service = DataExchangeWriter.class, position = 100, path = DataExchangeWriter.LOOKUP_PATH
        + "/" + DssDataExchangeWriter.DSS)
public final class DssDataExchangeWriter implements DataExchangeWriter
{
    public static final String DSS = "dss";
    private static final Logger LOGGER = Logger.getLogger(DssDataExchangeWriter.class.getName());
    private Path _dssWritePath;

    @Override
    public synchronized void writeData(TimeSeriesContainer timeSeriesContainer, MeasureWrapper measure, MerlinParameters runtimeParameters,
                                       MerlinExchangeCompletionTracker completionTracker, ProgressListener progressListener, MerlinDataExchangeLogBody logFileLogger, AtomicBoolean isCancelled)
    {
        StoreOption storeOption = runtimeParameters.getStoreOption();
        if(timeSeriesContainer != null && !isCancelled.get())
        {
            String seriesString = measure.getSeriesString();
            timeSeriesContainer.fileName = _dssWritePath.toString();
            int success = DssFileManagerImpl.getDssFileManager().writeTS(timeSeriesContainer, storeOption);
            if(success == 0)
            {
                String successMsg = "Successfully wrote Measure (" + seriesString+ ") to " + timeSeriesContainer.fullName;
                int percentComplete = completionTracker.writeTaskCompleted();
                if(progressListener != null)
                {
                    progressListener.progress(successMsg, ProgressListener.MessageType.IMPORTANT, percentComplete);
                }
                logFileLogger.log(successMsg);
                LOGGER.config(() -> successMsg);
            }
            else
            {
                String failMsg = "Failed to write Measure (" +  seriesString + ") to DSS! Error status code: " + success;
                if(progressListener != null)
                {
                    progressListener.progress(failMsg, ProgressListener.MessageType.ERROR);
                }
                logFileLogger.log(failMsg);
                LOGGER.config(() -> failMsg);
            }
        }
    }

    @Override
    public void initialize(DataStore dataStore, MerlinParameters parameters)
    {
        _dssWritePath = buildAbsoluteDssWritePath(dataStore.getPath(), parameters.getWatershedDirectory());
    }

    @Override
    public void close()
    {
        DssFileManagerImpl.getDssFileManager().close(_dssWritePath.toString());
    }

    @Override
    public String getDestinationPath()
    {
        return _dssWritePath.toString();
    }

    private static Path buildAbsoluteDssWritePath(String filepath, Path watershedDir)
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
