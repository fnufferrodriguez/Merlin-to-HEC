package gov.usbr.wq.merlindataexchange.io;

import com.rma.io.DssFileManager;
import gov.usbr.wq.merlindataexchange.MerlinDataExchangeParameters;
import gov.usbr.wq.merlindataexchange.MerlinExchangeDaoCompletionTracker;
import hec.io.StoreOption;
import hec.io.TimeSeriesContainer;
import hec.ui.ProgressListener;

import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Logger;

public final class DssDataExchangeWriter implements DataExchangeWriter
{

    private static final Logger LOGGER = Logger.getLogger(DssDataExchangeWriter.class.getName());
    private final DssFileManager _dssFileManager;
    private final Path _dssWritePath;

    public DssDataExchangeWriter(DssFileManager dssFileManager, Path dssWritePath)
    {
        _dssFileManager = dssFileManager;
        _dssWritePath = dssWritePath;
    }

    @Override
    public void writeData(TimeSeriesContainer timeSeriesContainer, String seriesPath, MerlinDataExchangeParameters runtimeParameters, MerlinExchangeDaoCompletionTracker completionTracker,
                          ProgressListener progressListener, Logger logFileLogger, AtomicBoolean isCancelled)
    {
        StoreOption storeOption = runtimeParameters.getStoreOption();
        if(timeSeriesContainer != null && !isCancelled.get())
        {
            String successfulConversionMsg = "Successfully converted Measure Measure (" + seriesPath + ") to timeseries! Writing timeseries to " + _dssWritePath;
            progressListener.progress(successfulConversionMsg, ProgressListener.MessageType.IMPORTANT);
            logFileLogger.info(() -> successfulConversionMsg);
            timeSeriesContainer.fileName = _dssWritePath.toString();
            int success = _dssFileManager.writeTS(timeSeriesContainer, storeOption);
            if(success == 0)
            {
                String successMsg = "Measure (" + seriesPath + ") successfully written to DSS! DSS Pathname: " + timeSeriesContainer.fullName;
                progressListener.progress(successMsg, ProgressListener.MessageType.IMPORTANT, completionTracker.writeTaskCompleted());
                logFileLogger.info(() -> successMsg);
                LOGGER.config(() -> successMsg);
            }
            else
            {
                String failMsg = "Failed to write Measure (" +  seriesPath + ") to DSS! Error status code: " + success;
                progressListener.progress(failMsg, ProgressListener.MessageType.ERROR);
                logFileLogger.severe(() -> failMsg);
                LOGGER.config(() -> failMsg);
            }
        }
    }

    @Override
    public void close()
    {
        _dssFileManager.close(_dssWritePath.toString());
    }

    @Override
    public String getDestinationPath()
    {
        return _dssWritePath.toString();
    }

}
