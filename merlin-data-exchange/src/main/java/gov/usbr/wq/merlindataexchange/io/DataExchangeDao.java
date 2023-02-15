package gov.usbr.wq.merlindataexchange.io;

import gov.usbr.wq.dataaccess.jwt.TokenContainer;
import gov.usbr.wq.merlindataexchange.DataExchangeCache;
import gov.usbr.wq.merlindataexchange.MerlinDataExchangeParameters;
import gov.usbr.wq.merlindataexchange.MerlinExchangeDaoCompletionTracker;
import gov.usbr.wq.merlindataexchange.configuration.DataExchangeSet;
import gov.usbr.wq.merlindataexchange.configuration.DataStore;
import hec.ui.ProgressListener;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Logger;

public abstract class DataExchangeDao
{
    public static final String LOOKUP_PATH = "dataexchange/dao";
    private DataExchangeReader _reader;
    private DataExchangeWriter _writer;
    abstract DataExchangeReader buildReader(DataStore dataStoreSource, MerlinDataExchangeParameters runtimeParameters);
    abstract DataExchangeWriter buildWriter(DataStore dataStoreDestination, MerlinDataExchangeParameters runtimeParameters);
    public CompletableFuture<Void> exchangeData(DataExchangeSet dataExchangeSet, MerlinDataExchangeParameters runtimeParameters, DataExchangeCache cache,
                                                DataStore dataStoreSource, DataStore dataStoreDestination, String seriesPath, TokenContainer accessToken,
                                                MerlinExchangeDaoCompletionTracker completionTracker, ProgressListener progressListener, AtomicBoolean isCancelled, Logger logger,
                                                ExecutorService executorService)
    {
        CompletableFuture<Void> retVal = new CompletableFuture<>();
        if(!isCancelled.get())
        {
            if(_reader == null)
            {
                _reader = buildReader(dataStoreSource, runtimeParameters);
                logProgress(progressListener, logger, "Source path: " + _reader.getSourcePath());
            }
            if(_writer == null)
            {
                _writer = buildWriter(dataStoreDestination, runtimeParameters);
                logProgress(progressListener, logger, "Destination path: " + _writer.getDestinationPath());
            }
            retVal = _reader.readData(dataExchangeSet, runtimeParameters, cache, seriesPath, accessToken,
                            completionTracker, progressListener, isCancelled, logger, executorService)
                    .thenAcceptAsync(tsc -> _writer.writeData(tsc, seriesPath, runtimeParameters, completionTracker, progressListener, logger, isCancelled), executorService);
        }
        return retVal;
    }

    private void logProgress(ProgressListener progressListener, Logger logger, String message)
    {
        if(progressListener != null)
        {
            progressListener.progress(message, ProgressListener.MessageType.IMPORTANT);
        }
        logger.info(() -> message);
    }

    public void cleanUp()
    {
        _reader.close();
        _writer.close();
        _reader = null;
        _writer = null;
    }


}
