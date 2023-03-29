package gov.usbr.wq.merlindataexchange.io;

import gov.usbr.wq.dataaccess.MerlinTimeSeriesDataAccess;
import gov.usbr.wq.dataaccess.http.ApiConnectionInfo;
import gov.usbr.wq.dataaccess.http.HttpAccessException;
import gov.usbr.wq.dataaccess.http.TokenContainer;
import gov.usbr.wq.dataaccess.model.DataWrapper;
import gov.usbr.wq.dataaccess.model.MeasureWrapper;
import gov.usbr.wq.dataaccess.model.QualityVersionWrapper;
import gov.usbr.wq.merlindataexchange.DataExchangeCache;
import gov.usbr.wq.merlindataexchange.MerlinDataExchangeLogBody;
import gov.usbr.wq.merlindataexchange.MerlinExchangeCompletionTracker;
import gov.usbr.wq.merlindataexchange.configuration.DataExchangeSet;
import gov.usbr.wq.merlindataexchange.configuration.DataStore;
import gov.usbr.wq.merlindataexchange.parameters.MerlinParameters;
import gov.usbr.wq.merlindataexchange.parameters.UsernamePasswordHolder;
import gov.usbr.wq.merlindataexchange.parameters.UsernamePasswordNotFoundException;
import hec.ui.ProgressListener;

import java.io.IOException;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.Logger;

public abstract class MerlinDataExchangeReader<T> implements DataExchangeReader<T>
{
    public static final String MERLIN = "merlin";
    private static final Logger LOGGER = Logger.getLogger(MerlinDataExchangeReader.class.getName());
    @Override
    public CompletableFuture<T> readData(DataExchangeSet dataExchangeSet, MerlinParameters runtimeParameters, DataStore sourceDataStore, DataExchangeCache cache,
                                                           MeasureWrapper measure, MerlinExchangeCompletionTracker completionTracker, ProgressListener progressListener, AtomicBoolean isCancelled,
                                                           MerlinDataExchangeLogBody logFileLogger, ExecutorService executorService, AtomicReference<String> readDurationString)
    {
        String merlinApiRoot = getSourcePath(sourceDataStore, runtimeParameters);
        Instant start = runtimeParameters.getStart();
        Instant end = runtimeParameters.getEnd();
        String fPartOverride = runtimeParameters.getFPartOverride();
        QualityVersionWrapper qualityVersion = QualityVersionFromSetUtil.getQualityVersionIdFromDataExchangeSet(dataExchangeSet, cache).orElse(null);
        String unitSystemToConvertTo = dataExchangeSet.getUnitSystem();
        Integer qualityVersionId = qualityVersion == null ? null : qualityVersion.getQualityVersionID();
        return CompletableFuture.supplyAsync(() ->
        {
            T retVal = null;
            try
            {
                UsernamePasswordHolder usernamePassword = runtimeParameters.getUsernamePasswordForUrl(merlinApiRoot);
                retVal = retrieveDataAsType(usernamePassword, start, end, merlinApiRoot, measure, qualityVersionId, progressListener, logFileLogger,
                        isCancelled, fPartOverride, unitSystemToConvertTo, completionTracker, measure.isProcessed(), readDurationString);
            }
            catch (UsernamePasswordNotFoundException e)
            {
                String errorMsg = "Failed to find username/password in parameters for URL: " + merlinApiRoot;
                logError(progressListener, logFileLogger, errorMsg, e);
            }
            return retVal;

        }, executorService);
    }

    private T retrieveDataAsType(UsernamePasswordHolder usernamePassword, Instant start, Instant end, String merlinApiRoot, MeasureWrapper measure, Integer qualityVersionId,
                                                   ProgressListener progressListener, MerlinDataExchangeLogBody logFileLogger,
                                                   AtomicBoolean isCancelled, String fPartOverride, String unitSystemToConvertTo, MerlinExchangeCompletionTracker completionTracker,
                                                   Boolean isProcessed, AtomicReference<String> readDurationString)
    {
        T retVal = null;
        try
        {
            TokenRegistry tokenRegistry = TokenRegistry.getRegistry();
            TokenContainer token = tokenRegistry.getToken(new ApiConnectionInfo(merlinApiRoot), usernamePassword.getUsername(), usernamePassword.getPassword());
            Instant readStart = Instant.now();
            DataWrapper data = retrieveData(start, end, merlinApiRoot,token, measure, qualityVersionId, progressListener, logFileLogger, isCancelled);
            Instant readEnd = Instant.now();
            readDurationString.set(ReadWriteTimestampUtil.getDuration(readStart, readEnd));
            if(data == null)
            {
                String errorMsg = "Failed to retrieve data for measure: " + measure.getSeriesString();
                if(progressListener != null)
                {
                    progressListener.progress(errorMsg, ProgressListener.MessageType.ERROR);
                }
                logFileLogger.log(errorMsg);
                LOGGER.config(() -> errorMsg);
            }
            else if(!isCancelled.get())
            {
                retVal = convertToType(data, unitSystemToConvertTo, fPartOverride, progressListener, logFileLogger, completionTracker, isProcessed, start, end, readDurationString);
            }
        }
        catch (HttpAccessException e)
        {
            String errorMsg = "Failed to authenticate user: " + usernamePassword.getUsername();
            logError(progressListener, logFileLogger, errorMsg, e);
        }
        return retVal;
    }

    protected abstract T convertToType(DataWrapper data, String unitSystemToConvertTo, String fPartOverride, ProgressListener progressListener,
                                       MerlinDataExchangeLogBody logFileLogger, MerlinExchangeCompletionTracker completionTracker, Boolean isProcessed,
                                       Instant start, Instant end, AtomicReference<String> readDurationString);


    private DataWrapper retrieveData(Instant start, Instant end, String merlinApiRoot, TokenContainer token, MeasureWrapper measure, Integer qualityVersionId,
                                     ProgressListener progressListener, MerlinDataExchangeLogBody logFileLogger, AtomicBoolean isCancelled)
    {
        MerlinTimeSeriesDataAccess access = new MerlinTimeSeriesDataAccess();
        DataWrapper retVal = null;
        if(!isCancelled.get())
        {
            try
            {
                retVal = access.getEventsBySeries(new ApiConnectionInfo(merlinApiRoot), token, measure, qualityVersionId, start, end);
            }
            catch (IOException | HttpAccessException ex)
            {
                logError(progressListener, logFileLogger, "Failed to retrieve data for measure with series string: " + measure.getSeriesString(), ex);
            }
        }
        return retVal;
    }

    @Override
    public String getSourcePath(DataStore sourceDataStore, MerlinParameters parameters)
    {
        return sourceDataStore.getPath();
    }

    void logProgressMessage(ProgressListener progressListener, String message, int nothingToWritePercentIncrement)
    {
        if (progressListener != null)
        {
            progressListener.progress(message, ProgressListener.MessageType.GENERAL, nothingToWritePercentIncrement);
        }
    }

    void logProgressMessage(ProgressListener progressListener, String msg)
    {
        if (progressListener != null)
        {
            progressListener.progress(msg, ProgressListener.MessageType.GENERAL);
        }
    }

    void logError(ProgressListener progressListener, MerlinDataExchangeLogBody logFileLogger, String errorMsg, Throwable e)
    {
        if(progressListener != null)
        {
            progressListener.progress(errorMsg, ProgressListener.MessageType.ERROR);
        }
        LOGGER.log(Level.CONFIG, e, () -> errorMsg);
        logFileLogger.log("Error occurred: " + errorMsg);
    }
}
