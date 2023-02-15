package gov.usbr.wq.merlindataexchange.io;

import gov.usbr.wq.dataaccess.MerlinTimeSeriesDataAccess;
import gov.usbr.wq.dataaccess.http.HttpAccessException;
import gov.usbr.wq.dataaccess.jwt.TokenContainer;
import gov.usbr.wq.dataaccess.model.DataWrapper;
import gov.usbr.wq.dataaccess.model.MeasureWrapper;
import gov.usbr.wq.dataaccess.model.MeasureWrapperBuilder;
import gov.usbr.wq.dataaccess.model.QualityVersionWrapper;
import gov.usbr.wq.merlindataexchange.DataExchangeCache;
import gov.usbr.wq.merlindataexchange.MerlinDataExchangeParameters;
import gov.usbr.wq.merlindataexchange.MerlinExchangeDaoCompletionTracker;
import gov.usbr.wq.merlindataexchange.configuration.DataExchangeSet;
import gov.usbr.wq.merlintohec.exceptions.MerlinInvalidTimestepException;
import gov.usbr.wq.merlintohec.model.MerlinDaoConversionUtil;
import hec.io.TimeSeriesContainer;
import hec.ui.ProgressListener;
import hec.ui.ProgressListener.MessageType;

import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

public final class MerlinDataExchangeReader implements DataExchangeReader
{

    private static final Logger LOGGER = Logger.getLogger(MerlinDataExchangeReader.class.getName());
    private final String _sourcePath;

    public MerlinDataExchangeReader(String sourcePath)
    {
        _sourcePath = sourcePath;
    }

    @Override
    public CompletableFuture<TimeSeriesContainer> readData(DataExchangeSet dataExchangeSet, MerlinDataExchangeParameters runtimeParameters, DataExchangeCache cache, String seriesPath,
                                                           TokenContainer accessToken, MerlinExchangeDaoCompletionTracker completionTracker,
                                                           ProgressListener progressListener, AtomicBoolean isCancelled, Logger logFileLogger, ExecutorService executorService)
    {
        Instant startTime = runtimeParameters.getStart();
        Instant endTime = runtimeParameters.getEnd();
        if (startTime == null)
        {
            startTime = Instant.ofEpochMilli(Long.MIN_VALUE);
        }
        if (endTime == null)
        {
            endTime =  Instant.ofEpochMilli(Long.MAX_VALUE);
        }
        Instant start = startTime;
        Instant end = endTime;
        String fPartOverride = runtimeParameters.getFPartOverride();
        QualityVersionWrapper qualityVersion = getQualityVersionIdFromDataExchangeSet(dataExchangeSet, cache, logFileLogger, progressListener).orElse(null);
        String unitSystemToConvertTo = dataExchangeSet.getUnitSystem();
        Integer qualityVersionId = qualityVersion == null ? null : qualityVersion.getQualityVersionID();
        logFileLogger.info(() -> "Start time: " + start.toString() + " | End time: " + end.toString() + " | Quality version: " + qualityVersionId);
        logFileLogger.info(() -> "Retrieving data for measure with series string: " + seriesPath + "...");
        progressListener.progress("Retrieving data for measure with series string: " + seriesPath + "...", MessageType.IMPORTANT);
        return CompletableFuture.supplyAsync(() ->
        {
            DataWrapper data = retrieveDataWithUpdatedTimeWindow(start, end, seriesPath, qualityVersionId,
                    accessToken, completionTracker, progressListener, logFileLogger, isCancelled);
            TimeSeriesContainer retVal = null;
            if(!isCancelled.get())
            {
                try
                {
                    retVal = MerlinDaoConversionUtil.convertToTsc(data, unitSystemToConvertTo, fPartOverride, progressListener, logFileLogger);
                }
                catch (MerlinInvalidTimestepException e)
                {
                    logFileLogger.log(Level.WARNING, e, () -> "Unsupported timestep: " + data.getTimestep());
                    progressListener.progress("Skipping Measure with unsupported timestep", MessageType.IMPORTANT);
                    LOGGER.log(Level.CONFIG, e, () -> "Unsupported timestep: " + data.getTimestep());
                }
            }
            return retVal;

        }, executorService);
    }

    @Override
    public void close()
    {
        //Nothing to close, reading from merlin web service
    }

    @Override
    public String getSourcePath()
    {
        return _sourcePath;
    }

    private static DataWrapper retrieveDataWithUpdatedTimeWindow(Instant start, Instant end, String seriesPath, Integer qualityVersionId,
                                                                 TokenContainer accessToken, MerlinExchangeDaoCompletionTracker completionTracker,
                                                                 ProgressListener progressListener, Logger logFileLogger, AtomicBoolean isCancelled)
    {
        MerlinTimeSeriesDataAccess access = new MerlinTimeSeriesDataAccess();
        DataWrapper retVal = null;
        if(!isCancelled.get())
        {
            try
            {
                MeasureWrapper measure = new MeasureWrapperBuilder().withSeriesString(seriesPath).build();
                retVal = access.getEventsBySeries(accessToken, measure, qualityVersionId, start, end);
                String progressMsg = "Successfully retrieved data for " + measure.getSeriesString() + " with " + retVal.getEvents().size() + " events!";
                logFileLogger.info(() -> progressMsg);
                progressListener.progress(progressMsg, MessageType.IMPORTANT, completionTracker.readTaskCompleted());
            }
            catch (IOException | HttpAccessException ex)
            {
                logFileLogger.log(Level.WARNING, ex, () -> "Failed to retrieve data for " + seriesPath);
                progressListener.progress("Failed to retrieve data for measure with series string: " + seriesPath, MessageType.ERROR);
                LOGGER.log(Level.CONFIG, ex, () -> "Failed to retrieve data for " + seriesPath);
            }
        }
        return retVal;
    }

    private Optional<QualityVersionWrapper> getQualityVersionIdFromDataExchangeSet(DataExchangeSet dataExchangeSet, DataExchangeCache cache, Logger logger, ProgressListener progressListener)
    {
        String qualityVersionNameFromSet = dataExchangeSet.getQualityVersionName();
        Integer qualityVersionIdFromSet = dataExchangeSet.getQualityVersionId();
        progressListener.progress("Retrieving Quality Version for " + qualityVersionNameFromSet + " (id: " +  qualityVersionIdFromSet + ")", ProgressListener.MessageType.IMPORTANT);
        List<QualityVersionWrapper> qualityVersions = cache.getCachedQualityVersions();
        Optional<QualityVersionWrapper> retVal = qualityVersions.stream()
                .filter(qualityVersion -> qualityVersion.getQualityVersionName().equalsIgnoreCase(qualityVersionNameFromSet))
                .findFirst();
        if(!retVal.isPresent())
        {
            retVal = qualityVersions.stream()
                    .filter(qualityVersion -> qualityVersion.getQualityVersionID().intValue() == qualityVersionIdFromSet)
                    .findFirst();
        }
        if(!retVal.isPresent())
        {
            logger.log(Level.WARNING, () -> "Failed to find matching quality version ID in retrieved quality versions for quality version name "
                    + qualityVersionNameFromSet + " or id " + qualityVersionIdFromSet
                    + ". Using NULL for quality version.");
        }
        return retVal;
    }

}
