package gov.usbr.wq.merlindataexchange.io.wq;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.BeanDescription;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.introspect.BeanPropertyDefinition;
import gov.usbr.wq.dataaccess.MerlinTimeSeriesDataAccess;
import gov.usbr.wq.dataaccess.http.ApiConnectionInfo;
import gov.usbr.wq.dataaccess.http.HttpAccessException;
import gov.usbr.wq.dataaccess.http.TokenContainer;
import gov.usbr.wq.dataaccess.model.DataWrapper;
import gov.usbr.wq.dataaccess.model.MeasureWrapper;
import gov.usbr.wq.dataaccess.model.MeasureWrapperBuilder;
import gov.usbr.wq.merlindataexchange.MerlinDataExchangeLogBody;
import gov.usbr.wq.merlindataexchange.MerlinExchangeCompletionTracker;
import gov.usbr.wq.merlindataexchange.NoEventsException;
import gov.usbr.wq.merlindataexchange.io.MerlinDataExchangeReader;
import hec.heclib.dss.DSSPathname;
import hec.ui.ProgressListener;

import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public final class MerlinDataExchangeDepthTempProfileReader extends MerlinDataExchangeReader<ProfileSample, ProfileSample>
{

    private static final Logger LOGGER = Logger.getLogger(MerlinDataExchangeDepthTempProfileReader.class.getName());

    @Override
    protected ProfileSample convertToType(ProfileSample sample, String unitSystemToConvertTo, String fPartOverride, ProgressListener progressListener,
                                           MerlinDataExchangeLogBody logFileLogger, MerlinExchangeCompletionTracker completionTracker, Boolean isProcessed,
                                           Instant start, Instant end, AtomicReference<String> readDurationString)
    {
        ProfileSample retVal = null;
        try
        {
            DataWrapper depthData = sample.getDepthData();
            DataWrapper tempData = sample.getTempData();
           if(tempData.getEvents().isEmpty() && depthData.getEvents().isEmpty())
           {
               throw new NoEventsException("CSV", tempData.getSeriesId() + " \n or " + depthData.getSeriesId());
           }
           retVal = sample;
        }
        catch (NoEventsException e)
        {
            handleNoEventsForData(sample.getTempData(), sample.getDepthData(), start, end, isProcessed, readDurationString, completionTracker, logFileLogger, progressListener, e);
        }
        return retVal;
    }

    private void handleNoEventsForData(DataWrapper tempData, DataWrapper depthData, Instant start, Instant end, Boolean isProcessed, AtomicReference<String> readDurationString,
                                       MerlinExchangeCompletionTracker completionTracker, MerlinDataExchangeLogBody logFileLogger, ProgressListener progressListener, NoEventsException e)
    {
        String progressMsg = "Read " + tempData.getSeriesId() + " \n and " + depthData.getSeriesId() + " | Is processed: " + isProcessed + " | Values read: 0"
                 + readDurationString.get();
        String noDataMsg = e.getMessage();
        if(start == null)
        {
            start = tempData.getStartTime().toInstant();
        }
        if(end == null)
        {
            end = tempData.getEndTime().toInstant();
        }
        Instant startDetermined = start;
        Instant endDetermined = end;
        //when no data is found we count it as a read + no data written completed and move on, but want to ensure these get written together.
        completionTracker.readWriteTaskCompleted();
        completionTracker.readWriteTaskCompleted();
        int readPercentIncrement = completionTracker.readWriteTaskCompleted();
        int nothingToWritePercentIncrement = completionTracker.readWriteTaskCompleted();
        logProgressMessage(progressListener, progressMsg, readPercentIncrement);
        logProgressMessage(progressListener, noDataMsg, nothingToWritePercentIncrement);
        logFileLogger.log(progressMsg);
        logFileLogger.log(noDataMsg);
        LOGGER.log(Level.CONFIG, e, () -> "No events for " + tempData.getSeriesId() + " or " + depthData.getSeriesId() +
                " in time window " + startDetermined + " | " + endDetermined);
    }

    @Override
    protected ProfileSample retrieveData(Instant start, Instant end, String merlinApiRoot, TokenContainer token, MeasureWrapper measure,
                                                    Integer qualityVersionId, ProgressListener progressListener, MerlinDataExchangeLogBody logFileLogger, AtomicBoolean isCancelled)
    {
        MerlinTimeSeriesDataAccess access = new MerlinTimeSeriesDataAccess();
        ProfileSample retVal = null;
        if(!isCancelled.get())
        {
            try
            {
                DSSPathname pathName = new DSSPathname(measure.getSeriesString());
                String bPart = pathName.getBPart();
                String fPart = pathName.getFPart();
                String pairedFPart = fPart.substring(0, fPart.length() - 1).concat("2");
                DSSPathname pairedPathName = new DSSPathname(pathName.toString());
                pairedPathName.setFPart(pairedFPart);
                DataWrapper tempData;
                DataWrapper depthData;
                if(bPart != null && bPart.toLowerCase().contains("temp"))
                {
                    tempData = access.getEventsBySeries(new ApiConnectionInfo(merlinApiRoot), token, measure, qualityVersionId, start, end);
                    MeasureWrapper depthMeasure = new MeasureWrapperBuilder()
                            .withSeriesString(pairedPathName.toString().replaceAll("^/|/$", ""))
                            .withIsProcessed(measure.isProcessed())
                            .withStart(measure.getStart())
                            .withEnd(measure.getEnd())
                            .withType(measure.getType())
                            .withTypeId(pairedFPart)
                            .build();
                    depthData = access.getEventsBySeries(new ApiConnectionInfo(merlinApiRoot), token, depthMeasure, qualityVersionId, start, end);
                }
                else
                {
                    depthData = access.getEventsBySeries(new ApiConnectionInfo(merlinApiRoot), token, measure, qualityVersionId, start, end);
                    MeasureWrapper tempMeasure = new MeasureWrapperBuilder()
                            .withSeriesString(pairedPathName.toString().replaceAll("^/|/$", ""))
                            .withIsProcessed(measure.isProcessed())
                            .withStart(measure.getStart())
                            .withEnd(measure.getEnd())
                            .withType(measure.getType())
                            .withTypeId(pairedFPart)
                            .build();
                    tempData = access.getEventsBySeries(new ApiConnectionInfo(merlinApiRoot), token, tempMeasure, qualityVersionId, start, end);
                }
                retVal = new ProfileSample(depthData, tempData);

            }
            catch (IOException | HttpAccessException ex)
            {
                logError(progressListener, logFileLogger, "Failed to retrieve data for measure with series string: " + measure.getSeriesString(), ex);
            }
        }
        return retVal;
    }

    @Override
    public List<MeasureWrapper> filterMeasuresToRead(List<MeasureWrapper> measures)
    {
        //the reader for depth-temp profiles will handle reading 2 measures at once (one for temp, one for depth)
        //so we want to filter out every other measure from the list that gets handed into the exchange as reading
        //the "paired" measure will happen internally inside the reader
        return IntStream.range(0, measures.size())
                .filter(i -> i % 2 == 0)
                .mapToObj(measures::get)
                .collect(Collectors.toList());
    }
}
