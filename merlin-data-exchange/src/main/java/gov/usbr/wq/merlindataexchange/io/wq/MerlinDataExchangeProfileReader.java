package gov.usbr.wq.merlindataexchange.io.wq;

import gov.usbr.wq.dataaccess.MerlinTimeSeriesDataAccess;
import gov.usbr.wq.dataaccess.http.ApiConnectionInfo;
import gov.usbr.wq.dataaccess.http.HttpAccessException;
import gov.usbr.wq.dataaccess.http.TokenContainer;
import gov.usbr.wq.dataaccess.model.DataWrapper;
import gov.usbr.wq.dataaccess.model.EventWrapper;
import gov.usbr.wq.dataaccess.model.MeasureWrapper;
import gov.usbr.wq.dataaccess.model.MeasureWrapperBuilder;
import gov.usbr.wq.merlindataexchange.MerlinDataExchangeLogBody;
import gov.usbr.wq.merlindataexchange.MerlinExchangeCompletionTracker;
import gov.usbr.wq.merlindataexchange.NoEventsException;
import gov.usbr.wq.merlindataexchange.configuration.Constituent;
import gov.usbr.wq.merlindataexchange.configuration.DataExchangeSet;
import gov.usbr.wq.merlindataexchange.configuration.DataStore;
import gov.usbr.wq.merlindataexchange.configuration.DataStoreProfile;
import gov.usbr.wq.merlindataexchange.io.DataExchangeReader;
import gov.usbr.wq.merlindataexchange.io.MerlinDataExchangeReader;
import hec.data.Parameter;
import hec.data.Units;
import hec.data.UnitsConversionException;
import hec.ui.ProgressListener;
import rma.services.annotations.ServiceProvider;

import java.io.IOException;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.NavigableSet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.Logger;

import static java.util.stream.Collectors.toList;

@ServiceProvider(service = DataExchangeReader.class, position = 200, path = DataExchangeReader.LOOKUP_PATH
        + "/" + MerlinDataExchangeReader.MERLIN + "/" + MerlinDataExchangeProfileReader.PROFILE)
public final class MerlinDataExchangeProfileReader extends MerlinDataExchangeReader<IndexedProfileDataWrappers, ProfileSample>
{
    public static final String PROFILE = "profile";
    private static final Logger LOGGER = Logger.getLogger(MerlinDataExchangeProfileReader.class.getName());

    @Override
    protected ProfileSample convertToType(IndexedProfileDataWrappers indexedProfileDataWrappers, DataStore dataStore, String unitSystemToConvertTo,
                                          String fPartOverride, ProgressListener progressListener, MerlinDataExchangeLogBody logFileLogger,
                                          MerlinExchangeCompletionTracker completionTracker, Boolean isProcessed,
                                          Instant start, Instant end, AtomicReference<String> readDurationString)
    {
        ProfileSample retVal = null;
        DataStoreProfile dataStoreProfile = (DataStoreProfile) dataStore;
        try
        {
            Constituent depthConstituent = dataStoreProfile.getDepthConstituent();
            if(depthConstituent != null)
            {
                ZonedDateTime dateTime = null;
                List<ProfileConstituentData> constituentData = new ArrayList<>();
                for(Constituent constituent : dataStoreProfile.getConstituents())
                {
                    DataWrapper depthDataWrapper = indexedProfileDataWrappers.get(constituent.getIndex());
                    NavigableSet<EventWrapper> constituentEvents = depthDataWrapper.getEvents();
                    if(depthConstituent.equals(constituent) && constituentEvents.isEmpty())
                    {
                        throw new NoEventsException("CSV", depthDataWrapper.getSeriesId());
                    }
                    if(dateTime == null && depthConstituent.equals(constituent))
                    {
                        dateTime = depthDataWrapper.getEvents().first().getDate();
                    }
                    Integer index = constituent.getIndex();
                    DataWrapper dataWrapper = indexedProfileDataWrappers.get(index);
                    List<Double> dataValues = dataWrapper.getEvents().stream().map(EventWrapper::getValue)
                            .collect(toList());
                    Parameter paramId = Parameter.getParameterForUnitsString(constituent.getUnit());
                    int convertToUnitSystemId = Units.getUnitSystemForUnits(constituent.getUnit());
                    dataValues = convertUnits(dataValues, paramId.getParameterId(), dataWrapper.getUnits(), convertToUnitSystemId);
                    constituentData.add(new ProfileConstituentData(constituent.getParameter(), dataValues, constituent.getUnit()));
                }
                retVal = new ProfileSample(dateTime, constituentData);
            }
        }
        catch (NoEventsException e)
        {
            handleNoEventsForData(indexedProfileDataWrappers, start, end, isProcessed, readDurationString, completionTracker, logFileLogger, progressListener, e);
        }
        catch (UnitsConversionException e)
        {
            logUnitConversionError(e, progressListener);
        }
        return retVal;
    }

    private List<Double> convertUnits(List<Double> dataList, int paramId, String units, int unitSystemToConvertTo) throws UnitsConversionException
    {
        List<Double> retVal = new ArrayList<>(dataList);
        int nativeUnitSystem = Units.getUnitSystemForUnits(units);
        if(nativeUnitSystem != unitSystemToConvertTo)
        {
            retVal.clear();
            for(Double data : dataList)
            {
                retVal.add(Units.convertUnits(data, paramId, nativeUnitSystem, unitSystemToConvertTo));
            }
        }
        return retVal;
    }

    private void logUnitConversionError(Exception e, ProgressListener progressListener)
    {
        LOGGER.log(Level.CONFIG, e, () -> "Failed to determine units to convert to");
        if (progressListener != null)
        {
            progressListener.progress("Failed to determine units to convert to", ProgressListener.MessageType.ERROR);
        }
    }

    private void handleNoEventsForData(IndexedProfileDataWrappers dataWrappers, Instant start, Instant end, Boolean isProcessed, AtomicReference<String> readDurationString,
                                       MerlinExchangeCompletionTracker completionTracker, MerlinDataExchangeLogBody logFileLogger, ProgressListener progressListener, NoEventsException e)
    {
        List<String> seriesIds = dataWrappers.values().stream().map(DataWrapper::getSeriesId).collect(toList());
        String seriesIdsString = String.join(",\n", seriesIds);
        String progressMsg = "Read " + seriesIdsString + " | Is processed: " + isProcessed + " | Values read: 0"
                 + readDurationString.get();
        String noDataMsg = e.getMessage();
        DataWrapper depth = dataWrappers.get(0);
        if(start == null)
        {
            start = depth.getStartTime().toInstant();
        }
        if(end == null)
        {
            end = depth.getEndTime().toInstant();
        }
        Instant startDetermined = start;
        Instant endDetermined = end;
        //when no data is found we count it as a read + no data written completed and move on, but want to ensure these get written together.
        for(int i=1; i < dataWrappers.size(); i++)
        {
            completionTracker.readWriteTaskCompleted();
            completionTracker.readWriteTaskCompleted();
        }
        int readPercentIncrement = completionTracker.readWriteTaskCompleted();
        int nothingToWritePercentIncrement = completionTracker.readWriteTaskCompleted();
        logProgressMessage(progressListener, progressMsg, readPercentIncrement);
        logProgressMessage(progressListener, noDataMsg, nothingToWritePercentIncrement);
        logFileLogger.log(progressMsg);
        logFileLogger.log(noDataMsg);
        LOGGER.log(Level.CONFIG, e, () -> "No events for " + seriesIdsString + " in time window " + startDetermined + " | " + endDetermined);
    }

    @Override
    protected IndexedProfileDataWrappers retrieveData(Instant start, Instant end, String merlinApiRoot, TokenContainer token, MeasureWrapper measure,
                                                      Integer qualityVersionId, DataStore sourceDataStore, ProgressListener progressListener, MerlinDataExchangeLogBody logFileLogger,
                                                      AtomicBoolean isCancelled, AtomicReference<List<String>> logHelper)
    {
        MerlinTimeSeriesDataAccess access = new MerlinTimeSeriesDataAccess();
        IndexedProfileDataWrappers retVal =  null;
        if(!isCancelled.get())
        {
            try
            {
                String[] split = measure.getSeriesString().split("/");
                DataStoreProfile dataStoreProfile = (DataStoreProfile) sourceDataStore;
                List<String> seriesIds = new ArrayList<>(dataStoreProfile.getConstituents().size());
                for(int i=0; i < dataStoreProfile.getConstituents().size(); i++)
                {
                    seriesIds.add("");
                }
                DataWrapper depthData = access.getEventsBySeries(new ApiConnectionInfo(merlinApiRoot), token, measure, qualityVersionId, start, end);
                Constituent depthConstituent = dataStoreProfile.getDepthConstituent();
                if(depthConstituent != null)
                {
                    retVal = new IndexedProfileDataWrappers();
                    Integer depthIndex = depthConstituent.getIndex();
                    retVal.put(depthIndex, depthData);
                    seriesIds.add(depthIndex, depthData.getSeriesId());
                    for (Constituent constituent : dataStoreProfile.getConstituents())
                    {
                        if (!constituent.equals(depthConstituent))
                        {
                            String fPart = split[5];
                            String pairedFPart = fPart.substring(0, fPart.length() - 1).concat(String.valueOf(constituent.getIndex()));
                            String pairedPathName = getPairedPathName(measure, constituent.getParameter(), pairedFPart);
                            MeasureWrapper constituentMeasure = new MeasureWrapperBuilder()
                                    .withSeriesString(pairedPathName)
                                    .withIsProcessed(measure.isProcessed())
                                    .withStart(measure.getStart())
                                    .withEnd(measure.getEnd())
                                    .withType(measure.getType())
                                    .withTypeId(pairedFPart)
                                    .build();
                            DataWrapper constituentMeasureData = access.getEventsBySeries(new ApiConnectionInfo(merlinApiRoot), token, constituentMeasure, qualityVersionId, start, end);
                            retVal.put(constituent.getIndex(), constituentMeasureData);
                            seriesIds.add(constituent.getIndex(), constituentMeasure.getSeriesString());
                        }
                    }
                    logHelper.set(seriesIds);
                }
            }
            catch (IOException | HttpAccessException ex)
            {
                logError(progressListener, logFileLogger, "Failed to retrieve data for measure with series string: " + measure.getSeriesString(), ex);
            }
        }
        return retVal;
    }

    private String getPairedPathName(MeasureWrapper measure, String paramName, String fPart)
    {
        String[] pairedPathNameSplit = measure.getSeriesString().split("/");
        pairedPathNameSplit[5] = fPart;
        pairedPathNameSplit[1] = paramName;
        StringBuilder pairedPathName = new StringBuilder();
        for (int i=0; i < pairedPathNameSplit.length; i++)
        {
            pairedPathName.append(pairedPathNameSplit[i]);
            if(i < pairedPathNameSplit.length-1)
            {
                pairedPathName.append("/");
            }
        }
        return pairedPathName.toString();
    }

    @Override
    public List<MeasureWrapper> filterMeasuresToRead(DataStore dataStore, DataExchangeSet dataExchangeSet, List<MeasureWrapper> measures)
    {
        //the reader for depth-temp profiles will handle reading 2 measures at once (one for temp, one for depth)
        //so we want to filter out all but the depth measures from the list that gets handed into the exchange as reading
        //the other constituent measures will happen internally inside the reader
        List<MeasureWrapper> retVal = new ArrayList<>();
        if(dataStore instanceof DataStoreProfile)
        {
            DataStoreProfile dataStoreProfile = (DataStoreProfile) dataStore;
            List<Constituent> constituents = dataStoreProfile.getConstituents();
            Constituent depthConstituent = null;
            for(Constituent constituent : constituents)
            {
                if(constituent.getParameter().equalsIgnoreCase(dataStoreProfile.getDepthParameterName()))
                {
                    depthConstituent = constituent;
                    break;
                }
            }
            if(depthConstituent != null)
            {
                Constituent depthConstituentFound = depthConstituent;
                retVal = measures.stream()
                        .filter(m -> m.getParameter().equalsIgnoreCase(depthConstituentFound.getParameter())
                                && m.getType().equalsIgnoreCase(dataExchangeSet.getDataType()))
                        .collect(toList());
            }
        }
        return retVal;
    }
}
