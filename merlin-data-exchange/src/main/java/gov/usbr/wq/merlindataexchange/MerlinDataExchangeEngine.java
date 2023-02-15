package gov.usbr.wq.merlindataexchange;

import gov.usbr.wq.dataaccess.MerlinTimeSeriesDataAccess;
import gov.usbr.wq.dataaccess.http.HttpAccessException;
import gov.usbr.wq.dataaccess.http.HttpAccessUtils;
import gov.usbr.wq.dataaccess.jwt.TokenContainer;
import gov.usbr.wq.dataaccess.model.MeasureWrapper;
import gov.usbr.wq.dataaccess.model.QualityVersionWrapper;
import gov.usbr.wq.dataaccess.model.TemplateWrapper;
import gov.usbr.wq.merlindataexchange.configuration.DataExchangeConfiguration;
import gov.usbr.wq.merlindataexchange.configuration.DataExchangeSet;
import gov.usbr.wq.merlindataexchange.configuration.DataStore;
import gov.usbr.wq.merlindataexchange.configuration.DataStoreRef;
import gov.usbr.wq.merlindataexchange.io.DataExchangeDao;
import gov.usbr.wq.merlindataexchange.io.DataExchangeDaoFactory;
import hec.ui.ProgressListener;
import hec.ui.ProgressListener.MessageType;
import rma.services.annotations.ServiceProvider;

import javax.xml.stream.XMLStreamException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.FileHandler;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;


@ServiceProvider(service = DataExchangeEngine.class, position = 100, path = DataExchangeEngine.LOOKUP_PATH
        + "/" + MerlinDataExchangeEngine.MERLIN)
public final class MerlinDataExchangeEngine implements DataExchangeEngine
{
    public static final String MERLIN = "merlin";
    private static final Logger LOGGER = Logger.getLogger("MerlinDataExchange");
    private static final int PERCENT_COMPLETE_ALLOCATED_FOR_INITIAL_SETUP = 5;
    private static final int THREAD_COUNT = 5;
    private static final String THREAD_PROPERTY_KEY = "merlin.dataexchange.threadpool.size";
    private final ExecutorService _executorService = Executors.newFixedThreadPool(getThreadPoolSize(), new MerlinThreadFactory());
    private final DataExchangeCache _dataExchangeCache = new DataExchangeCache();
    private final MerlinTimeSeriesDataAccess _merlinDataAccess = new MerlinTimeSeriesDataAccess();
    private final AtomicBoolean _isCancelled = new AtomicBoolean(false);
    private ProgressListener _progressListener;

    private static int getThreadPoolSize()
    {
        int retVal;
        //availableProcessors gives us logical cores, which includes hyperthreading stuff.  We can't determine if hyperthreading is on, so let's always halve the available processors.
        //Let's make sure we don't go lower than 1 by using Math.max.  1 / 2 = 0 in integer values, so this could be bad...
        String threadPoolSize = System.getProperty(THREAD_PROPERTY_KEY);
        if (threadPoolSize != null)
        {
            retVal = Math.max(Integer.parseInt(threadPoolSize), 1);
            LOGGER.log(Level.FINE, () -> "Merlin executor service created using System Property " + THREAD_PROPERTY_KEY + " with thread pool size of: " + retVal);
        }
        else
        {
            int coreCount = Math.max(getCoreCount(), 1);
            retVal = THREAD_COUNT * coreCount; //5 should cover bases for concurrent merlin web data retrieval?
            LOGGER.log(Level.FINE, () -> "System Property " + THREAD_PROPERTY_KEY + " not set. Merlin executor service created using default thread pool size of: " + retVal);
        }
        return retVal;
    }

    private static int getCoreCount()
    {
        return Runtime.getRuntime().availableProcessors() / 2;
    }

    @Override
    public CompletableFuture<Void> runExtract(List<Path> xmlConfigurationFiles, MerlinDataExchangeParameters runtimeParameters, ProgressListener progressListener) throws HttpAccessException
    {
        _progressListener = Objects.requireNonNull(progressListener, "Missing required ProgressListener");
        _isCancelled.set(false);
        _progressListener.start();
        TokenContainer token = HttpAccessUtils.authenticate(runtimeParameters.getUsername(), runtimeParameters.getPassword());
        Map<Path, DataExchangeConfiguration> parsedConfigurations = parseConfigurations(xmlConfigurationFiles);
        return initializeCacheAsync(parsedConfigurations, token)
                .whenComplete((idc, e) -> beginExtract(parsedConfigurations, token, runtimeParameters, e));
    }

    @Override
    public void cancelExtract()
    {
        _isCancelled.set(true);
        if(_progressListener != null)
        {
            logProgress("Merlin Data Exchanged Cancelled. Finishing gracefully...");
        }
    }

    private void beginExtract(Map<Path, DataExchangeConfiguration> xmlConfigurationFiles, TokenContainer token, MerlinDataExchangeParameters runtimeParameters, Throwable initializationError)
    {
        if(initializationError != null)
        {
            LOGGER.log(Level.WARNING, initializationError, () -> "Failed to initialize templates, quality versions, and measures cache");
        }
        else
        {
            if(!_isCancelled.get())
            {
                int totalMeasures = _dataExchangeCache.getCachedTemplateToMeasurements().values().stream()
                        .mapToInt(List::size)
                        .sum();
                MerlinExchangeDaoCompletionTracker completionTracker = new MerlinExchangeDaoCompletionTracker(totalMeasures, PERCENT_COMPLETE_ALLOCATED_FOR_INITIAL_SETUP);
                for(Map.Entry<Path, DataExchangeConfiguration> entry : xmlConfigurationFiles.entrySet())
                {
                    Path configPath = entry.getKey();
                    DataExchangeConfiguration dataExchangeConfiguration = entry.getValue();
                    String configNameWithoutExtension = configPath.getFileName().toString().split("\\.")[0];
                    setUpLoggingForConfig(configNameWithoutExtension, runtimeParameters.getLogFileDirectory());
                    extractConfiguration(dataExchangeConfiguration, runtimeParameters, token, completionTracker);
                }
            }
            finish();
        }
    }

    private void finish()
    {
        try
        {
            if (_isCancelled.get())
            {
                logProgress("Merlin Data Exchanged Cancelled successfully");
            }
            else
            {
                logProgress("Merlin Data Exchange Complete");
            }
            _progressListener.finish();
        }
        finally
        {
            Handler[] handler = LOGGER.getHandlers();
            for(Handler h: handler)
            {
                h.close();
            }
        }
    }

    private Map<Path, DataExchangeConfiguration> parseConfigurations(List<Path> xmlConfigurationFiles)
    {
        Map<Path, DataExchangeConfiguration> retVal = new TreeMap<>();
        xmlConfigurationFiles.forEach(configFilepath -> retVal.put(configFilepath, parseDataExchangeConfiguration(configFilepath)));
        logProgress("Configuration files parsed", 1);
        return retVal;
    }

    private void extractConfiguration(DataExchangeConfiguration dataExchangeConfig, MerlinDataExchangeParameters runtimeParameters, TokenContainer token,
                                      MerlinExchangeDaoCompletionTracker completionTracker)
    {
        if (dataExchangeConfig != null)
        {
            List<DataExchangeSet> dataExchangeSets = dataExchangeConfig.getDataExchangeSets();
            dataExchangeSets.forEach(dataExchangeSet ->
            {
                if(!_isCancelled.get())
                {
                    exchangeDataForSet(dataExchangeSet, dataExchangeConfig, runtimeParameters, token, completionTracker);
                }
            });
        }
    }

    private void setUpLoggingForConfig(String configNameWithoutExtension, Path logDirectory)
    {
        Path logFile = logDirectory.resolve(configNameWithoutExtension + ".log");
        try
        {
            Files.createDirectories(logDirectory);
            new FileOutputStream(logFile.toString(), false).close();
            FileHandler fileHandler = new FileHandler(logFile.toString());
            if (LOGGER.getHandlers().length > 0)
            {
                LOGGER.removeHandler(LOGGER.getHandlers()[0]);
            }
            LOGGER.addHandler(fileHandler);
            SimpleFormatter formatter = new SimpleFormatter();
            fileHandler.setFormatter(formatter);
        }
        catch (IOException e)
        {
            LOGGER.log(Level.SEVERE, e, () -> "Failed to create log file: " + logFile);
        }
    }

    private void exchangeDataForSet(DataExchangeSet dataExchangeSet, DataExchangeConfiguration dataExchangeConfig,
                                    MerlinDataExchangeParameters runtimeParameters, TokenContainer token, MerlinExchangeDaoCompletionTracker completionTracker)
    {
        DataStoreRef dataStoreRefB = dataExchangeSet.getDataStoreRefB();
        DataStoreRef dataStoreRefA = dataExchangeSet.getDataStoreRefA();
        DataStoreRef sourceRef = dataStoreRefA;
        DataStoreRef destinationRef = dataStoreRefB;
        if (dataExchangeSet.getSourceId().equalsIgnoreCase(dataStoreRefB.getId()))
        {
            destinationRef = dataStoreRefA;
            sourceRef = dataStoreRefB;
        }
        Optional<DataStore> dataStoreDestinationOpt = dataExchangeConfig.getDataStoreByRef(destinationRef);
        Optional<DataStore> dataStoreSourceOpt = dataExchangeConfig.getDataStoreByRef(sourceRef);
        if(!dataStoreDestinationOpt.isPresent())
        {
            String destinationRefIdId = destinationRef.getId();
            _progressListener.progress("No datastore found for datastore-ref id: " + destinationRefIdId, MessageType.ERROR);
            LOGGER.log(Level.SEVERE,() -> "No datastore found for datastore-ref id: " + destinationRefIdId);
        }
        if(!dataStoreSourceOpt.isPresent())
        {
            String destinationRefIdId = destinationRef.getId();
            _progressListener.progress("No datastore found for datastore-ref id: " + destinationRefIdId, MessageType.ERROR);
            LOGGER.log(Level.SEVERE,() -> "No datastore found for datastore-ref id: " + destinationRefIdId);
        }
        if(dataStoreDestinationOpt.isPresent() && dataStoreSourceOpt.isPresent())
        {
            exchangeData(dataExchangeSet, dataStoreSourceOpt.get(), dataStoreDestinationOpt.get(), runtimeParameters, token, completionTracker);
        }
    }

    private void exchangeData(DataExchangeSet dataExchangeSet, DataStore dataStoreSource, DataStore dataStoreDestination,
                              MerlinDataExchangeParameters runtimeParameters, TokenContainer token, MerlinExchangeDaoCompletionTracker completionTracker)
    {
        TemplateWrapper template = getTemplateFromDataExchangeSet(dataExchangeSet);
        if(template != null)
        {
            String unitSystemToConvertTo = dataExchangeSet.getUnitSystem();
            logProgress("Specified unit system: " + unitSystemToConvertTo);
            DataExchangeDao dao = DataExchangeDaoFactory.lookupDao(dataStoreSource, dataStoreDestination, LOGGER);
            List<MeasureWrapper> measures = _dataExchangeCache.getCachedTemplateToMeasurements().get(template);
            List<CompletableFuture<Void>> measurementFutures = new ArrayList<>();
            try
            {
                measures.forEach(measure ->
                        measurementFutures.add(dao.exchangeData(dataExchangeSet, runtimeParameters, _dataExchangeCache,
                                dataStoreSource, dataStoreDestination, measure.getSeriesString(), token, completionTracker, _progressListener,
                                _isCancelled, LOGGER, _executorService)));
                CompletableFuture.allOf(measurementFutures.toArray(new CompletableFuture[0])).join();
            }
            finally
            {
                dao.cleanUp();
            }

        }
    }

    private void logProgress(String message)
    {
        if(_progressListener != null)
        {
            _progressListener.progress(message, MessageType.IMPORTANT);
        }
        LOGGER.info(() -> message);
    }

    private void logProgress(String message, int progressPercentage)
    {
        if(_progressListener != null)
        {
            _progressListener.progress(message, MessageType.IMPORTANT, progressPercentage);
        }
        LOGGER.info(() -> message);
    }

    private List<MeasureWrapper> retrieveMeasures(TokenContainer accessToken, TemplateWrapper template, ProgressListener progressListener)
    {
        List<MeasureWrapper> retVal = new ArrayList<>();
        try
        {
            logProgress("Retrieving measures for template " + template.getName() + " (id: " + template.getDprId() + ")...");
            retVal = _merlinDataAccess.getMeasurementsByTemplate(accessToken, template);
            logProgress("Successfully retrieved " + retVal.size() + " measures!", PERCENT_COMPLETE_ALLOCATED_FOR_INITIAL_SETUP);
        }
        catch (HttpAccessException | IOException ex)
        {
            LOGGER.log(Level.SEVERE, ex, () -> "Unable to access the merlin web services to retrieve measures for template " + template);
            progressListener.progress("Failed to retrieve measures for template " + template.getName() + " (id: " + template.getDprId() + ")", MessageType.ERROR);
        }
        return retVal;
    }

    private TemplateWrapper getTemplateFromDataExchangeSet(DataExchangeSet dataExchangeSet)
    {
        String templateNameFromSet = dataExchangeSet.getTemplateName();
        int templateIdFromSet = dataExchangeSet.getTemplateId();
        logProgress("Retrieving Template for " + templateNameFromSet + " (id: " + templateIdFromSet + ")");
        List<TemplateWrapper> cachedTemplates = _dataExchangeCache.getCachedTemplates();
        TemplateWrapper retVal = cachedTemplates.stream()
                .filter(template -> template.getName().equalsIgnoreCase(templateNameFromSet))
                .findFirst()
                .orElse(null);
        if(retVal == null)
        {
            retVal = cachedTemplates.stream()
                    .filter(template -> template.getDprId() == templateIdFromSet)
                    .findFirst()
                    .orElse(null);
        }
        if(retVal == null)
        {
            String errorMsg = "Failed to find matching template ID in retrieved templates for template name " + templateNameFromSet + " or id " + templateIdFromSet;
            LOGGER.log(Level.SEVERE, () -> errorMsg);
            _progressListener.progress(errorMsg, MessageType.ERROR);
        }
        return retVal;
    }

    private DataExchangeConfiguration parseDataExchangeConfiguration(Path xmlConfigurationFile)
    {
        DataExchangeConfiguration retVal = null;
        try
        {
            _progressListener.progress("Parsing configuration file: " + xmlConfigurationFile.toString() + "...", MessageType.IMPORTANT);
            retVal = MerlinDataExchangeParser.parseXmlFile(xmlConfigurationFile);
            _progressListener.progress("Parsed configuration file successfully!", MessageType.IMPORTANT);
        }
        catch (IOException | XMLStreamException e)
        {
            String errorMsg = "Failed to parse data exchange configuration xml for " + xmlConfigurationFile.toString();
            LOGGER.log(Level.WARNING, e, () -> errorMsg);
            _progressListener.progress(errorMsg, MessageType.ERROR);
        }
        return retVal;
    }

    private CompletableFuture<Void> initializeCacheAsync(Map<Path, DataExchangeConfiguration> parsedConfigurations, TokenContainer token)
    {
        return CompletableFuture.runAsync(() ->
        {
            if(_dataExchangeCache.getCachedTemplates().isEmpty() || _dataExchangeCache.getCachedQualityVersions().isEmpty())
            {
                try
                {
                    logProgress("Retrieving templates from Merlin Web Service");
                    List<TemplateWrapper> templates = _merlinDataAccess.getTemplates(token);
                    _dataExchangeCache.cacheTemplates(templates);
                    logProgress("Successfully retrieved " + templates.size() + " templates.", (int) (PERCENT_COMPLETE_ALLOCATED_FOR_INITIAL_SETUP * 0.4));
                    logProgress("Retrieving quality versions from Merlin Web Service");
                    List<QualityVersionWrapper> qualityVersions = _merlinDataAccess.getQualityVersions(token);
                    _dataExchangeCache.cachedQualityVersions(qualityVersions);
                    logProgress("Successfully retrieved " + qualityVersions.size() + " quality versions.", (int) (PERCENT_COMPLETE_ALLOCATED_FOR_INITIAL_SETUP * 0.6));
                    if(!_isCancelled.get())
                    {
                        initializeCachedMeasurements(parsedConfigurations, token);
                    }
                }
                catch (IOException | HttpAccessException e)
                {
                    _progressListener.progress("Failed to retrieve templates and quality versions from Merlin Web Service", MessageType.ERROR);
                    throw new CompletionException(e);
                }
            }
            else
            {
                initializeCachedMeasurements(parsedConfigurations, token);
            }
        }, _executorService);
    }

    private void initializeCachedMeasurements(Map<Path, DataExchangeConfiguration> parsedConfigurations, TokenContainer token)
    {
        parsedConfigurations.values().forEach(dataExchangeConfig ->
        {
            List<DataExchangeSet> exchangeSets = dataExchangeConfig.getDataExchangeSets();
            exchangeSets.forEach(set ->
            {
                TemplateWrapper template = getTemplateFromDataExchangeSet(set);
                if(template != null && !_dataExchangeCache.getCachedTemplateToMeasurements().containsKey(template))
                {
                    List<MeasureWrapper> measures = retrieveMeasures(token, template, _progressListener);
                    _dataExchangeCache.cacheMeasures(template, measures);
                }
            });
        });
    }

}
