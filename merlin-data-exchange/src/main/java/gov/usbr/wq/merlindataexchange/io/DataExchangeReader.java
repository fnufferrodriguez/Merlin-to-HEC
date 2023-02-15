package gov.usbr.wq.merlindataexchange.io;

import gov.usbr.wq.dataaccess.jwt.TokenContainer;
import gov.usbr.wq.merlindataexchange.DataExchangeCache;
import gov.usbr.wq.merlindataexchange.MerlinDataExchangeParameters;
import gov.usbr.wq.merlindataexchange.MerlinExchangeDaoCompletionTracker;
import gov.usbr.wq.merlindataexchange.configuration.DataExchangeSet;
import hec.io.TimeSeriesContainer;
import hec.ui.ProgressListener;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Logger;

public interface DataExchangeReader
{
    CompletableFuture<TimeSeriesContainer> readData(DataExchangeSet configuration, MerlinDataExchangeParameters runtimeParameters, DataExchangeCache cache, String seriesPath,
                                                    TokenContainer accessToken, MerlinExchangeDaoCompletionTracker completionTracker,
                                                    ProgressListener progressListener, AtomicBoolean isCancelled, Logger logger, ExecutorService executorService);

    void close();

    String getSourcePath();
}
