package gov.usbr.wq.merlindataexchange.io;

import gov.usbr.wq.merlindataexchange.MerlinDataExchangeParameters;
import gov.usbr.wq.merlindataexchange.MerlinExchangeDaoCompletionTracker;
import hec.io.TimeSeriesContainer;
import hec.ui.ProgressListener;

import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Logger;

public interface DataExchangeWriter
{
    void writeData(TimeSeriesContainer timeSeriesContainer, String seriesPath, MerlinDataExchangeParameters runtimeParameters, MerlinExchangeDaoCompletionTracker completionTracker,
                   ProgressListener progressListener, Logger logger, AtomicBoolean isCancelled);

    void close();

    String getDestinationPath();
}
