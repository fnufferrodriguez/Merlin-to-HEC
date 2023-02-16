package gov.usbr.wq.merlindataexchange;

import hec.ui.ProgressListener;

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public interface DataExchangeEngine
{
    String LOOKUP_PATH = "dataexchange/engine";

    CompletableFuture<MerlinDataExchangeStatus> runExtract(List<Path> xmlConfigurationFiles, MerlinDataExchangeParameters runtimeParameters, ProgressListener progressListener);

    void cancelExtract();
}
