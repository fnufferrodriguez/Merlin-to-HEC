package gov.usbr.wq.merlindataexchange.io.wq;

import java.util.List;

final class DepthTempProfileSample
{
    private final List<DepthTempProfileSampleMeasurement> _measurements;

    DepthTempProfileSample(List<DepthTempProfileSampleMeasurement> measurements)
    {
        _measurements = measurements;
    }

    List<DepthTempProfileSampleMeasurement> getMeasurements()
    {
        return _measurements;
    }
}
