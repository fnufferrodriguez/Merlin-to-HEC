package gov.usbr.wq.merlindataexchange.io.wq;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

final class DepthTempProfileSamples
{
    private final Double _elevation;
    private final List<DepthTempProfileSample> _samples;
    private final Map<String, String> _headersMap;

    DepthTempProfileSamples(Double elevation, List<DepthTempProfileSample> samples, Map<String, String> headersMap)
    {
        _elevation = elevation;
        _samples = samples;
        _headersMap = headersMap;
    }

    Double getElevation()
    {
        return _elevation;
    }

    List<DepthTempProfileSample> getSamples()
    {
        return new ArrayList<>(_samples);
    }

    Map<String, String> getHeadersMap()
    {
        return new LinkedHashMap<>(_headersMap);
    }

}
