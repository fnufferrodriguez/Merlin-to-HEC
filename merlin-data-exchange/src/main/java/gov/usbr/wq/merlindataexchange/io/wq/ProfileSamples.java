package gov.usbr.wq.merlindataexchange.io.wq;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

final class ProfileSamples
{
    private final Double _elevation;
    private final List<ProfileSample> _samples;
    private final Map<String, String> _headersMap;

    ProfileSamples(Double elevation, List<ProfileSample> samples, Map<String, String> headersMap)
    {
        _elevation = elevation;
        _samples = samples;
        _headersMap = headersMap;
    }

    Double getElevation()
    {
        return _elevation;
    }

    List<ProfileSample> getSamples()
    {
        return new ArrayList<>(_samples);
    }

    Map<String, String> getHeadersMap()
    {
        return new LinkedHashMap<>(_headersMap);
    }

}
