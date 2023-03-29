package gov.usbr.wq.merlindataexchange.io.wq;

import java.time.ZonedDateTime;
import java.util.Objects;

public final class DepthTempProfileSampleMeasurement
{
    private final ZonedDateTime _dateTime;
    private final Double _temperature;
    private final Double _depth;

    public DepthTempProfileSampleMeasurement(ZonedDateTime dateTime, Double temperature, Double depth)
    {
        _dateTime = dateTime;
        _temperature = temperature;
        _depth = depth;
    }

    ZonedDateTime getDateTime()
    {
        return _dateTime;
    }

    Double getTemperature()
    {
        return _temperature;
    }

    Double getDepth()
    {
        return _depth;
    }



    @Override
    public boolean equals(Object o)
    {
        if (this == o)
        {
            return true;
        }
        if (o == null || getClass() != o.getClass())
        {
            return false;
        }
        DepthTempProfileSampleMeasurement that = (DepthTempProfileSampleMeasurement) o;
        return Objects.equals(_dateTime, that._dateTime) && Objects.equals(_temperature, that._temperature) && Objects.equals(_depth, that._depth);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(_dateTime, _temperature, _depth);
    }
}
