package gov.usbr.wq.merlindataexchange.io.wq;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

import java.time.ZonedDateTime;
import java.util.Objects;

@JsonPropertyOrder({ "Date", "_temperature", "_depth" })
@JsonInclude(JsonInclude.Include.NON_NULL)
final class CsvDepthTempProfileSampleMeasurement
{
    @JsonProperty("Date")
    private ZonedDateTime _dateTime;
    private Double _temperature;
    private Double _depth;
    CsvDepthTempProfileSampleMeasurement(ZonedDateTime dateTime, Double temperature, Double depth)
    {
        _dateTime = dateTime;
        _temperature = temperature;
        _depth = depth;
    }

    CsvDepthTempProfileSampleMeasurement()
    {
        _dateTime = null;
        _temperature = null;
        _depth = null;
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

    void setDateTime(ZonedDateTime dateTime)
    {
        _dateTime = dateTime;
    }

    void setTemperature(Double temperature)
    {
        _temperature = temperature;
    }

    void setDepth(Double depth)
    {
        _depth = depth;
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
        CsvDepthTempProfileSampleMeasurement that = (CsvDepthTempProfileSampleMeasurement) o;
        return Objects.equals(_dateTime, that._dateTime) && Objects.equals(_temperature, that._temperature) && Objects.equals(_depth, that._depth);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(_dateTime, _temperature, _depth);
    }
}
