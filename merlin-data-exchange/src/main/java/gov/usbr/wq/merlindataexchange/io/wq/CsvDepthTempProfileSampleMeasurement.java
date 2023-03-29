package gov.usbr.wq.merlindataexchange.io.wq;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

import java.time.ZonedDateTime;
import java.util.Objects;

@JsonPropertyOrder({ "date", "temperature", "depth" })
@JsonInclude(JsonInclude.Include.NON_NULL)
final class CsvDepthTempProfileSampleMeasurement
{
    @JsonIgnore
    private DepthTempProfileSampleMeasurement _delegate;
    CsvDepthTempProfileSampleMeasurement()
    {
        _delegate = new DepthTempProfileSampleMeasurement(null, null, null);
    }

    CsvDepthTempProfileSampleMeasurement(DepthTempProfileSampleMeasurement delegate)
    {
        _delegate = delegate;
    }

    @JsonProperty("date")
    ZonedDateTime getDateTime()
    {
        return _delegate.getDateTime();
    }

    @JsonProperty("temperature")
    Double getTemperature()
    {
        return _delegate.getTemperature();
    }

    @JsonProperty("depth")
    Double getDepth()
    {
        return _delegate.getDepth();
    }

    @JsonProperty("date")
    void setDateTime(ZonedDateTime dateTime)
    {
        _delegate = new DepthTempProfileSampleMeasurement(dateTime, _delegate.getTemperature(), _delegate.getDepth());
    }

    @JsonProperty("temperature")
    void setTemperature(Double temperature)
    {
        _delegate = new DepthTempProfileSampleMeasurement(_delegate.getDateTime(), temperature, _delegate.getDepth());
    }

    @JsonProperty("depth")
    void setDepth(Double depth)
    {
        _delegate = new DepthTempProfileSampleMeasurement(_delegate.getDateTime(), _delegate.getTemperature(), depth);
    }

    DepthTempProfileSampleMeasurement getDelegate()
    {
        return _delegate;
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
        return Objects.equals(_delegate, that._delegate);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(_delegate);
    }
}
