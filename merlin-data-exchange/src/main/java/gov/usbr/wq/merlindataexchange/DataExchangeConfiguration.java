package gov.usbr.wq.merlindataexchange;

import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlRootElement;

@JacksonXmlRootElement(namespace = "https://www.w3.org/2001/XMLSchema-instance", localName = "data-exchange-configuration")
final class DataExchangeConfiguration
{
    @JacksonXmlProperty(localName = "datastore-merlin")
    private DataStoreMerlin _dataStoreMerlin;
    @JacksonXmlProperty(localName = "datastore-local-dss")
    private DataStoreLocalDss _dataStoreLocalDss;
    @JacksonXmlProperty(localName = "ts-data-exchange-set")
    private TimeSeriesDataExchangeSet _timeSeriesDataExchangeSet;

    DataStoreMerlin getDataStoreMerlin()
    {
        return _dataStoreMerlin;
    }

    void setDataStoreMerlin(DataStoreMerlin dataStoreMerlin)
    {
        _dataStoreMerlin = dataStoreMerlin;
    }

    DataStoreLocalDss getDataStoreLocalDss()
    {
        return _dataStoreLocalDss;
    }

    void setDataStoreLocalDss(DataStoreLocalDss dataStoreLocalDss)
    {
        _dataStoreLocalDss = dataStoreLocalDss;
    }

    TimeSeriesDataExchangeSet getTimeSeriesDataExchangeSet()
    {
        return _timeSeriesDataExchangeSet;
    }

    void setTimeSeriesDataExchangeSet(TimeSeriesDataExchangeSet timeSeriesDataExchangeSet)
    {
        _timeSeriesDataExchangeSet = timeSeriesDataExchangeSet;
    }

    DataStore getDataStoreByRef(DataStoreRef ref)
    {
        DataStore retVal = null;
        if(ref != null)
        {
            String id = ref.getId();
            if(id.equalsIgnoreCase(_dataStoreMerlin.getId()))
            {
                retVal = _dataStoreMerlin;
            }
            else if(id.equalsIgnoreCase(_dataStoreLocalDss.getId()))
            {
                retVal = _dataStoreLocalDss;
            }
        }
        return retVal;
    }

}
