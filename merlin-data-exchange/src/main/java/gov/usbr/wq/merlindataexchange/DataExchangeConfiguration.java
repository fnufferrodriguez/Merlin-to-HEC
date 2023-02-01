package gov.usbr.wq.merlindataexchange;

import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlRootElement;

import java.util.List;

@JacksonXmlRootElement(namespace = "https://www.w3.org/2001/XMLSchema-instance", localName = "data-exchange-configuration")
final class DataExchangeConfiguration
{
    @JacksonXmlProperty(localName = "datastore-merlin")
    private List<DataStoreMerlin> _dataStoresMerlin;
    @JacksonXmlProperty(localName = "datastore-local-dss")
    private List<DataStoreLocalDss> _dataStoresLocalDss;
    @JacksonXmlProperty(localName = "data-exchange-set")
    private List<DataExchangeSet> _dataExchangeSet;

    List<DataStoreMerlin> getDataStoresMerlin()
    {
        return _dataStoresMerlin;
    }

    void setDataStoresMerlin(List<DataStoreMerlin> dataStoreMerlin)
    {
        _dataStoresMerlin = dataStoreMerlin;
    }

    List<DataStoreLocalDss> getDataStoresLocalDss()
    {
        return _dataStoresLocalDss;
    }

    void setDataStoresLocalDss(List<DataStoreLocalDss> dataStoresLocalDss)
    {
        _dataStoresLocalDss = dataStoresLocalDss;
    }

    List<DataExchangeSet> getTimeSeriesDataExchangeSets()
    {
        return _dataExchangeSet;
    }

    void setTimeSeriesDataExchangeSets(List<DataExchangeSet> dataExchangeSet)
    {
        _dataExchangeSet = dataExchangeSet;
    }

}
