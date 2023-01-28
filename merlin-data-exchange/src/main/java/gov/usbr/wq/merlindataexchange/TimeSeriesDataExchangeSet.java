package gov.usbr.wq.merlindataexchange;

import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;
import hec.heclib.util.Unit;

final class TimeSeriesDataExchangeSet
{
    @JacksonXmlProperty(isAttribute = true, localName = "id")
    private String _id;
    @JacksonXmlProperty(isAttribute = true, localName = "template-id")
    private int _templateId;
    @JacksonXmlProperty(isAttribute = true, localName = "quality-version-id")
    private Integer _qualityVersionId;
    @JacksonXmlProperty(isAttribute = true, localName = "unit-system")
    private String _unitSystem = Unit.SI;
    @JacksonXmlProperty(isAttribute = true, localName = "sort-order")
    private double _sortOrder = 0.0;
    @JacksonXmlProperty(localName = "datastore-ref-merlin")
    private DataStoreRef _dataStoreRefMerlin;

    @JacksonXmlProperty(localName = "datastore-ref-local-dss")
    private DataStoreRef _dataStoreRefLocalDss;

    String getId()
    {
        return _id;
    }

    void setId(String id)
    {
        _id = id;
    }

    int getTemplateId()
    {
        return _templateId;
    }

    void setTemplateId(int templateId)
    {
        _templateId = templateId;
    }

    Integer getQualityVersionId()
    {
        return _qualityVersionId;
    }

    void setQualityVersionId(Integer qualityVersionId)
    {
        _qualityVersionId = qualityVersionId;
    }

    String getUnitSystem()
    {
        return _unitSystem;
    }

    void setUnitSystem(String unitSystem)
    {
        _unitSystem = unitSystem;
    }

    double getSortOrder() {
        return _sortOrder;
    }

    void setSortOrder(double sortOrder)
    {
        _sortOrder = sortOrder;
    }

    DataStoreRef getDataStoreRefMerlin()
    {
        return _dataStoreRefMerlin;
    }

    void setDataStoreRefMerlin(DataStoreRef dataStoreRefMerlin)
    {
        _dataStoreRefMerlin = dataStoreRefMerlin;
    }

    DataStoreRef getDataStoreRefLocalDss()
    {
        return _dataStoreRefLocalDss;
    }

    void setDataStoreRefLocalDss(DataStoreRef dataStoreRefLocalDss)
    {
        _dataStoreRefLocalDss = dataStoreRefLocalDss;
    }

}
