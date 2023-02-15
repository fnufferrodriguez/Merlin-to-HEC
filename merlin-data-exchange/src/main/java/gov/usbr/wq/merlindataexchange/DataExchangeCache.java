package gov.usbr.wq.merlindataexchange;

import gov.usbr.wq.dataaccess.model.MeasureWrapper;
import gov.usbr.wq.dataaccess.model.QualityVersionWrapper;
import gov.usbr.wq.dataaccess.model.TemplateWrapper;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class DataExchangeCache
{
    private final List<TemplateWrapper> _cachedTemplates = new ArrayList<>();
    private final List<QualityVersionWrapper> _cachedQualityVersions = new ArrayList<>();
    private final Map<TemplateWrapper, List<MeasureWrapper>> _cachedTemplateToMeasurements = new HashMap<>();

    public List<TemplateWrapper> getCachedTemplates()
    {
        return new ArrayList<>(_cachedTemplates);
    }

    public List<QualityVersionWrapper> getCachedQualityVersions()
    {
        return new ArrayList<>(_cachedQualityVersions);
    }

    public Map<TemplateWrapper, List<MeasureWrapper>> getCachedTemplateToMeasurements()
    {
        return new HashMap<>(_cachedTemplateToMeasurements);
    }

    public void cacheTemplates(List<TemplateWrapper> templates)
    {
        _cachedTemplates.addAll(templates);
    }

    public void cachedQualityVersions(List<QualityVersionWrapper> qualityVersions)
    {
        _cachedQualityVersions.addAll(qualityVersions);
    }
    public void cacheMeasures(TemplateWrapper template, List<MeasureWrapper> measures)
    {
        _cachedTemplateToMeasurements.put(template, measures);
    }

    public void clearCache()
    {
        _cachedTemplates.clear();
        _cachedQualityVersions.clear();
        _cachedTemplateToMeasurements.clear();
    }

}
