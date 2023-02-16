package gov.usbr.wq.merlindataexchange;

import gov.usbr.wq.dataaccess.model.QualityVersionWrapper;
import gov.usbr.wq.dataaccess.model.TemplateWrapper;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public final class DataExchangeCache
{
    private final Set<TemplateWrapper> _cachedTemplates = new HashSet<>();
    private final Set<QualityVersionWrapper> _cachedQualityVersions = new HashSet<>();
    private final Map<TemplateWrapper, List<String>> _cachedTemplateToSeriesIds = new HashMap<>();

    public List<TemplateWrapper> getCachedTemplates()
    {
        return new ArrayList<>(_cachedTemplates);
    }

    public List<QualityVersionWrapper> getCachedQualityVersions()
    {
        return new ArrayList<>(_cachedQualityVersions);
    }

    public Map<TemplateWrapper, List<String>> getCachedTemplateToSeriesIds()
    {
        return new HashMap<>(_cachedTemplateToSeriesIds);
    }

    public void cacheTemplates(List<TemplateWrapper> templates)
    {
        _cachedTemplates.addAll(templates);
    }

    public void cacheQualityVersions(List<QualityVersionWrapper> qualityVersions)
    {
        _cachedQualityVersions.addAll(qualityVersions);
    }
    public void cacheSeriesIds(TemplateWrapper template, List<String> seriesIds)
    {
        _cachedTemplateToSeriesIds.put(template, seriesIds);
    }

    public void clearCache()
    {
        _cachedTemplates.clear();
        _cachedQualityVersions.clear();
        _cachedTemplateToSeriesIds.clear();
    }

}
