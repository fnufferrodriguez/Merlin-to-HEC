package gov.usbr.wq.merlindataexchange.io.wq;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.BeanDescription;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.introspect.BeanPropertyDefinition;
import gov.usbr.wq.dataaccess.model.DataWrapper;
import gov.usbr.wq.dataaccess.model.MeasureWrapper;
import gov.usbr.wq.merlindataexchange.MerlinDataExchangeLogBody;
import gov.usbr.wq.merlindataexchange.MerlinExchangeCompletionTracker;
import gov.usbr.wq.merlindataexchange.io.MerlinDataExchangeReader;
import hec.ui.ProgressListener;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicReference;

public final class MerlinDataExchangeCsvProfileReader extends MerlinDataExchangeReader<DepthTempProfileSamples>
{
    @Override
    protected DepthTempProfileSamples convertToType(DataWrapper data, String unitSystemToConvertTo, String fPartOverride, ProgressListener progressListener,
                                                           MerlinDataExchangeLogBody logFileLogger, MerlinExchangeCompletionTracker completionTracker, Boolean isProcessed,
                                                           Instant start, Instant end, AtomicReference<String> readDurationString)
    {
        DepthTempProfileSamples retVal = new DepthTempProfileSamples(null, new ArrayList<>(), new TreeMap<>());
        //TODO: Convert returned data from merlin into appropriate format here
        return null;
    }

    private Map<String, String> buildHeadersMap(MeasureWrapper measure)
    {
        Map<String, String> retVal = new TreeMap<>();
//        retVal.put("date", getPropertyName("date"));
//        retVal.put(measure.getParameter(), getPropertyName("temperature"));
//        getPropertyName("depth");
        return retVal;
    }

    private static String getPropertyName(String propertyName)
    {
        String retVal = propertyName;
        ObjectMapper mapper = new ObjectMapper();
        Class<?> clazz = CsvDepthTempProfileSampleMeasurement.class;
        BeanDescription beanDescription = mapper.getSerializationConfig().introspect(mapper.constructType(clazz));
        List<BeanPropertyDefinition> properties = beanDescription.findProperties();
        for (BeanPropertyDefinition property : properties)
        {
            if(property.getName().equalsIgnoreCase(propertyName))
            {
                JsonProperty jsonPropertyAnnotation = property.getPrimaryMember().getAnnotation(JsonProperty.class);
                retVal = jsonPropertyAnnotation == null ? propertyName : jsonPropertyAnnotation.value();
                break;
            }
        }
        return retVal;
    }
}
