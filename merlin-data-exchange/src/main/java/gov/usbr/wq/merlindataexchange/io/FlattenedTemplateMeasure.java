package gov.usbr.wq.merlindataexchange.io;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.annotation.JsonUnwrapped;
import gov.usbr.wq.dataaccess.json.Measure;
import gov.usbr.wq.dataaccess.json.Template;

@JsonPropertyOrder({ "template", "measure" })
public class FlattenedTemplateMeasure {
    @JsonUnwrapped
    Template template;
    @JsonUnwrapped
    Measure measure;

    public FlattenedTemplateMeasure(Template template, Measure measure) {
        this.template = template;
        this.measure = measure;
    }


}
