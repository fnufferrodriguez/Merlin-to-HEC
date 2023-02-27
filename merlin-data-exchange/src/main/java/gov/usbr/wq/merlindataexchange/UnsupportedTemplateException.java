package gov.usbr.wq.merlindataexchange;

final class UnsupportedTemplateException extends Exception
{
    UnsupportedTemplateException(String templateName, int templateId)
    {
        super("Failed to find matching template from Merlin for template name: " + templateName + " or template ID: " + templateId);
    }
}
