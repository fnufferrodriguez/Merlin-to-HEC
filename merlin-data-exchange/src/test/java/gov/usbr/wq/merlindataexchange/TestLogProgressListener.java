package gov.usbr.wq.merlindataexchange;


import hec.ui.ProgressListener;

import java.util.logging.ConsoleHandler;
import java.util.logging.Formatter;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

public final class TestLogProgressListener implements ProgressListener
{

    private static final Logger LOGGER = Logger.getLogger(TestLogProgressListener.class.getName());
    private int _progress = 0;

    public TestLogProgressListener()
    {
        LOGGER.setUseParentHandlers(false);
        LOGGER.addHandler(new ConsoleHandler()
        {
            @Override
            public Formatter getFormatter()
            {
                return new Formatter() {
                    @Override
                    public String format(LogRecord record)
                    {
                        return "PROGRESS LOG " + record.getMessage() + "\n";
                    }
                };
            }
        });
    }
    @Override
    public void start()
    {

    }

    @Override
    public void start(int i)
    {

    }

    @Override
    public void switchToIndeterminate()
    {

    }

    @Override
    public void setStayOnTop(boolean b)
    {

    }

    @Override
    public void switchToDeterminate(int i)
    {

    }

    @Override
    public void finish()
    {
    }

    @Override
    public void progress(int i)
    {
        _progress = i;
        LOGGER.info("Progress: " + i + "%");
    }

    @Override
    public void progress(String s)
    {
        LOGGER.info(s);
    }

    @Override
    public void progress(String s, MessageType messageType)
    {
        if(messageType == MessageType.ERROR)
        {
            LOGGER.info("ERROR: " + s);
        }
        else
        {
            LOGGER.info("(" + messageType + "): " + s);
        }
    }

    @Override
    public void progress(String s, int i)
    {

    }

    @Override
    public void progress(String s, MessageType messageType, int i)
    {
        progress(s + " (" + i + "%)", messageType);
    }

    @Override
    public void incrementProgress(int i)
    {

    }

    public int getProgress()
    {
        return _progress;
    }
}
