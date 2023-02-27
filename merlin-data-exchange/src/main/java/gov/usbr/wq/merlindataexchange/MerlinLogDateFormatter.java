package gov.usbr.wq.merlindataexchange;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;

final class MerlinLogDateFormatter
{
    private MerlinLogDateFormatter()
    {
        throw new AssertionError("Utility class. Don't instantiate");
    }

    static String formatInstant(Instant instant)
    {
        instant = instant.truncatedTo(ChronoUnit.SECONDS);
        return instant.atZone(ZoneId.systemDefault()).format( DateTimeFormatter.ISO_OFFSET_DATE_TIME);
    }

}
