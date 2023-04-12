package gov.usbr.wq.merlindataexchange.io.wq;

import java.time.ZonedDateTime;
import java.util.List;

final class ProfileSample
{
    private final ZonedDateTime _dateTime;
    private final List<ProfileConstituentData> _constituentDataList;

    public ProfileSample(ZonedDateTime dateTime, List<ProfileConstituentData> constituentDataList)
    {
        _dateTime = dateTime;
        _constituentDataList = constituentDataList;
    }

    ZonedDateTime getDateTime()
    {
        return _dateTime;
    }

    List<ProfileConstituentData> getConstituentDataList()
    {
        return _constituentDataList;
    }
}
