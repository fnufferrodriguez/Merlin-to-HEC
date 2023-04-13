package gov.usbr.wq.merlindataexchange.io.wq;

import gov.usbr.wq.dataaccess.model.DataWrapper;

import java.util.ArrayList;

final class MerlinProfileDataWrappers extends ArrayList<DataWrapper>
{
    private boolean _removeFirstProfile = false;
    private boolean _removeLastProfile = false;

    boolean removeFirstProfile()
    {
        return _removeFirstProfile;
    }

    void setRemoveFirstProfile()
    {
        _removeFirstProfile = true;
    }

    boolean removeLastProfile()
    {
        return _removeLastProfile;
    }

    void setRemoveLastProfile()
    {
        _removeLastProfile = true;
    }
}
