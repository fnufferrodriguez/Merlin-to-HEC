package gov.usbr.wq.merlindataexchange.io.wq;

import gov.usbr.wq.dataaccess.model.DataWrapper;

final class ProfileSample
{
    private final DataWrapper _depthData;
    private final DataWrapper _tempData;

    public ProfileSample(DataWrapper depthData, DataWrapper tempData)
    {
        _depthData = depthData;
        _tempData = tempData;
    }

    DataWrapper getDepthData()
    {
        return _depthData;
    }

    DataWrapper getTempData()
    {
        return _tempData;
    }
}
