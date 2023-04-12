package gov.usbr.wq.merlindataexchange.io.wq;

import gov.usbr.wq.dataaccess.model.DataWrapper;

import java.util.HashMap;
import java.util.Map;

final class IndexedProfileDataWrappers extends HashMap<Integer, DataWrapper>
{
    IndexedProfileDataWrappers(Map<Integer, DataWrapper> indexedDataWrapperMap)
    {
        super.putAll(indexedDataWrapperMap);
    }

    IndexedProfileDataWrappers()
    {
        super();
    }

}
