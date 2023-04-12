package gov.usbr.wq.merlindataexchange.io.wq;

import java.util.List;

final class ProfileConstituentData
{
    private final List<Double> _dataValues;
    private final String _parameter;
    private final String _unit;

    ProfileConstituentData(String parameter, List<Double> dataValues, String unit)
    {
        _dataValues = dataValues;
        _parameter = parameter;
        _unit = unit;
    }

    List<Double> getDataValues()
    {
        return _dataValues;
    }

    String getParameter()
    {
        return _parameter;
    }

    String getUnit()
    {
        return _unit;
    }
}
