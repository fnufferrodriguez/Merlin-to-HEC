package gov.usbr.wq.merlindataexchange.io.wq;

import java.util.List;
import java.util.Objects;

final class ProfileConstituent
{
    private final List<Double> _dataValues;
    private final String _parameter;
    private final String _unit;

    ProfileConstituent(String parameter, List<Double> dataValues, String unit)
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

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
        {
            return true;
        }
        if (o == null || getClass() != o.getClass())
        {
            return false;
        }
        ProfileConstituent that = (ProfileConstituent) o;
        return Objects.equals(_dataValues, that._dataValues) && Objects.equals(_parameter, that._parameter) && Objects.equals(_unit, that._unit);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(_dataValues, _parameter, _unit);
    }
}
