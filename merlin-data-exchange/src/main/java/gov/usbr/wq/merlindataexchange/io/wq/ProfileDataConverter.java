package gov.usbr.wq.merlindataexchange.io.wq;

import gov.usbr.wq.merlindataexchange.configuration.DataStoreProfile;

import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

final class ProfileDataConverter
{

    private static final String SIGNIFICANT_CHANGE_TIME_STEP_MULTIPLE_PROPERTY = "merlin.dataexchange.reader.profile.significantchange.timestep.multiple";
    private static final String SIGNIFICANT_CHANGE_DEPTH_PERCENT_DECREASE_PROPERTY = "merlin.dataexchange.reader.profile.significantchange.depth.decrease.percent";

    private ProfileDataConverter()
    {
        throw new AssertionError("Utility Class. Don't instantiate");
    }

    private static ZonedDateTime calculateProfileDateTime(List<ZonedDateTime> constituentDataList)
    {
        return constituentDataList.get(0);
    }

    static List<ProfileSample> splitDataIntoProfileSamples(List<ProfileConstituent> constituents, List<ZonedDateTime> readingDateTimes, int maxTimeStep, boolean removeFirstProfile, boolean removeLastProfile)
    {
        List<List<ZonedDateTime>> dateTimeGroups = separateDateTimeGroups(constituents, readingDateTimes, maxTimeStep);
        List<ProfileSample> retVal = new ArrayList<>();
        List<List<ProfileConstituent>> separatedProfileConstituents = new ArrayList<>();
        for(ProfileConstituent constituent : constituents)
        {
            List<List<Double>> separatedValuesList = separateValues(constituent.getDataValues(), dateTimeGroups);
            List<ProfileConstituent> profileConstituentSubGroups = new ArrayList<>();
            for(List<Double> valuesGroup : separatedValuesList)
            {
                ProfileConstituent profileConstituentSubGroup = new ProfileConstituent(constituent.getParameter(), valuesGroup, constituent.getUnit());
                profileConstituentSubGroups.add(profileConstituentSubGroup);
            }
            separatedProfileConstituents.add(profileConstituentSubGroups);
        }
        //go down groups
        for(int groupIndex =0; groupIndex < dateTimeGroups.size(); groupIndex++)
        {
            List<ProfileConstituent> constituentsAtGroupIndex = new ArrayList<>();
            //go across columns
            for(List<ProfileConstituent> constituentSplitUp : separatedProfileConstituents)
            {
                //collect subgroup in each column into list
                constituentsAtGroupIndex.add(constituentSplitUp.get(groupIndex));
            }
            ZonedDateTime profileSampleDateTime = calculateProfileDateTime(dateTimeGroups.get(groupIndex));
            retVal.add(new ProfileSample(profileSampleDateTime, constituentsAtGroupIndex));
        }
        if(removeFirstProfile)
        {
            retVal.remove(0);
        }
        if(!retVal.isEmpty() && removeLastProfile)
        {
            retVal.remove(retVal.size()-1);
        }
        return retVal;
    }

    private static List<List<Double>> separateValues(List<Double> values, List<List<ZonedDateTime>> dateTimeGroups)
    {
        List<List<Double>> result = new ArrayList<>();
        int index = 0;
        for (List<ZonedDateTime> dateTimeGroup : dateTimeGroups)
        {
            int groupSize = dateTimeGroup.size();
            List<Double> sublist = new ArrayList<>();
            for (int i = 0; i < groupSize; i++)
            {
                sublist.add(values.get(index));
                index++;
            }
            result.add(sublist);
        }
        return result;
    }

    private static List<List<ZonedDateTime>> separateDateTimeGroups(List<ProfileConstituent> constituents, List<ZonedDateTime> readingDateTimes, int maxTimeStep)
    {
        double depthPercentDecrease = getSignificantChangeDepthPercentDecrease();
        List<List<ZonedDateTime>> separatedDateTimes = new ArrayList<>();
        ZonedDateTime previousTime = null;
        Optional<ProfileConstituent> depthConstituentOpt = constituents.stream()
                .filter(c -> c.getParameter().equalsIgnoreCase(DataStoreProfile.DEPTH))
                .findFirst();
        if(depthConstituentOpt.isPresent())
        {
            ProfileConstituent profileConstituent = depthConstituentOpt.get();
            for (int i = 0; i < readingDateTimes.size(); i++)
            {
                ZonedDateTime currentTime = readingDateTimes.get(i);
                if (previousTime == null)
                {
                    separatedDateTimes.add(new ArrayList<>());
                    previousTime = currentTime;
                    separatedDateTimes.get(separatedDateTimes.size()-1).add(previousTime);
                }
                else
                {
                    Double currentDepth = profileConstituent.getDataValues().get(i);
                    Double previousDepth = profileConstituent.getDataValues().get(i - 1);
                    if (isSignificantTimeChange(previousTime, currentTime, maxTimeStep) ||
                            isSignificantDepthChange(currentDepth, previousDepth))
                    {
                        separatedDateTimes.add(new ArrayList<>());
                        separatedDateTimes.get(separatedDateTimes.size()-1).add(currentTime);
                    }
                    else
                    {
                        separatedDateTimes.get(separatedDateTimes.size()-1).add(currentTime);
                    }
                    previousTime = currentTime;
                }
            }
        }
        return separatedDateTimes;
    }

    private static boolean isSignificantDepthChange(Double currentDepth, Double previousDepth)
    {
        return isDecreaseByPercent(currentDepth, previousDepth, getSignificantChangeDepthPercentDecrease());
    }

    private static boolean isSignificantTimeChange(ZonedDateTime previousTime, ZonedDateTime currentTime, int maxTimeStep)
    {
        Duration timeDiff = Duration.between(previousTime, currentTime);
        long timeStepMultiple = getSignificantChangeTimeStepMultiple();
        return timeDiff.compareTo(Duration.ofMinutes(timeStepMultiple * maxTimeStep)) >= 0;
    }

    static long getSignificantChangeTimeStepMultiple()
    {
        String timeStepMultipleProperty = System.getProperty(SIGNIFICANT_CHANGE_TIME_STEP_MULTIPLE_PROPERTY);
        long timeStepMultiple = 6;
        if(timeStepMultipleProperty != null)
        {
            timeStepMultiple = Integer.parseInt(timeStepMultipleProperty);
        }
        return timeStepMultiple;
    }

    private static double getSignificantChangeDepthPercentDecrease()
    {
        String depthPercentDecreaseProperty = System.getProperty(SIGNIFICANT_CHANGE_DEPTH_PERCENT_DECREASE_PROPERTY);
        double depthPercentDecrease = 50.0;
        if(depthPercentDecreaseProperty != null)
        {
            depthPercentDecrease = Double.parseDouble(depthPercentDecreaseProperty);
        }
        return depthPercentDecrease;
    }

    private static boolean isDecreaseByPercent(double value1, double value2, double percent)
    {
        boolean retVal = false;
        if (value1 < value2)
        {
            double decreasePercentage = (value2 - value1) / value2 * 100.0;
            retVal = decreasePercentage >= percent;
        }
        return retVal;
    }

    static boolean isDifferenceSignificantChange(ZonedDateTime previousDateTime, ZonedDateTime currentDateTime, int maxTimeStep, Double previousDepth, Double currentDepth)
    {
        return isSignificantTimeChange(previousDateTime, currentDateTime, maxTimeStep)
                || isSignificantDepthChange(currentDepth, previousDepth);
    }

}
