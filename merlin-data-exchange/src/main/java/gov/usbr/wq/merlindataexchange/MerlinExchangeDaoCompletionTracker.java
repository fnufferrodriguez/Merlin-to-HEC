package gov.usbr.wq.merlindataexchange;

public final class MerlinExchangeDaoCompletionTracker
{
    private static final int TASKS_TO_PERFORM_PER_MEASURE = 2; //1 for read and 1 for write
    private final int _numberOfMeasuresToComplete;
    private final int _percentCompleteBeforeReadAndWrite;
    private int _numberCompleted = 0;

    public MerlinExchangeDaoCompletionTracker(int numberOfMeasuresToComplete, int percentCompleteBeforeReadAndWrite)
    {
        _numberOfMeasuresToComplete = numberOfMeasuresToComplete;
        _percentCompleteBeforeReadAndWrite = percentCompleteBeforeReadAndWrite;
    }

    public int readWriteTaskCompleted()
    {
        _numberCompleted ++;
        int totalNumOfTasksToBeCompleted = _numberOfMeasuresToComplete * TASKS_TO_PERFORM_PER_MEASURE;
        int weightForReadWriteTasks = 100 - _percentCompleteBeforeReadAndWrite;
        int weightedCompletedPercentage = (int) (weightForReadWriteTasks * ((double)_numberCompleted/totalNumOfTasksToBeCompleted)); //convert to percentage int
        return weightedCompletedPercentage + _percentCompleteBeforeReadAndWrite;
    }

}
