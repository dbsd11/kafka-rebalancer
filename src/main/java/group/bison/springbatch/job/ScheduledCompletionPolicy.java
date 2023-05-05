package group.bison.springbatch.job;

import org.springframework.batch.repeat.RepeatContext;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.batch.repeat.policy.SimpleCompletionPolicy;

public class ScheduledCompletionPolicy extends SimpleCompletionPolicy {

    private long startTimestamp;

    public ScheduledCompletionPolicy(int chunkSize) {
        super(chunkSize);
    }

    @Override
    public RepeatContext start(RepeatContext context) {
        startTimestamp = System.currentTimeMillis();
        return super.start(context);
    }

    @Override
    public boolean isComplete(RepeatContext context) {
        return (System.currentTimeMillis() - startTimestamp) > 1000 || super.isComplete(context);
    }

    @Override
    public boolean isComplete(RepeatContext context, RepeatStatus result) {
        return (System.currentTimeMillis() - startTimestamp) > 1000
                || super.isComplete(context, result);
    }
}
