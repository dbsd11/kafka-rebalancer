package group.bison.springbatch.job;

import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.configuration.JobRegistry;
import org.springframework.batch.core.configuration.annotation.BatchConfigurer;
import org.springframework.batch.core.listener.JobExecutionListenerSupport;
import org.springframework.boot.autoconfigure.batch.JobExecutionEvent;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.ApplicationEventPublisherAware;
import org.springframework.context.ApplicationListener;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Data
public class AfterJobExecutionListenerSupport extends JobExecutionListenerSupport implements ApplicationEventPublisherAware, ApplicationListener<JobExecutionEvent> {

    private BatchConfigurer batchConfig;

    private JobRegistry jobRegistry;

    private ApplicationEventPublisher publisher;

    @Override
    public void afterJob(JobExecution jobExecution) {
        String jobName = jobExecution.getJobInstance().getJobName();
        log.info("afterJob {} exitStatus:{} exceptions:{} {}", jobName, jobExecution.getExitStatus(), jobExecution.getAllFailureExceptions());
        
        try {
            batchConfig.getJobRepository().update(jobExecution);
        } catch (Exception e) {
            log.error("failed update jobExecution", e);
        }

        try {
            Job job = jobRegistry.getJob(jobName);
            if(job != null && job.isRestartable() && batchConfig != null && batchConfig.getJobLauncher() != null) {
                // try to restart job
                log.info("try to restart job {}", jobName);

                // restart not completed status
                jobExecution.setStatus(BatchStatus.STOPPED);
                batchConfig.getJobRepository().update(jobExecution);

                JobExecution restartJobExecution = batchConfig.getJobLauncher().run(job, jobExecution.getJobParameters());
                if(publisher != null) {
                    publisher.publishEvent(new JobExecutionEvent(restartJobExecution));
                }
            }
        } catch (Exception e) {
            log.error("failed to restart job {}", jobName, e);
        }
    }

    @Override
    public void onApplicationEvent(JobExecutionEvent event) {
        JobExecution jobExecution = event.getJobExecution();
        if(!jobExecution.isRunning()) {
            afterJob(jobExecution);
        }
    }

	@Override
	public void setApplicationEventPublisher(ApplicationEventPublisher applicationEventPublisher) {
        this.publisher = applicationEventPublisher;
	}
}
