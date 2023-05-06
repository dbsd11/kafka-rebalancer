package group.bison.springbatch.config;

import java.lang.reflect.Field;
import java.util.concurrent.Executor;

import org.springframework.batch.core.configuration.JobRegistry;
import org.springframework.batch.core.configuration.annotation.BatchConfigurer;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.SimpleBatchConfiguration;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.support.JobRegistryBeanPostProcessor;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.support.BeanDefinitionValidationException;
import org.springframework.boot.autoconfigure.task.TaskExecutionAutoConfiguration;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.core.task.support.TaskExecutorAdapter;

import group.bison.springbatch.job.AfterJobExecutionListenerSupport;
import group.bison.springbatch.job.InMemoryBatchConfigurer;

public class OverideSpringBatchJobConfiguration implements ApplicationContextAware, InitializingBean {

	private SimpleBatchConfiguration simpleBatchConfiguration;

	@Bean
    @Primary
    @Order(Ordered.HIGHEST_PRECEDENCE)
    public BatchConfigurer batchConfigurer(@Autowired @Qualifier(value = TaskExecutionAutoConfiguration.APPLICATION_TASK_EXECUTOR_BEAN_NAME) Executor executor) throws Exception {
        InMemoryBatchConfigurer batchConfigurer = new InMemoryBatchConfigurer();
        batchConfigurer.setTaskExecutor(new TaskExecutorAdapter(executor));
        return batchConfigurer;
    }

	@Bean
    public AfterJobExecutionListenerSupport jobListener(@Autowired BatchConfigurer batchConfig, @Autowired  JobRegistry jobRegistry) {
        AfterJobExecutionListenerSupport jobListener = new AfterJobExecutionListenerSupport();
        jobListener.setBatchConfig(batchConfig);
        jobListener.setJobRegistry(jobRegistry);
        return jobListener;
    }

	@Bean
	public JobBuilderFactory jobBuilders() throws Exception {
		return simpleBatchConfiguration.jobBuilders();
	}

	@Bean
	public StepBuilderFactory stepBuilders() throws Exception {
		return simpleBatchConfiguration.stepBuilders();
	}

	@Bean
	public JobRepository jobRepository() throws Exception {
		return simpleBatchConfiguration.jobRepository();
	}

	@Bean
	public JobLauncher jobLauncher() throws Exception {
		return simpleBatchConfiguration.jobLauncher();
	}

	@Bean
	public JobRegistry jobRegistry() throws Exception {
		return simpleBatchConfiguration.jobRegistry();
	}

	@Bean
	public JobRegistryBeanPostProcessor jobRegistryBeanPostProcessor() throws Exception {
		JobRegistryBeanPostProcessor jobRegistryBeanPostProcessor = new JobRegistryBeanPostProcessor();
		jobRegistryBeanPostProcessor.setJobRegistry(jobRegistry());
		return jobRegistryBeanPostProcessor;
	} 

	@Bean
	public JobExplorer jobExplorer() {
		return simpleBatchConfiguration.jobExplorer();
	}

	@Override
	public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
		this.simpleBatchConfiguration = new SimpleBatchConfiguration();
		try {
			Field contextField = SimpleBatchConfiguration.class.getDeclaredField("context");
			contextField.setAccessible(true);
			contextField.set(simpleBatchConfiguration, applicationContext);
		} catch (Exception e) {
			throw new BeanDefinitionValidationException("no context field in SimpleBatchConfiguration");
		}
	}

	@Override
	public void afterPropertiesSet() throws Exception {
		if(simpleBatchConfiguration != null) {
			simpleBatchConfiguration.afterPropertiesSet();
		}
	}
}
