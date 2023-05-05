package group.bison.springbatch.job;

import javax.sql.DataSource;

import org.springframework.batch.core.configuration.annotation.DefaultBatchConfigurer;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.explore.support.MapJobExplorerFactoryBean;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.support.SimpleJobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.repository.support.MapJobRepositoryFactoryBean;
import org.springframework.batch.support.transaction.ResourcelessTransactionManager;
import org.springframework.core.task.TaskExecutor;

import lombok.Data;

@Data
public class InMemoryBatchConfigurer extends DefaultBatchConfigurer {

    private TaskExecutor taskExecutor;

    @Override
    public void setDataSource(DataSource dataSource) {
    }

    @Override
    protected JobRepository createJobRepository() throws Exception {
        MapJobRepositoryFactoryBean jobRepositoryFactory = new MapJobRepositoryFactoryBean(new ResourcelessTransactionManager());
		jobRepositoryFactory.afterPropertiesSet();
		return jobRepositoryFactory.getObject();
    }

    @Override
    protected JobExplorer createJobExplorer() throws Exception {
        MapJobExplorerFactoryBean jobExplorerFactory = new MapJobExplorerFactoryBean((MapJobRepositoryFactoryBean)getJobRepository());
        jobExplorerFactory.afterPropertiesSet();
        return jobExplorerFactory.getObject();
    }

    @Override
    protected JobLauncher createJobLauncher() throws Exception {
        SimpleJobLauncher jobLauncher = (SimpleJobLauncher) super.createJobLauncher();
        jobLauncher.setTaskExecutor(taskExecutor);
        return jobLauncher;
    }
}
