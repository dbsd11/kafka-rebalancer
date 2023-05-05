package group.bison.kafka.rebalancer.config;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.integration.config.annotation.EnableBatchIntegration;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.IntegrationFlows;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.kafka.listener.ConsumerProperties;

import group.bison.kafka.rebalancer.ConsumeRebalancer;
import group.bison.kafka.rebalancer.remote_mq.MemoryMq;
import group.bison.kafka.rebalancer.utils.JsonUtil;
import group.bison.springbatch.config.OverideSpringBatchJobConfiguration;
import group.bison.springbatch.job.AfterJobExecutionListenerSupport;
import group.bison.springbatch.job.ScheduledCompletionPolicy;
import group.bison.springbatch.reader.RemoteItemReader;
import group.bison.springbatch.reader.SpringBatchKafkaItemReader;
import group.bison.springbatch.writer.LocalMqItemWriter;
import group.bison.springbatch.writer.RemoteMqItemWriter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Configuration
@Import(OverideSpringBatchJobConfiguration.class)
@EnableBatchIntegration
public class ConsumeRebalanceConfig {
    @Autowired
    private JobBuilderFactory jobBuilderFactory;
    @Autowired
    private StepBuilderFactory stepBuilderFactory;
    @Autowired
    private ConsumerProperties consumerProperties;
    @Autowired
    private NamedParameterJdbcTemplate jdbcTemplate;

    @Bean
	public Job job(AfterJobExecutionListenerSupport jobExecutionListener) {
		return jobBuilderFactory.get("consumeRebalanceJob")
			.incrementer(new RunIdIncrementer())
			.start(startConsumeRebalance())
            .listener(jobExecutionListener)
			.build();
	}

    @Bean
	public Job pullRemoteMessageJob(AfterJobExecutionListenerSupport jobExecutionListener) {
		return jobBuilderFactory.get("pullRemoteMessageJob")
			.incrementer(new RunIdIncrementer())
			.start(pullRemoteMessage())
            .listener(jobExecutionListener)
			.build();
	}

    @Bean
    @StepScope
    public Step startConsumeRebalance() {
        Properties consumerProps = new Properties();
        consumerProps.putAll(consumerProperties.getKafkaConsumerProperties());
        String topic = consumerProperties.getTopics()[0];
        SpringBatchKafkaItemReader kafkaItemReader = new SpringBatchKafkaItemReader(consumerProps, topic);

        MemoryMq.batchAddKafkaInfoFetcherRefresh(Collections.singletonList(topic));

        ConsumeRebalancer consumeRebalancer = new ConsumeRebalancer(topic, (message) -> {
           Map map = JsonUtil.fromJson(message.toString(), HashMap.class);
           return (String)map.get("traceId");
        }, (key) -> key.hashCode() % 512);
        
        return stepBuilderFactory.get("startConsumeRebalance").chunk(new ScheduledCompletionPolicy(1000)).reader(kafkaItemReader).writer(new RemoteMqItemWriter(topic, consumeRebalancer)).build();
    }

    @Bean
    @StepScope
    public Step pullRemoteMessage() {
        String topic = consumerProperties.getTopics()[0];
        return stepBuilderFactory.get("pullRemoteMessage").chunk(new ScheduledCompletionPolicy(1000)).reader(new RemoteItemReader(topic)).writer(new LocalMqItemWriter(topic)).build();
    }

    @Bean
	public IntegrationFlow consumeRebalanceFlow() {
        String topic = consumerProperties.getTopics()[0];
		return IntegrationFlows
				.from(MemoryMq.getOrCreateLocalChannel(topic))
				.handle(testDataWriter())
				.get();
	}

    @Bean
    @StepScope
    public JdbcBatchItemWriter testDataWriter() {
        JdbcBatchItemWriter jdbcBatchItemWriter = new JdbcBatchItemWriter();
        jdbcBatchItemWriter.setJdbcTemplate(jdbcTemplate);
        jdbcBatchItemWriter.afterPropertiesSet();
        return jdbcBatchItemWriter;
    }
}
