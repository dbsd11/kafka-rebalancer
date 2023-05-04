package group.bison.kafka.rebalancer.config;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.BatchConfigurer;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.integration.config.annotation.EnableBatchIntegration;
import org.springframework.batch.item.NonTransientResourceException;
import org.springframework.batch.item.ParseException;
import org.springframework.batch.item.UnexpectedInputException;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.kafka.KafkaItemReader;
import org.springframework.batch.item.kafka.builder.KafkaItemReaderBuilder;
import org.springframework.batch.item.support.AbstractItemStreamItemReader;
import org.springframework.batch.item.support.AbstractItemStreamItemWriter;
import org.springframework.batch.repeat.policy.SimpleCompletionPolicy;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.batch.BasicBatchConfigurer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.IntegrationFlows;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.kafka.listener.ConsumerProperties;
import org.springframework.lang.Nullable;
import org.springframework.util.CollectionUtils;

import group.bison.kafka.rebalancer.ConsumeRebalancer;
import group.bison.kafka.rebalancer.remote_mq.MemoryMq;
import group.bison.kafka.rebalancer.tools.KafkaInfoFetcher;
import group.bison.kafka.rebalancer.utils.HttpUtils;
import group.bison.kafka.rebalancer.utils.JsonUtil;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Configuration
@EnableBatchProcessing
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
    @Primary
    public BatchConfigurer batchConfigurer(Executor executor) {
        return new BasicBatchConfigurer();
    }

    @Bean
	public Job job() {
		return jobBuilderFactory.get("consumeRebalanceJob")
			.incrementer(new RunIdIncrementer())
			.start(startConsumeRebalance())
			.build();
	}

    @Bean
	public Job pullRemoteMessageJob() {
		return jobBuilderFactory.get("pullRemoteMessageJob")
			.incrementer(new RunIdIncrementer())
			.start(pullRemoteMessage())
			.build();
	}

    @Bean
    @StepScope
    public Step startConsumeRebalance() {
        Properties consumerProps = new Properties();
        consumerProps.putAll(consumerProperties.getKafkaConsumerProperties());
        String topic = consumerProperties.getTopics()[0];
        MemoryMq.batchAddKafkaInfoFetcherRefresh(Collections.singletonList(topic));
        ConsumeRebalancer consumeRebalancer = new ConsumeRebalancer(topic, (message) -> {
           Map map = JsonUtil.fromJson(message.toString(), HashMap.class);
           return (String)map.get("traceId");
        }, (key) -> key.hashCode() % 512);
        KafkaItemReader<Long, Object> kafkaItemReader = new KafkaItemReaderBuilder<Long, Object>().consumerProperties(consumerProps).name("test-data-reader").saveState(true).topic(topic).build();
        return stepBuilderFactory.get("startConsumeRebalance").chunk(1000).reader(kafkaItemReader).writer(new AbstractItemStreamItemWriter() {

            @Override
            public void write(List items) throws Exception {
                if(CollectionUtils.isEmpty(items)) {
                    return;
                }
                items.forEach(item -> {
                    String consumerInstance = consumeRebalancer.computeConsumerInstance(item);
                    MemoryMq.push(topic, consumerInstance, item);
                });

                //wait for consume
            }
            
        }).build();
    }

    @Bean
    @StepScope
    public Step pullRemoteMessage() {
        String topic = consumerProperties.getTopics()[0];
        String localConsumer = "";

        return stepBuilderFactory.get("pullRemoteMessage").chunk(new SimpleCompletionPolicy()).reader(new AbstractItemStreamItemReader() {

            private Iterator itemIt = null;

            @Override
            @Nullable
            public Object read()
                    throws Exception, UnexpectedInputException, ParseException, NonTransientResourceException {
                if(itemIt == null || !itemIt.hasNext()) {
                    List pulledRemoteMessageList = new LinkedList();
                    Map<Integer, String> currentTopicPartitionConsumerMap = KafkaInfoFetcher.getCurrentTopicPartitionConsumerMap(topic);
                    currentTopicPartitionConsumerMap.values().forEach((consumer) -> {
                        try {
                            String responseStr = HttpUtils.http2RequestWithPool("GET", String.join("",  "http://", consumer, "/kafka-rebalancer/pull/", topic, "/", localConsumer), null, null);
                            List remoteMessageList = (List)JsonUtil.fromJson(responseStr, HashMap.class).get("list");
                            pulledRemoteMessageList.addAll(0, remoteMessageList);
                        } catch (Exception e) {
                            log.error(e.getMessage(), e);
                        }
                    });
                    itemIt = pulledRemoteMessageList.iterator();
                }
                return (itemIt == null || !itemIt.hasNext()) ? null : itemIt.next();
            } 
        }).writer(new AbstractItemStreamItemWriter() {

            @Override
            public void write(List items) throws Exception {
                if(CollectionUtils.isEmpty(items)) {
                    return;
                }
                items.forEach(item -> {
                    MemoryMq.push(topic, null, item);
                });
            }
            
        }).build();
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
