package group.bison.kafka.rebalancer.config;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.springframework.batch.integration.config.annotation.EnableBatchIntegration;
import org.springframework.batch.item.kafka.KafkaItemWriter;
import org.springframework.batch.item.kafka.builder.KafkaItemWriterBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.channel.QueueChannel;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.IntegrationFlows;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;

import group.bison.kafka.rebalancer.impl.ProducePartitioner;
import group.bison.kafka.rebalancer.impl.ProduceRebalancer;
import group.bison.kafka.rebalancer.utils.JsonUtil;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Configuration
@EnableBatchIntegration
public class ProduceRebalanceConfig {
    
    @Autowired
    private KafkaProperties kafkaProperties;

    @Bean
    public QueueChannel channel() {
        return new QueueChannel();
    }

    @Bean
	public IntegrationFlow produceRebalanceFlow() {
        String topic = "test-data";
        ProduceRebalancer produceRebalancer = new ProduceRebalancer(topic, (message) -> {
            Map map = JsonUtil.fromJson(message.toString(), HashMap.class);
            return (String)map.get("traceId");
         }, (key) -> key.hashCode() % 512);

        Properties producerProps = new Properties();
        producerProps.putAll(kafkaProperties.buildProducerProperties());
        producerProps.setProperty(ProducerConfig.PARTITIONER_CLASS_CONFIG, ProducePartitioner.class.getName());
        KafkaTemplate kafkaTemplate = new KafkaTemplate<>(new DefaultKafkaProducerFactory(producerProps));

        KafkaItemWriter<Long, Object> kafkaItemWriter = new KafkaItemWriterBuilder<Long, Object>().kafkaTemplate(kafkaTemplate).itemKeyMapper(obj -> produceRebalancer.generateMessageKey(produceRebalancer)).build();

		return IntegrationFlows
				.from(channel())
				.handle(kafkaItemWriter)
				.get();
	}
}
