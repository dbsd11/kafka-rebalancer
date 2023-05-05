package group.bison.kafka.rebalancer.config;

import group.bison.kafka.rebalancer.impl.ProduceRebalancer;
import group.bison.kafka.rebalancer.utils.JsonUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

@Slf4j
@Configuration
@EnableBatchIntegration
public class ProduceRebalanceConfig {
    
    @Autowired
    private KafkaProperties kafkaProperties;

    @Bean
    public QueueChannel produceChannel() {
        return new QueueChannel();
    }

    @Bean
	public IntegrationFlow produceRebalanceFlow() {
        String topic = kafkaProperties.getConsumer().getProperties().get("topics.0");
        ProduceRebalancer produceRebalancer = new ProduceRebalancer(topic, (message) -> {
            Map map = JsonUtil.fromJson(String.valueOf(message), HashMap.class);
            return (String)map.get("field1");
         }, (key) -> key.hashCode() % 512);

        Properties producerProps = new Properties();
        producerProps.putAll(kafkaProperties.buildProducerProperties());
        KafkaTemplate kafkaTemplate = new KafkaTemplate<>(new DefaultKafkaProducerFactory(producerProps));
        kafkaTemplate.setDefaultTopic(topic);

        KafkaItemWriter<String, Object> kafkaItemWriter = new KafkaItemWriterBuilder<String, Object>().kafkaTemplate(kafkaTemplate).itemKeyMapper(obj -> produceRebalancer.generateMessageKey(obj)).build();

		return IntegrationFlows
				.from(produceChannel())
                .filter((messageInfo) -> {
                    if(!(messageInfo instanceof Map)) {
                        return false;
                    }
                    return StringUtils.equalsIgnoreCase((String)((Map) messageInfo).get("topic"), topic);
                })
                .transform((messageInfo) -> Collections.singletonList((String)((Map) messageInfo).get("message")))
                .handle(kafkaItemWriter)
				.get();
	}
}
