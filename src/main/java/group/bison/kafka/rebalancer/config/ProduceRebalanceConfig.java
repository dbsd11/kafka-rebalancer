package group.bison.kafka.rebalancer.config;

import java.util.HashMap;
import java.util.Map;

import org.springframework.batch.integration.config.annotation.EnableBatchIntegration;
import org.springframework.batch.item.kafka.KafkaItemWriter;
import org.springframework.batch.item.kafka.builder.KafkaItemWriterBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.channel.QueueChannel;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.IntegrationFlows;
import org.springframework.kafka.core.KafkaTemplate;

import group.bison.kafka.rebalancer.ProduceRebalancer;
import group.bison.kafka.rebalancer.utils.JsonUtil;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Configuration
@EnableBatchIntegration
public class ProduceRebalanceConfig {
    @Autowired
    private KafkaTemplate kafkaTemplate;

    @Bean
    public QueueChannel channel() {
        return new QueueChannel();
    }

    @Bean
	public IntegrationFlow produceRebalanceFlow() {
        ProduceRebalancer produceRebalancer = new ProduceRebalancer(kafkaTemplate.getDefaultTopic(), (message) -> {
            Map map = JsonUtil.fromJson(message.toString(), HashMap.class);
            return (String)map.get("traceId");
         }, (key) -> key.hashCode() % 512);
        KafkaItemWriter<Long, Object> kafkaItemWriter = new KafkaItemWriterBuilder<Long, Object>().kafkaTemplate(kafkaTemplate).itemKeyMapper(obj -> produceRebalancer.generateMessageKey(produceRebalancer)).build();

		return IntegrationFlows
				.from(channel())
				.handle(kafkaItemWriter)
				.get();
	}
}
