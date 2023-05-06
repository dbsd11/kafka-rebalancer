package group.bison.kafka.rebalancer.impl;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;

import group.bison.kafka.rebalancer.policy.MessageKeyPartitionPolicy;
import group.bison.kafka.rebalancer.policy.MessageKeyPolicy;
import group.bison.kafka.rebalancer.tools.KafkaInfoFetcher;
import group.bison.kafka.rebalancer.topic.TopicConsumerInfoRefresh;
import group.bison.kafka.rebalancer.topic.TopicInfoRefresh;
import group.bison.kafka.rebalancer.utils.InetUtils;
import lombok.Data;

@Data
public class ConsumeRebalancer {

    private final String topic;

    private final String consumerGroup;

    private int topicPartitionNum = 1;

    private Map<Integer, String> topicPartition2ConsumerMap = new HashMap<>();

    private final MessageKeyPolicy messageKeyPolicy;

    private final MessageKeyPartitionPolicy messageKeyPartitionPolicy;

    public ConsumeRebalancer(String topic, String consumerGroup, MessageKeyPolicy messageKeyPolicy, MessageKeyPartitionPolicy messageKeyPartitionPolicy) {
        this.topic = topic;
        this.consumerGroup = consumerGroup;
        this.messageKeyPolicy = messageKeyPolicy;
        this.messageKeyPartitionPolicy = messageKeyPartitionPolicy;

        init();
    }

    public void init() {
        KafkaInfoFetcher.addTopicInfoRefresh(new TopicInfoRefresh(topic) {
            @Override
            public void refreshTopicPartitionNum(int newTopicPartitionNum) {
                topicPartitionNum = newTopicPartitionNum;
            }
        });

        KafkaInfoFetcher.addTopicConsumerInfoRefresh(new TopicConsumerInfoRefresh(topic, consumerGroup) {

            @Override
            public void refreshTopicPartitionConsumer(Map<Integer, String> newTopicPartitionConsumerMap) {
                topicPartition2ConsumerMap = newTopicPartitionConsumerMap;
            }
        });
    }

    public String computeConsumerInstance(Object message) {
        String key = messageKeyPolicy.computeKey(message);
        int partition = messageKeyPartitionPolicy.computePartition(key);
        int topicPartition = partition % topicPartitionNum;
        String consumerInstance = topicPartition2ConsumerMap.get(topicPartition);
        return InetUtils.isLocalAddress(consumerInstance) ? null : consumerInstance;
    }

}
