package group.bison.kafka.rebalancer.impl;

import java.util.concurrent.atomic.AtomicLong;

import group.bison.kafka.rebalancer.policy.MessageKeyPartitionPolicy;
import group.bison.kafka.rebalancer.policy.MessageKeyPolicy;
import group.bison.kafka.rebalancer.tools.KafkaInfoFetcher;
import group.bison.kafka.rebalancer.topic.TopicInfoRefresh;
import lombok.Data;

@Data
public class ProduceRebalancer {

    private final String topic;

    private int topicPartitionNum = 1;

    private final MessageKeyPolicy messageKeyPolicy;

    private final MessageKeyPartitionPolicy messageKeyPartitionPolicy;

    private AtomicLong keyInfactorAtom = new AtomicLong();

    public ProduceRebalancer(String topic, MessageKeyPolicy messageKeyPolicy, MessageKeyPartitionPolicy messageKeyPartitionPolicy) {
        this.topic = topic;
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
        ProducePartitioner.setProduceRebalancer(topic, this);
    }

    public String generateMessageKey(Object message) {
        return messageKeyPolicy.computeKey(message);
    }

    public int computeMessagePartition(Object message) {
        String key = messageKeyPolicy.computeKey(message);
        int partition = messageKeyPartitionPolicy.computePartition(key);
        int topicPartition = partition % topicPartitionNum;
        return topicPartition;
    }
}
