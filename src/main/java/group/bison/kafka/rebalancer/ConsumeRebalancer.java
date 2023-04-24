package group.bison.kafka.rebalancer;

import java.util.Map;

import group.bison.kafka.rebalancer.policy.MessageKeyPartitionPolicy;
import group.bison.kafka.rebalancer.policy.MessageKeyPolicy;
import group.bison.kafka.rebalancer.tools.KafkaInfoFetcher;
import group.bison.kafka.rebalancer.topic.TopicConsumerInfoRefresh;
import group.bison.kafka.rebalancer.topic.TopicInfoRefresh;

import lombok.Data;

@Data
public class ConsumeRebalancer {

    private final String topic;

    private int topicPartitionNum = 1;

    private Map<Integer, String> topicPartition2ConsumerMap;

    private final MessageKeyPolicy messageKeyPolicy;

    private final MessageKeyPartitionPolicy messageKeyPartitionPolicy;

    public void addKafkaInfoFetcherRefresh() {
        KafkaInfoFetcher.addTopicInfoRefresh(new TopicInfoRefresh(topic) {
            @Override
            public void refreshTopicPartitionNum(int newTopicPartitionNum) {
                topicPartitionNum = newTopicPartitionNum;
            }
        });

        KafkaInfoFetcher.addTopicConsumerInfoRefresh(new TopicConsumerInfoRefresh(topic) {

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
        return topicPartition2ConsumerMap.get(topicPartition);
    }

}
