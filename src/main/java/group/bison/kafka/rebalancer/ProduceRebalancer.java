package group.bison.kafka.rebalancer;

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

    public void addKafkaInfoFetcherRefresh() {
        KafkaInfoFetcher.addTopicInfoRefresh(new TopicInfoRefresh(topic) {
            @Override
            public void refreshTopicPartitionNum(int newTopicPartitionNum) {
                topicPartitionNum = newTopicPartitionNum;
            }
        });
    }

    public Long generateMessageKey(Object message) {
        String key = messageKeyPolicy.computeKey(message);
        int partition = messageKeyPartitionPolicy.computePartition(key);
        int topicPartition = partition % topicPartitionNum;
        long topicMessageKey = 0;
        while ((topicMessageKey = keyInfactorAtom.incrementAndGet()) % topicPartition != 0) {
            if (topicMessageKey > Long.MAX_VALUE / 2) {
                keyInfactorAtom.set(0);
            }
        }
        return topicMessageKey;
    }
}
