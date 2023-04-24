package group.bison.kafka.rebalancer.topic;

import java.util.Map;

public abstract class TopicConsumerInfoRefresh {

    private String topic;

    public TopicConsumerInfoRefresh() {}

    public TopicConsumerInfoRefresh(String topic) {
        this.topic = topic;
    }

    public String topic() {
        return this.topic;
    }
    
    public abstract void refreshTopicPartitionConsumer(Map<Integer, String> topicPartitionConsumerMap);
}
