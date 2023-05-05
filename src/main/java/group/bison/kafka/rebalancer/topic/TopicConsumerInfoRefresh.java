package group.bison.kafka.rebalancer.topic;

import java.util.Map;

public abstract class TopicConsumerInfoRefresh {

    private String topic;

    private String consumerGroup;

    public TopicConsumerInfoRefresh() {}

    public TopicConsumerInfoRefresh(String topic, String consumerGroup) {
        this.topic = topic;
        this.consumerGroup = consumerGroup;
    }

    public String topic() {
        return this.topic;
    }

    public String getConsumerGroup() {
        return this.consumerGroup;
    }
    
    public abstract void refreshTopicPartitionConsumer(Map<Integer, String> topicPartitionConsumerMap);
}
