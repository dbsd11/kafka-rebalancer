package group.bison.kafka.rebalancer.topic;

public abstract class TopicInfoRefresh {

    private String topic;

    public TopicInfoRefresh() {}

    public TopicInfoRefresh(String topic) {
        this.topic = topic;
    }

    public String topic() {
        return topic;
    }
    
   public abstract void refreshTopicPartitionNum(int topicPartitionNum);
}
