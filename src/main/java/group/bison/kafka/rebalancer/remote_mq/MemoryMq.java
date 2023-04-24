package group.bison.kafka.rebalancer.remote_mq;

import java.util.ArrayDeque;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.lang3.StringUtils;
import org.springframework.integration.channel.QueueChannel;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.GenericMessage;
import org.springframework.util.CollectionUtils;

import group.bison.kafka.rebalancer.tools.KafkaInfoFetcher;
import group.bison.kafka.rebalancer.topic.TopicConsumerInfoRefresh;

public class MemoryMq {

    private static Map<String, QueueChannel> consumerChannelMap = new ConcurrentHashMap<>();

    public static void batchAddKafkaInfoFetcherRefresh(List<String> topicList) {
        if (CollectionUtils.isEmpty(topicList)) {
            return;
        }

        topicList.forEach(topic -> {
            KafkaInfoFetcher.addTopicConsumerInfoRefresh(generateTopicConsumerInfoRefresh(topic));
        });
    }

    public static boolean push(String topic, String consumer, Object message) {
        if(message == null) {
            return false;
        }

        String consumerKey = getConsumerKey(topic, consumer);
        if (!consumerChannelMap.containsKey(consumerKey)) {
            return false;
        }

        QueueChannel messageChannel = consumerChannelMap.get(consumerKey);
        return messageChannel.send(new GenericMessage(message));
    }

    public static List pull(String topic, String consumer, Integer batchSize) {
        String consumerKey = getConsumerKey(topic, consumer);
        if (!consumerChannelMap.containsKey(consumerKey)) {
            return Collections.emptyList();
        }

        QueueChannel messageChannel = consumerChannelMap.get(consumerKey);
        List messageList = new LinkedList<>();

        Message message = null;
        int pollLimit = batchSize != null ? batchSize : Integer.valueOf(1000);
        while ((message = messageChannel.receive()) != null && messageList.size() <= pollLimit) {
            messageList.add(message.getPayload());
        }
        return messageList;
    }

    public static QueueChannel getOrCreateLocalChannel(String topic) {
        String localConsumerKey = getConsumerKey(topic, null);
        QueueChannel localChannel = consumerChannelMap.computeIfAbsent(localConsumerKey, (localConsumerKeyStr) -> new QueueChannel(new ArrayDeque<>(10000)));
        return localChannel;
    }

    static String getConsumerKey(String topic, String consumer) {
        return String.join("", topic, "-", StringUtils.defaultIfEmpty(consumer, "local"));
    }

    static TopicConsumerInfoRefresh generateTopicConsumerInfoRefresh(String topic) {
        return new TopicConsumerInfoRefresh(topic) {

            @Override
            public void refreshTopicPartitionConsumer(Map<Integer, String> topicPartitionConsumerMap) {

                topicPartitionConsumerMap.values().forEach(consumer -> {
                    String consumerKey = getConsumerKey(topic, consumer);
                    consumerChannelMap.putIfAbsent(consumerKey, new QueueChannel(new ArrayDeque(10000)));
                });
            }
        };
    }

}