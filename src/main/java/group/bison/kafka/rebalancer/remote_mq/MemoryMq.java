package group.bison.kafka.rebalancer.remote_mq;

import java.util.ArrayDeque;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang3.StringUtils;
import org.springframework.integration.channel.QueueChannel;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.GenericMessage;
import org.springframework.util.CollectionUtils;

import group.bison.kafka.rebalancer.tools.KafkaInfoFetcher;
import group.bison.kafka.rebalancer.topic.TopicConsumerInfoRefresh;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MemoryMq {

    private static AtomicInteger autoIncreAtom = new AtomicInteger();
    private static Map<String, String> consumerKeyMap = new ConcurrentHashMap();
    private static Map<String, QueueChannel> consumerChannelMap = new ConcurrentHashMap<>();

    public static void initTopicMq(List<String> topicList, String consumerGroup) {
        batchAddKafkaInfoFetcherRefresh(topicList, consumerGroup);
    }

    public static void batchAddKafkaInfoFetcherRefresh(List<String> topicList, String consumerGroup) {
        if (CollectionUtils.isEmpty(topicList)) {
            return;
        }

        topicList.forEach(topic -> {
            KafkaInfoFetcher.addTopicConsumerInfoRefresh(generateTopicConsumerInfoRefresh(topic, consumerGroup));
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
        try {
            while ((message = messageChannel.receive(1000)) != null && messageList.size() <= pollLimit) {
                messageList.add(message.getPayload());
            }
        } catch (Exception e) {
            log.debug("pull timeout {}", e.getMessage());
        }
        
        return messageList;
    }

    public static QueueChannel getOrCreateLocalChannel(String topic) {
        String localConsumerKey = getConsumerKey(topic, null);
        QueueChannel localChannel = consumerChannelMap.computeIfAbsent(localConsumerKey, (localConsumerKeyStr) -> new QueueChannel(new ArrayDeque<>(10000)));
        return localChannel;
    }

    static String getConsumerKey(String topic, String consumer) {
        String topicWithConsumer = String.join("", topic, "-", StringUtils.defaultIfEmpty(consumer, "local"));
        return consumerKeyMap.computeIfAbsent(topicWithConsumer, (key) -> String.join("", topic, "-", String.valueOf(autoIncreAtom.getAndIncrement())));
    }

    static TopicConsumerInfoRefresh generateTopicConsumerInfoRefresh(String topic, String consumerGroup) {
        return new TopicConsumerInfoRefresh(topic, consumerGroup) {

            @Override
            public void refreshTopicPartitionConsumer(Map<Integer, String> topicPartitionConsumerMap) {
                Set<String> consumers = new HashSet<>(topicPartitionConsumerMap.values());
                consumers.forEach(consumer -> {
                    String consumerKey = getConsumerKey(topic, consumer);
                    consumerChannelMap.putIfAbsent(consumerKey, new QueueChannel(new ArrayDeque(10000)));
                });

                // check offline consumer
                consumerKeyMap.entrySet().removeIf(entry -> {
                    if(StringUtils.equals(entry.getValue(), getConsumerKey(topic, null))) {
                        return false;
                    }

                    String topicWithConsumer = entry.getKey();
                    boolean consumerOffline = consumers.stream().noneMatch(consumer -> StringUtils.containsIgnoreCase(topicWithConsumer, consumer));
                    return consumerOffline;
                });
            }
        };
    }

}
