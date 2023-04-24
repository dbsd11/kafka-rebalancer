package group.bison.kafka.rebalancer.tools;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

import group.bison.kafka.rebalancer.topic.TopicConsumerInfoRefresh;
import group.bison.kafka.rebalancer.topic.TopicInfoRefresh;
import io.netty.util.concurrent.Future;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class KafkaInfoFetcher {

    private static List<TopicInfoRefresh> topicInfoRefreshList = new LinkedList<>();

    private static List<TopicConsumerInfoRefresh> topicConsumerInfoRefreshList = new LinkedList<>();

    private static AtomicReference<Future> refreshKafkaInfoFutureAtom = new AtomicReference<Future>();

    static {
        refreshKafkaInfoFutureAtom.compareAndSet(null, (Future) CompletableFuture.runAsync(() -> {
            while (true) {
                try {
                    Thread.sleep(5000);
                    Map<String, Integer> topicPartitionFetchResultMap = new HashMap<>();
                    Map<String, Map<Integer, String>> topicPartitionConsumerFetchResultMap = new HashMap<>();

                    topicInfoRefreshList.forEach(topicInfoRefresh -> {
                        try {
                            String topic = topicInfoRefresh.topic();
                            Integer topicPartitionNum = topicPartitionFetchResultMap.computeIfAbsent(topic,
                                    (topicStr) -> getCurrentTopicPartitionNum(topicStr));
                            topicInfoRefresh.refreshTopicPartitionNum(topicPartitionNum);
                        } catch (Exception e) {
                            log.error(e.getMessage(), e);
                        }

                    });

                    topicConsumerInfoRefreshList.forEach(topicConsumerInfoRefresh -> {
                        try {
                            String topic = topicConsumerInfoRefresh.topic();
                            Map<Integer, String> topicConsumerMap = topicPartitionConsumerFetchResultMap
                                    .computeIfAbsent(topic,
                                            (topicStr) -> getCurrentTopicPartitionConsumerMap(topicStr));
                            topicConsumerInfoRefresh.refreshTopicPartitionConsumer(topicConsumerMap);
                        } catch (Exception e) {
                            log.error(e.getMessage(), e);
                        }
                    });
                } catch (Exception e) {
                    log.error(e.getMessage(), e);
                }
            }
        }));
    }

    public static int getCurrentTopicPartitionNum(String topic) {
        return 0;
    }

    public static Map<Integer, String> getCurrentTopicPartitionConsumerMap(String topic) {
        return Collections.emptyMap();
    }

    public static void addTopicInfoRefresh(TopicInfoRefresh topicInfoRefresh) {
        topicInfoRefreshList.add(topicInfoRefresh);
    }

    public static void addTopicConsumerInfoRefresh(TopicConsumerInfoRefresh topicConsumerInfoRefresh) {
        topicConsumerInfoRefreshList.add(topicConsumerInfoRefresh);


    }
}
