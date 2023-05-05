package group.bison.kafka.rebalancer.tools;

import group.bison.kafka.rebalancer.topic.TopicConsumerInfoRefresh;
import group.bison.kafka.rebalancer.topic.TopicInfoRefresh;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.DescribeConsumerGroupsResult;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.utils.CopyOnWriteMap;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;

@Slf4j
@Component
public class KafkaInfoFetcher implements InitializingBean {

    private static KafkaInfoFetcher INSTANCE = null;

    @Autowired
    private KafkaProperties kafkaProperties;

    private static Map<String, Integer> topicPartitionFetchResultMap = new CopyOnWriteMap<>();

    private static Map<String, Map<Integer, String>> topicPartitionConsumerFetchResultMap = new CopyOnWriteMap<>();

    private static List<TopicInfoRefresh> topicInfoRefreshList = new CopyOnWriteArrayList<>();

    private static List<TopicConsumerInfoRefresh> topicConsumerInfoRefreshList = new CopyOnWriteArrayList<>();

    private static AtomicReference<Future> refreshKafkaInfoFutureAtom = new AtomicReference<Future>();

    static {
        refreshKafkaInfoFutureAtom.compareAndSet(null, CompletableFuture.runAsync(() -> {
            while (true) {
                try {
                    Thread.sleep(5000);

                    topicPartitionFetchResultMap.clear();
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

                    topicPartitionConsumerFetchResultMap.clear();
                    topicConsumerInfoRefreshList.forEach(topicConsumerInfoRefresh -> {
                        try {
                            String topic = topicConsumerInfoRefresh.topic();
                            String consumerGroup = topicConsumerInfoRefresh.getConsumerGroup();
                            Map<Integer, String> topicConsumerMap = topicPartitionConsumerFetchResultMap
                                    .computeIfAbsent(topic,
                                            (topicStr) -> getCurrentTopicPartitionConsumerMap(topicStr, consumerGroup));
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
        if(INSTANCE == null || INSTANCE.kafkaProperties == null) {
            return 0;
        }

        int currentTopicPartitionNum = 0;
        if(!topicPartitionFetchResultMap.containsKey(topic)) {
            try(AdminClient adminClient = AdminClient.create(INSTANCE.kafkaProperties.buildAdminProperties())) {
                DescribeTopicsResult describeTopicsResult = adminClient.describeTopics(Collections.singleton(topic));
                TopicDescription topicDescription = describeTopicsResult.topicNameValues().get(topic).get();
                currentTopicPartitionNum = topicDescription.partitions().size();
                topicPartitionFetchResultMap.put(topic, currentTopicPartitionNum);
            } catch (Exception e) {
                log.error("failed describeTopics {} ", topic, e);
            }
        } else {
            currentTopicPartitionNum = topicPartitionFetchResultMap.get(topic);
        }
        return currentTopicPartitionNum;
    }

    public static Map<Integer, String> getCurrentTopicPartitionConsumerMap(String topic, String consumerGroup) {
        if(INSTANCE == null || INSTANCE.kafkaProperties == null) {
            return Collections.emptyMap();
        }

        Map<Integer, String> currentTopicPartitionConsumerMap = new HashMap<>();
        if(!topicPartitionConsumerFetchResultMap.containsKey(topic)) {
            try(AdminClient adminClient = AdminClient.create(INSTANCE.kafkaProperties.buildAdminProperties())) {
                DescribeConsumerGroupsResult describeConsumerGroupsResult = adminClient.describeConsumerGroups(Collections.singleton(consumerGroup));
                ConsumerGroupDescription consumerGroupDescription = describeConsumerGroupsResult.describedGroups().get(consumerGroup).get();
                consumerGroupDescription.members().forEach(memberDescription -> {
                    memberDescription.assignment().topicPartitions().forEach(topicPartition -> {
                        if(StringUtils.equals(topic, topicPartition.topic())) {
                            currentTopicPartitionConsumerMap.put(topicPartition.partition(), memberDescription.host().substring(memberDescription.host().startsWith("/") ? 1 : 0));
                        }
                    });
                });
                topicPartitionConsumerFetchResultMap.put(topic, currentTopicPartitionConsumerMap);
            } catch (Exception e) {
                log.error("failed describeConsumerGroups {} ", topic, e);
            }
        } else {
            currentTopicPartitionConsumerMap.putAll(topicPartitionConsumerFetchResultMap.get(topic));
        }
        return currentTopicPartitionConsumerMap;
    }

    public static void addTopicInfoRefresh(TopicInfoRefresh topicInfoRefresh) {
        topicInfoRefreshList.add(topicInfoRefresh);
    }

    public static void addTopicConsumerInfoRefresh(TopicConsumerInfoRefresh topicConsumerInfoRefresh) {
        topicConsumerInfoRefreshList.add(topicConsumerInfoRefresh);
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        INSTANCE = this;
    }
}
