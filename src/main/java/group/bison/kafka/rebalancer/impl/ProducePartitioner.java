package group.bison.kafka.rebalancer.impl;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import lombok.Data;

@Data
public class ProducePartitioner implements Partitioner {

    private static Map<String, ProduceRebalancer> topicProduceRebalancerMap = new HashMap<>();

    @Override
    public void configure(Map<String, ?> configs) {
    }

    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        if(!topicProduceRebalancerMap.containsKey(topic)) {
            return 0;
        }
        return topicProduceRebalancerMap.get(topic).computeMessagePartition(value);
    }

    @Override
    public void close() {
    }
    
    public static void setProduceRebalancer(String topic, ProduceRebalancer produceRebalancer) {
        topicProduceRebalancerMap.put(topic, produceRebalancer);
    }
}
