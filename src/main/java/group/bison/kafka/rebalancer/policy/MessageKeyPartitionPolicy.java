package group.bison.kafka.rebalancer.policy;

public interface MessageKeyPartitionPolicy {
    
    int computePartition(String key);
}
