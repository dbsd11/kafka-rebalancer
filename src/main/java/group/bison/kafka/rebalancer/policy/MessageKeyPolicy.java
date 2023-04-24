package group.bison.kafka.rebalancer.policy;

public interface MessageKeyPolicy {
    
    String computeKey(Object message);
}
