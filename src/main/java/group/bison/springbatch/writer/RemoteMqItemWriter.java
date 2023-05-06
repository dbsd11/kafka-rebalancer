package group.bison.springbatch.writer;

import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.springframework.batch.item.support.AbstractItemStreamItemWriter;
import org.springframework.util.CollectionUtils;

import com.auth0.jwt.internal.com.fasterxml.jackson.databind.deser.std.JdkDeserializers.StringDeserializer;
import com.auth0.jwt.internal.com.fasterxml.jackson.databind.ser.std.StringSerializer;

import group.bison.kafka.rebalancer.impl.ConsumeRebalancer;
import group.bison.kafka.rebalancer.remote_mq.MemoryMq;
import group.bison.kafka.rebalancer.utils.InetUtils;

public class RemoteMqItemWriter extends AbstractItemStreamItemWriter {

    private String topic;

    private ConsumeRebalancer consumeRebalancer;

    public RemoteMqItemWriter(String topic, ConsumeRebalancer consumeRebalancer) {
        this.topic = topic;
        this.consumeRebalancer = consumeRebalancer;
    }

    @Override
    public void write(List items) throws Exception {
        if(CollectionUtils.isEmpty(items)) {
            return;
        }
        items.forEach(item -> {
            String consumerInstance = consumeRebalancer.computeConsumerInstance(item);
            MemoryMq.push(topic, consumerInstance, item);
        });
    }
    
}
