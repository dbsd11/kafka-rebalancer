package group.bison.springbatch.writer;

import java.util.List;

import org.springframework.batch.item.support.AbstractItemStreamItemWriter;
import org.springframework.util.CollectionUtils;

import group.bison.kafka.rebalancer.remote_mq.MemoryMq;

public class LocalMqItemWriter extends AbstractItemStreamItemWriter {

    private String topic;

    public LocalMqItemWriter(String topic) {
        this.topic = topic;
    }

    @Override
    public void write(List items) throws Exception {
        if(CollectionUtils.isEmpty(items)) {
            return;
        }
        items.forEach(item -> {
            MemoryMq.push(topic, null, item);
        });
    }
    
}
