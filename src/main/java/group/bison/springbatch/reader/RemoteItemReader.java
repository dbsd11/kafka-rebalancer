package group.bison.springbatch.reader;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.springframework.batch.item.NonTransientResourceException;
import org.springframework.batch.item.ParseException;
import org.springframework.batch.item.UnexpectedInputException;
import org.springframework.batch.item.support.AbstractItemStreamItemReader;
import org.springframework.lang.Nullable;

import group.bison.kafka.rebalancer.tools.KafkaInfoFetcher;
import group.bison.kafka.rebalancer.utils.HttpUtils;
import group.bison.kafka.rebalancer.utils.InetUtils;
import group.bison.kafka.rebalancer.utils.JsonUtil;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class RemoteItemReader extends AbstractItemStreamItemReader {
    private String topic;

    private Iterator itemIt = null;

    private String localConsumer = null;

    public RemoteItemReader(String topic) {
        this.topic = topic;
        this.localConsumer = InetUtils.getLocalHostName();
    }


    @Override
    @Nullable
    public Object read() throws Exception, UnexpectedInputException, ParseException, NonTransientResourceException {
        if(itemIt == null || !itemIt.hasNext()) {
            List pulledRemoteMessageList = new LinkedList();
            Map<Integer, String> currentTopicPartitionConsumerMap = KafkaInfoFetcher.getCurrentTopicPartitionConsumerMap(topic);
            currentTopicPartitionConsumerMap.values().forEach((consumer) -> {
                try {
                    String responseStr = HttpUtils.http2RequestWithPool("GET", String.join("",  "http://", consumer, "/kafka-rebalancer/pull/", topic, "/", localConsumer), null, null);
                    List remoteMessageList = (List)JsonUtil.fromJson(responseStr, HashMap.class).get("list");
                    pulledRemoteMessageList.addAll(0, remoteMessageList);
                } catch (Exception e) {
                    log.error(e.getMessage(), e);
                }
            });
            itemIt = pulledRemoteMessageList.iterator();
        }
        return (itemIt == null || !itemIt.hasNext()) ? null : itemIt.next();
    }
}
