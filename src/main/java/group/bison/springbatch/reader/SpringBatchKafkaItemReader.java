package group.bison.springbatch.reader;

import java.time.Duration;
import java.util.Collections;
import java.util.Iterator;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.NonTransientResourceException;
import org.springframework.batch.item.ParseException;
import org.springframework.batch.item.UnexpectedInputException;
import org.springframework.batch.item.support.AbstractItemStreamItemReader;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SpringBatchKafkaItemReader extends AbstractItemStreamItemReader {

    private KafkaConsumer<String, Object> kafkaConsumer;

    private Iterator<ConsumerRecord<String, Object>> consumerRecords;

    private Duration pollTimeout = Duration.ofSeconds(30L);

    private Properties consumerProperties;

    private String topic;

    public SpringBatchKafkaItemReader(Properties consumerProperties, String topic) {
        this.consumerProperties = consumerProperties;
        this.topic = topic;
    }

    @Override
    public void open(ExecutionContext executionContext) {
        this.kafkaConsumer = new KafkaConsumer<>(this.consumerProperties);
        kafkaConsumer.subscribe(Collections.singleton(topic));
    }

    @Override
    public Object read()
            throws Exception, UnexpectedInputException, ParseException, NonTransientResourceException {
        Object item = null;
        boolean needRestartConsumer = false;
        while (item == null) {
            if (needRestartConsumer) {
                needRestartConsumer = false;
                this.kafkaConsumer = new KafkaConsumer<>(this.consumerProperties);
                kafkaConsumer.subscribe(Collections.singleton(topic));
            }

            if (this.consumerRecords == null || !this.consumerRecords.hasNext()) {
                try {
                    this.consumerRecords = this.kafkaConsumer.poll(this.pollTimeout).iterator();
                } catch (Exception e) {
                    log.info("kafka poll records failed {} ", e.getMessage());
                    needRestartConsumer = true;
                    try {
                        this.kafkaConsumer.close();
                    } catch (Exception e1) {
                    }
                }
            }

            if (this.consumerRecords != null && this.consumerRecords.hasNext()) {
                item = this.consumerRecords.next().value();
            }
        }
        return item;
    }

    @Override
    public void update(ExecutionContext executionContext) {
        this.kafkaConsumer.commitSync();
    }

    @Override
    public void close() {
        if (this.kafkaConsumer != null) {
            this.kafkaConsumer.close();
        }
    }
}
