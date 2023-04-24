package group.bison.kafka.rebalancer.controller;

import java.util.Collections;
import java.util.List;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import group.bison.kafka.rebalancer.remote_mq.MemoryMq;

@RestController
@RequestMapping("/kafka-rebalancer")
public class PullMessageController {

    @GetMapping("/pull/{topic}/{consumer}")
    public ResponseEntity pull(@PathVariable(name = "topic", required = true) String topic, @PathVariable(name = "consumer", required = true) String consumer,  @RequestParam(name = "batchSize", required = false, defaultValue = "1000") Integer batchSize) {
        List messageList = MemoryMq.pull(topic, topic, batchSize);
        return ResponseEntity.ok(Collections.singletonMap("list", messageList));
    }
    
}
