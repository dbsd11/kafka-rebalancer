package group.bison.kafka.rebalancer.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.integration.channel.QueueChannel;
import org.springframework.messaging.support.GenericMessage;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

@RestController
@RequestMapping("/kafka-rebalancer")
public class ProduceMockMessageController {

    @Autowired
    private QueueChannel produceChannel;

    @PostMapping("/produce/mock/{topic}")
    public ResponseEntity produceMockMessage(@PathVariable(name = "topic", required = true) String topic, @RequestBody String body) {
        Map<String, Object> produceInfoMap = new HashMap<>();
        produceInfoMap.put("topic", topic);
        produceInfoMap.put("message", body);
        boolean sendSuccess = produceChannel.send(new GenericMessage<>(produceInfoMap));
        return ResponseEntity.ok(Collections.singletonMap("sendSuccess", sendSuccess));
    }
}
