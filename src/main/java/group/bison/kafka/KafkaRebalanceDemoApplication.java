package group.bison.kafka;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.web.servlet.error.ErrorMvcAutoConfiguration;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.web.reactive.config.EnableWebFlux;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@SpringBootApplication(exclude = {ErrorMvcAutoConfiguration.class})
@EnableWebFlux
@EnableCaching
public class KafkaRebalanceDemoApplication {

    public static void main(String[] args) throws Exception {
        try {
            SpringApplication.run(KafkaRebalanceDemoApplication.class, args);
        } catch (Throwable e) {
            log.error(e.getMessage(), e);
        }
    }

}