package ashraful.rnd.awssqs.controller;

import ashraful.rnd.awssqs.dto.EventDto;
import ashraful.rnd.awssqs.producer.SqsEventProducer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

@Slf4j
@RestController
@RequiredArgsConstructor
public class EventController {
    private final SqsEventProducer sqsEventProducer;

    @PostMapping("/publishEvent")
    public Mono<String> publishEvent(@RequestBody EventDto eventDto){
        return sqsEventProducer.publishEvent(eventDto);
    }

}
