package ashraful.rnd.awssqs;

import ashraful.rnd.awssqs.dto.EventDto;
import ashraful.rnd.awssqs.producer.SqsEventProducer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest
public class SqsEvent {

    @Autowired
    private SqsEventProducer sqsEventProducer;

    @Test
    public void test(){
        EventDto eventDto = EventDto.builder()
                .eventId("test-event-1")
                .eventMessage("test-message-"+System.currentTimeMillis())
                .eventTime(System.currentTimeMillis()+"")
                .build();

        sqsEventProducer.publishEvent(eventDto);

    }
}
