package ashraful.rnd.awssqs.producer;

import ashraful.rnd.awssqs.commons.MapperUtil;
import ashraful.rnd.awssqs.dto.EventDto;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;

import javax.annotation.PostConstruct;
import java.util.concurrent.CompletableFuture;

@Slf4j
@Service
@RequiredArgsConstructor
public class SqsEventProducer {
    private final SqsAsyncClient sqsAsyncClient;
    private final MapperUtil mapperUtil;

    @Value("${sqs.queue.name}")
    private String queueName;
    private String queueUrl;

    @PostConstruct
    public void setQueueUrl() {
        this.queueUrl = getQueueUrl(queueName);
    }

    public String getQueueUrl(String queueName) {
        var queueUrlRequest = GetQueueUrlRequest.builder().queueName(queueName).build();
        try {

            return sqsAsyncClient.getQueueUrl(queueUrlRequest).get().queueUrl();
        } catch (Exception e) {
            log.info("Sqs queue url fetch error : {}", queueName, e);
            System.exit(1);
        }

        return "";
    }

    public Mono<String> publishEvent(EventDto eventDto) {

        var messageBody = mapperUtil.objectToJson(eventDto);

        var sendMessageRequest = SendMessageRequest.builder()
                .queueUrl(this.queueUrl)
                .messageBody(messageBody)
                .build();

        var future = sqsAsyncClient.sendMessage(sendMessageRequest);

        return Mono.fromFuture(future.thenApply(sendMessageResponse -> {
//            log.info("SqsEventProducer:: publishEvent:: publishResponse: {}", sendMessageResponse.toString());
            return sendMessageResponse.messageId();
        }));

    }

}
