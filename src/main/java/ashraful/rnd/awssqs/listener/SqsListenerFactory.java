package ashraful.rnd.awssqs.listener;

import ashraful.rnd.awssqs.commons.SqsMessageType;
import ashraful.rnd.awssqs.sqs.SqsConsumerSettings;
import ashraful.rnd.awssqs.sqs.SqsMessageConsumerService;
import ashraful.rnd.awssqs.sqs.SqsMessageProcessorService;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;

import javax.annotation.PostConstruct;
import java.util.UUID;

@Component
@RequiredArgsConstructor
public class SqsListenerFactory {
    private final SqsAsyncClient sqsAsyncClient;
    private final SqsMessageProcessorService sqsMessageProcessorService;

    @Value("${sqs.queue.name}")
    private String queueName;

    @PostConstruct
    public void runListener() {
        var subscriptionMessageConsumer = new SqsMessageConsumerService(sqsAsyncClient,
                getSubscriptionListenerSetting(),
                sqsMessageProcessorService
        );
        subscriptionMessageConsumer.run();
    }

    private SqsConsumerSettings getSubscriptionListenerSetting() {
        return SqsConsumerSettings.builder()
                .queueName(this.queueName)
                .sqsMessageType(SqsMessageType.TEST_EVENT.name())
                .consumerServiceId(getConsumerServiceId())
                .minListenerCount(3)
                .maxListenerCount(20)
                .listenerCountAdjustDelayInSecond(20)
                .listenerCountAdjustmentThreshold(2000)
                .consumerDelayPerBatchProcess(10)
                .batchSize(10)
                .pollingWaitTimeInSeconds(20)
                .visibilityTimeoutInSeconds(30)
                .delayThresholdInMillisForWorkerRestart(120000)
                .build();
    }

    private String getConsumerServiceId() {
        return this.queueName + "_" + UUID.randomUUID()
                .toString()
                .replace("-","")
                .substring(0, 4) + "_";
    }

}
