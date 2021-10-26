package ashraful.rnd.awssqs.sqs;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;
import software.amazon.awssdk.services.sqs.model.GetQueueAttributesRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueAttributesResponse;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlRequest;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
@RequiredArgsConstructor
public class SqsMessageConsumerService implements Runnable {

    private static final String INITIAL_WORKER_TAG = "_init_";
    private static final String SAFETY_WORKER_TAG = "_safety_";
    private static final String INCREASED_WORKER_TAG = "_max_";

    private static final String queueMessageCountAttribute = "ApproximateNumberOfMessages";

    private final SqsAsyncClient sqsAsyncClient;
    private final SqsConsumerSettings sqsConsumerSettings;
    private final SqsMessageProcessorService sqsMessageProcessorService;

    private String queueUrl;
    private int[] restartCount;
    private int currentListenerCount;
    private SqsMessageConsumer[] sqsMessageConsumers;

    @Override
    public void run() {
        setQueueUrl();
        initializeListeners();
        continuousListenerAutoScaleProcess();
    }

    private void setQueueUrl() {
        var queueUrlRequest = GetQueueUrlRequest
                .builder()
                .queueName(sqsConsumerSettings.getQueueName())
                .build();
        try {
            this.queueUrl = sqsAsyncClient.getQueueUrl(queueUrlRequest)
                    .get()
                    .queueUrl();
        } catch (Exception e) {
            log.info("Sqs queue url fetch error : {}", sqsConsumerSettings.getQueueName(), e);
            System.exit(1);
        }
    }

    private void initializeListeners() {
        sqsMessageConsumers = new SqsMessageConsumer[sqsConsumerSettings.getMaxListenerCount()];

        restartCount = new int[sqsConsumerSettings.getMaxListenerCount()];
        for (int i = 0; i < sqsConsumerSettings.getMinListenerCount(); i++) {

            sqsMessageConsumers[i] = new SqsMessageConsumer(queueUrl,
                    sqsConsumerSettings.getConsumerServiceId() + INITIAL_WORKER_TAG + i,
                    new AtomicBoolean(true),
                    sqsAsyncClient,
                    sqsConsumerSettings,
                    sqsMessageProcessorService
            );
            sqsMessageConsumers[i].run();
            restartCount[i]++;
        }
        currentListenerCount = sqsConsumerSettings.getMinListenerCount();
    }

    private void continuousListenerAutoScaleProcess() {
        getQueueMessageCount()
                .map(messageCount -> {
                    updateListenerStatus(messageCount);
                    log.info("Queue {} message count : {}. Worker restart count : {}",
                            sqsConsumerSettings.getConsumerServiceId(), messageCount, restartCount);
                    return messageCount;
                })
                .delaySubscription(Duration.ofSeconds(sqsConsumerSettings.getListenerCountAdjustDelayInSecond()))
                .onErrorContinue((throwable, o) -> logOnError(throwable))
                .repeat()
                .subscribe();
    }

    private Mono<Integer> getQueueMessageCount() {
        var getQueueAttributesRequest = GetQueueAttributesRequest.builder()
                .queueUrl(this.queueUrl)
                .attributeNamesWithStrings(queueMessageCountAttribute)
                .build();

        return Mono.fromFuture(() -> sqsAsyncClient.getQueueAttributes(getQueueAttributesRequest))
                .map(GetQueueAttributesResponse::attributesAsStrings)
                .map(attributeMap -> Integer.valueOf(attributeMap.get(queueMessageCountAttribute)));
    }

    private void updateListenerStatus(long currentMessageCount) {
        int requiredListenerCount = currentMessageCount > sqsConsumerSettings.getListenerCountAdjustmentThreshold() ?
                sqsConsumerSettings.getMaxListenerCount() : sqsConsumerSettings.getMinListenerCount();

        if (requiredListenerCount < currentListenerCount) {
            for (int i = requiredListenerCount; i < currentListenerCount; i++) {
                sqsMessageConsumers[i].stopConsumer();
            }
        }

        //If Any Core/Initial Worker Crashes then this will recover
        for (int i = 0; i < requiredListenerCount; i++) {
            if (sqsMessageConsumers[i] == null || sqsMessageConsumers[i].getConsumerStopStatus()) {
                sqsMessageConsumers[i] = new SqsMessageConsumer(queueUrl,
                        sqsConsumerSettings.getConsumerServiceId() + SAFETY_WORKER_TAG + createRandomChar() + i,
                        new AtomicBoolean(true),
                        sqsAsyncClient,
                        sqsConsumerSettings,
                        sqsMessageProcessorService
                );
                sqsMessageConsumers[i].run();
                restartCount[i]++;
            }
        }

        currentListenerCount = requiredListenerCount;
    }

    private String createRandomChar() {
        StringBuilder randomChar = new StringBuilder();
        String possible = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
        for (int i = 0; i < 5; i++) {
            randomChar.append(possible.charAt((int) Math.floor(Math.random() * possible.length())));
        }
        return randomChar.toString() + "_";
    }

    private void logOnError(Throwable throwable) {
        log.error("SQSConsumer - {} | Error", queueUrl, throwable);
    }
}
