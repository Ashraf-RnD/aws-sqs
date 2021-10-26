package ashraful.rnd.awssqs.sqs;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;
import software.amazon.awssdk.services.sqs.model.*;

import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

@Slf4j
@RequiredArgsConstructor
public class SqsMessageConsumer implements Runnable{

    private final String queueUrl;
    private final String consumerId;
    private final AtomicBoolean primary;
    private final SqsAsyncClient sqsAsyncClient;
    private final SqsConsumerSettings sqsConsumerSettings;
    private final SqsMessageProcessorService sqsMessageProcessorService;

    private long lastProcessTime = System.currentTimeMillis();
    private AtomicBoolean stop = new AtomicBoolean(false);



    @Override
    public void run() {
        log.info("SQSConsumer - {} | StartConsumer", consumerId);
        this.stop.set(false);

        var receiveMessageRequest = getReceiveMessageRequest();

        var receiveMessageResponseMono =
                Mono.fromFuture(() -> sqsAsyncClient.receiveMessage(receiveMessageRequest))
                        .doOnError(this::logOnError)
                        .onErrorReturn(ReceiveMessageResponse.builder().build());

        receiveMessageResponseMono
                .map(ReceiveMessageResponse::messages)
                .flatMap(this::deleteMessageAndUpdateConsumerStatus)
                .map(Flux::fromIterable)
                .flatMap(messageFlux -> messageFlux
                        .parallel()
                        .runOn(Schedulers.boundedElastic())
                        .flatMap(message -> sqsMessageProcessorService.processMessage(message.body(),sqsConsumerSettings.getSqsMessageType())
                                .thenReturn(message.receiptHandle())
                        )
                        .then()
                )
                .delaySubscription(Duration.ofSeconds(sqsConsumerSettings.getConsumerDelayPerBatchProcess()))
                .doOnSuccess(subscription -> {lastProcessTime=System.currentTimeMillis();})
                .onErrorContinue((e, object) -> logOnError(e))
                .repeat(this::runAgain)
                .subscribe();
    }

    private ReceiveMessageRequest getReceiveMessageRequest() {

        return ReceiveMessageRequest.builder()
                .maxNumberOfMessages(sqsConsumerSettings.getBatchSize())
                .queueUrl(this.queueUrl)
                .waitTimeSeconds(sqsConsumerSettings.getPollingWaitTimeInSeconds())
                .visibilityTimeout(sqsConsumerSettings.getVisibilityTimeoutInSeconds())
                .build();
    }

    private Mono<List<Message>> deleteMessageAndUpdateConsumerStatus(List<Message> messages) {
        log.info("SQSConsumer - {} | Time since last execution : {} | Message fetched count : {}", consumerId,
                (System.currentTimeMillis() - lastProcessTime), messages.size());
        if (messages.size() > 0) {
            var deleteMessageBatchRequest = getDeleteMessageBatchRequest(messages);

            return batchDeleteSqsMessages(deleteMessageBatchRequest)
                    .doOnError(this::logOnError)
                    .onErrorReturn(DeleteMessageBatchResponse.builder().build())
                    .thenReturn(messages);
        } else {
            stopConsumer();

            return Mono.just(messages);
        }
    }

    private DeleteMessageBatchRequest getDeleteMessageBatchRequest(List<Message> messages) {

        return DeleteMessageBatchRequest.builder()
                .queueUrl(this.queueUrl)
                .entries(messages
                        .stream()
                        .filter(Objects::nonNull)
                        .map(message -> DeleteMessageBatchRequestEntry
                                .builder()
                                .id(message.messageId())
                                .receiptHandle(message.receiptHandle())
                                .build())
                        .collect(Collectors.toList()))
                .build();
    }
    private Mono<DeleteMessageBatchResponse> batchDeleteSqsMessages(DeleteMessageBatchRequest deleteMessageBatchRequest) {
        return Mono.fromFuture(() -> sqsAsyncClient.deleteMessageBatch(deleteMessageBatchRequest));
    }

    private boolean runAgain() {

        return this.primary.get() || !this.stop.get();
    }

    public boolean getConsumerStopStatus() {

        return this.stop.get() || stopStatusOnLastRunTime();
    }

    public boolean stopStatusOnLastRunTime() {

        return (System.currentTimeMillis() - lastProcessTime) >
                sqsConsumerSettings.getDelayThresholdInMillisForWorkerRestart();
    }

    public void stopConsumer() {
        if (!this.primary.get()) {
            log.info("SQSConsumer - {} | StopConsumer", consumerId);
            this.stop.set(true);
        }
    }

    private void logOnError(Throwable throwable) {
        log.error("SQSConsumer - {} | Error", consumerId, throwable);
    }
}
