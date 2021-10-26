package ashraful.rnd.awssqs.sqs;

import reactor.core.publisher.Mono;

public interface SqsMessageProcessorService {
    Mono<Boolean> processMessage(String message, String messageType);
}
