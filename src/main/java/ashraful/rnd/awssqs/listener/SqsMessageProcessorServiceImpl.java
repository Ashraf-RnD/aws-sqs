package ashraful.rnd.awssqs.listener;

import ashraful.rnd.awssqs.sqs.SqsMessageProcessorService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

@Slf4j
@Component
@RequiredArgsConstructor
public class SqsMessageProcessorServiceImpl implements SqsMessageProcessorService {


    @Override
    public Mono<Boolean> processMessage(String message, String messageType) {

        log.info("SqsMessageProcessorServiceImpl:: processMessage:: message: {}, messageType: {}",message,messageType);

        return Mono.just(true);
    }
}
