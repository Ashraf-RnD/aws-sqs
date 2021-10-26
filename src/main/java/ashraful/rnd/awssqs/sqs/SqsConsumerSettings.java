package ashraful.rnd.awssqs.sqs;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class SqsConsumerSettings implements Serializable {
    private String queueName;
    private String sqsMessageType;
    private String consumerServiceId;

    //tweak according to queue message type and queue load
    private int minListenerCount; //1
    private int maxListenerCount; //20
    private int listenerCountAdjustDelayInSecond; //60
    private int listenerCountAdjustmentThreshold;//2000

    private int consumerDelayPerBatchProcess; //5
    private int batchSize; //10
    private int pollingWaitTimeInSeconds; //20
    private int visibilityTimeoutInSeconds; //30
    private long delayThresholdInMillisForWorkerRestart; //120000
}
