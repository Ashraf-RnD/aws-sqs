/*
 * Md Ashraful Alam
 * 10/8/20, 12:32 AM
 */

package ashraful.rnd.awssqs.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;
import software.amazon.awssdk.services.sqs.SqsClient;

import java.net.URI;

@Configuration
public class SqsConfig {

    public static final String LOCALSTACK_ENDPOINT = "http://localhost:4576";

    @Bean
    public SqsAsyncClient sqsAsyncClient(){
        return SqsAsyncClient.builder()
                .endpointOverride(URI.create(LOCALSTACK_ENDPOINT))
                .region(Region.AP_SOUTHEAST_1)
                .build();
    }

    /*@Bean
    public SqsClient sqsClient(){
        return SqsClient.builder()
                .endpointOverride(URI.create(LOCALSTACK_ENDPOINT))
                .region(Region.AP_SOUTHEAST_1)
                .build();
    }*/

}
