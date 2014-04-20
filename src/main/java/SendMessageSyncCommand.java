import com.amazonaws.services.sqs.AmazonSQSClient;
import com.netflix.hystrix.HystrixCommand;
import com.netflix.hystrix.HystrixCommandGroupKey;
import com.netflix.hystrix.HystrixCommandProperties;
import com.netflix.hystrix.HystrixThreadPoolProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.netflix.hystrix.HystrixCommand.Setter.withGroupKey;

/**
 * Created with IntelliJ IDEA.
 * User: miroslavmiladinovic
 * Date: 19/04/2014
 * Time: 19:33
 * To change this template use File | Settings | File Templates.
 */
public class SendMessageSyncCommand extends HystrixCommand<String> {
    protected static final Logger logger = LoggerFactory.getLogger(SendMessageSyncCommand.class);

    private final AmazonSQSClient sqs;
    private final String queueUrl;
    private final String message;

    public SendMessageSyncCommand(String message, String queueUrl, AmazonSQSClient sqs) {
        super(withGroupKey(HystrixCommandGroupKey.Factory.asKey("SendMessageSyncCommand"))
                .andCommandPropertiesDefaults(HystrixCommandProperties.Setter()
                        .withExecutionIsolationThreadTimeoutInMilliseconds(5000))
                .andThreadPoolPropertiesDefaults(HystrixThreadPoolProperties.Setter().withMaxQueueSize(10)));
        this.sqs = sqs;
        this.queueUrl = queueUrl;
        this.message = message;
    }

    @Override
    protected String run() throws Exception {
        return sqs.sendMessage(queueUrl, message).getMessageId();
    }
}
