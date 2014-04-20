import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.*;

/**
 * Created with IntelliJ IDEA.
 * User: miroslavmiladinovic
 * Date: 19/04/2014
 * Time: 20:02
 * To change this template use File | Settings | File Templates.
 */
public class Main {

    private static final Logger logger = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) throws Exception {
        ClientConfiguration config = new ClientConfiguration();

        Region usWest2 = Region.getRegion(Regions.fromName(Constants.AWS_REGION));
        AmazonSQSClient sqs = usWest2.createClient(AmazonSQSClient.class, new DefaultAWSCredentialsProviderChain(), config);

        CreateQueueRequest createQueueRequest = new CreateQueueRequest(UUID.randomUUID().toString());
        String queueUrl = sqs.createQueue(createQueueRequest).getQueueUrl();
        logger.info("created SQS queue {}", queueUrl);


        BlockingQueue<SimpleMessage> queue = new LinkedBlockingDeque<SimpleMessage>(Constants.BOUND);
        BlockingQueue<String> dlq = new LinkedBlockingQueue<String>(Constants.BOUND);

        Thread producer = new Thread(new MessageProducer(queue));
        producer.start();

        List<MessageSender> consumers = new ArrayList<MessageSender>();
        List<Thread> consumerThreads = new ArrayList<Thread>();
        for (int i = 0; i < Constants.NO_OF_SENDERS; i++) {
            MessageSender r = new MessageSender(queueUrl, sqs, queue, dlq);
            Thread t = new Thread(r); t.start();
            consumers.add(r);
            consumerThreads.add(t);
        }
        logger.info("waiting for 10 seconds");
        Thread.sleep(TimeUnit.SECONDS.toMillis(10));

        logger.info("shutting down producers and senders");
        producer.interrupt();
        for (MessageSender sender : consumers) {
            sender.cancel();
        }
        for (Thread t : consumerThreads) {
            t.interrupt();
        }

    }


}
