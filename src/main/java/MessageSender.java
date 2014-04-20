import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.Observer;
import rx.util.functions.Action1;

import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created with IntelliJ IDEA.
 * User: miroslavmiladinovic
 * Date: 19/04/2014
 * Time: 19:03
 * To change this template use File | Settings | File Templates.
 */
public class MessageSender implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(MessageSender.class);

    private final BlockingQueue<SimpleMessage> queue;
    private final BlockingQueue<String> dlq;
    private final String queueUrl;
    private final AmazonSQSClient sqs;
    private final ObjectMapper mapper = new ObjectMapper();

    private final AtomicInteger counter = new AtomicInteger(0);
    private final AtomicInteger sentToSQS = new AtomicInteger(0);
    private final AtomicInteger errorToSQS = new AtomicInteger(0);

    private volatile boolean cancelled;

    public MessageSender(String queueUrl, AmazonSQSClient sqs, BlockingQueue<SimpleMessage> queue, BlockingQueue<String> dlq) {
        this.queue = queue;
        this.dlq = dlq;
        this.queueUrl = queueUrl;
        this.sqs = sqs;
    }

    public void cancel() {
        this.cancelled = true;
    }

    @Override
    public void run() {
        try {
            while (!cancelled) {
                Observable<String> msgId = sendMessage(queue.take());
                handleSQSResponse(msgId);
            }
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        finally {
            logger.info("received total of {} messages. sent to SQS ok {}, error to SQS {}", counter, sentToSQS, errorToSQS);
        }

    }

    private Observable<String> sendMessage(SimpleMessage message) throws InterruptedException {
        try {
            String messageAsString = mapper.writeValueAsString(message);
            counter.incrementAndGet();
            SendMessageSyncCommand syncSend = new SendMessageSyncCommand(messageAsString, queueUrl, sqs);
            return syncSend.toObservable();
        } catch (JsonProcessingException e) {
            logger.error("cannot make json out of message.", e);
        }
        return null;
    }

    private void handleSQSResponse(Observable<String> messageId) {
        messageId.subscribe(new Observer<String>() {
            @Override
            public void onCompleted() {
            }

            @Override
            public void onError(Throwable e) {
                // logger.error("problem sending to sqs: "+e.getMessage());
                errorToSQS.incrementAndGet();
            }

            @Override
            public void onNext(String args) {
                sentToSQS.incrementAndGet();
            }
        });
    }
}
