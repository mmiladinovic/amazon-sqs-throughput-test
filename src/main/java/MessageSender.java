import com.amazonaws.services.sqs.AmazonSQSClient;
import com.codahale.metrics.Counter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.Observer;
import rx.Subscription;

import java.util.ArrayList;
import java.util.List;
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
    private final String queueUrl;
    private final AmazonSQSClient sqs;
    private final ObjectMapper mapper = new ObjectMapper();

    private final AtomicInteger counter = new AtomicInteger(0);
    private final AtomicInteger sentToSQS = new AtomicInteger(0);
    private final AtomicInteger errorToSQS = new AtomicInteger(0);

    private final Meter messagesDequeuedMeter;
    private final Meter sentToSQSMeter;
    private final Meter errorToSQSMeter;
    private final Timer sqsSendTimer;

    private volatile boolean cancelled;

    public MessageSender(String queueUrl, AmazonSQSClient sqs, BlockingQueue<SimpleMessage> queue, MetricRegistry metricRegistry) {
        this.queue = queue;
        this.queueUrl = queueUrl;
        this.sqs = sqs;

        messagesDequeuedMeter = metricRegistry.meter("messagesDequeuedMeter");
        sentToSQSMeter = metricRegistry.meter("sentToSQSMeter");
        errorToSQSMeter = metricRegistry.meter("errorToSQSMeter");
        sqsSendTimer = metricRegistry.timer("sqsSendTime");
    }

    public void cancel() {
        this.cancelled = true;
    }

    @Override
    public void run() {
        try {
            while (!cancelled) {
                //sendMessageWithHystrix(queue.take());
                  // sendMessageSync(queue.take());
                List<SimpleMessage> batch = new ArrayList<SimpleMessage>(10);
                if (queue.drainTo(batch, 10) > 0) {
                    sendMessageBatchSync(batch);
                }

            }
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        finally {
            // logger.info("received total of {} messages. sent to SQS ok {}, error to SQS {}", counter, sentToSQS, errorToSQS);
        }

    }

    private void sendMessageBatchSync(List<SimpleMessage> batch) throws InterruptedException {
        int i = counter.addAndGet(batch.size());
        messagesDequeuedMeter.mark(batch.size());
        Timer.Context timer = null;
        try {
            List<String> messagesAsString = new ArrayList<String>(batch.size());
            for (SimpleMessage message : batch) {
                messagesAsString.add(mapper.writeValueAsString(message));
            }
            SendMessageBatchSyncCommand batchSend = new SendMessageBatchSyncCommand(messagesAsString, queueUrl, sqs);
            timer = sqsSendTimer.time();
            batchSend.run();
            sentToSQSMeter.mark(batch.size());
            sentToSQS.addAndGet(batch.size());
        } catch (JsonProcessingException e) {
            logger.error("cannot make json out of message.", e);
        }
        catch (InterruptedException e) {
            throw e;
        }
        catch (Exception e) {
            errorToSQS.incrementAndGet();
            errorToSQSMeter.mark();
            if (i % 100 == 0) {
                logger.error("exception sending message", e);
            }
        }
        finally {
            if (timer != null) timer.stop();
        }
    }

    private void sendMessageSync(SimpleMessage message) throws InterruptedException {
        int i = counter.incrementAndGet();
        messagesDequeuedMeter.mark();
        Timer.Context timer = null;
        try {
            String messageAsString = mapper.writeValueAsString(message);
            SendMessageSyncCommand syncSend = new SendMessageSyncCommand(messageAsString, queueUrl, sqs);
            timer = sqsSendTimer.time();
            syncSend.run();
            sentToSQSMeter.mark();
            sentToSQS.incrementAndGet();
        } catch (JsonProcessingException e) {
            logger.error("cannot make json out of message.", e);
        }
        catch (InterruptedException e) {
            throw e;
        }
        catch (Exception e) {
            errorToSQS.incrementAndGet();
            errorToSQSMeter.mark();
            if (i % 100 == 0) {
                logger.error("exception sending message", e);
            }
        }
        finally {
            if (timer != null) timer.stop();
        }

    }

    private void sendMessageWithHystrix(SimpleMessage message) throws InterruptedException {
        try {
            String messageAsString = mapper.writeValueAsString(message);
            counter.incrementAndGet();
            SendMessageSyncCommand syncSend = new SendMessageSyncCommand(messageAsString, queueUrl, sqs);
            syncSend.execute();
            sentToSQS.incrementAndGet();
        } catch (JsonProcessingException e) {
            logger.error("cannot make json out of message.", e);
        }
    }

    private void handleSQSResponse(Observable<String> messageId) {
        if (messageId == null) {
            return;
        }
        final Subscription s = messageId.subscribe(new Observer<String>() {
            @Override
            public void onCompleted() {
            }

            @Override
            public void onError(Throwable e) {
                int i = errorToSQS.incrementAndGet();
                if (i % 100 == 0) {
                    logger.error("problem sending to sqs: ",e);
                }
            }

            @Override
            public void onNext(String args) {
                sentToSQS.incrementAndGet();
            }
        });
    }

    public int getMessagesSent() {
        return sentToSQS.get();
    }

}
