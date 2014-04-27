package com.mmiladinovic.sqs;

import com.amazonaws.services.sqs.AmazonSQSClient;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;

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
        sentToSQSMeter = metricRegistry.meter(Constants.SENT_TO_SQS_METER);
        errorToSQSMeter = metricRegistry.meter(Constants.ERROR_FROM_SQS_METER);
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
    }

    private void sendMessageBatchSync(List<SimpleMessage> batch) throws InterruptedException {
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
        } catch (JsonProcessingException e) {
            logger.error("cannot make json out of message.", e);
        }
        catch (InterruptedException e) {
            throw e;
        }
        catch (Exception e) {
            errorToSQSMeter.mark();
        }
        finally {
            if (timer != null) timer.stop();
        }
    }

    private void sendMessageSync(SimpleMessage message) throws InterruptedException {
        messagesDequeuedMeter.mark();
        Timer.Context timer = null;
        try {
            String messageAsString = mapper.writeValueAsString(message);
            SendMessageSyncCommand syncSend = new SendMessageSyncCommand(messageAsString, queueUrl, sqs);
            timer = sqsSendTimer.time();
            syncSend.run();
            sentToSQSMeter.mark();
        } catch (JsonProcessingException e) {
            logger.error("cannot make json out of message.", e);
        }
        catch (InterruptedException e) {
            throw e;
        }
        catch (Exception e) {
            errorToSQSMeter.mark();
        }
        finally {
            if (timer != null) timer.stop();
        }

    }

    public long getMessagesSent() {
        return sentToSQSMeter.getCount();
    }

    public long getErrorToSQSMeter() {
        return errorToSQSMeter.getCount();
    }
}
