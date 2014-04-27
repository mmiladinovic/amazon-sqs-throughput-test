package com.mmiladinovic.sqs.consumer;

import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;
import com.codahale.metrics.Counter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.mmiladinovic.sqs.Constants;
import com.mmiladinovic.sqs.producer.MessageSender;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;

/**
 * Created with IntelliJ IDEA.
 * User: miroslavmiladinovic
 * Date: 23/04/2014
 * Time: 17:17
 * To change this template use File | Settings | File Templates.
 */
public class MessagePoller implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(MessageSender.class);

    private final BlockingQueue<SQSMessage> receiveQueue;
    private final String queueUrl;
    private final AmazonSQSClient sqs;

    private final Meter messagePollerMeter;
    private final Counter messageErrorsCounter;
    private final Timer sqsReceiveTimer;

    private volatile boolean cancelled;

    public MessagePoller(BlockingQueue<SQSMessage> receiveQueue, String queueUrl, AmazonSQSClient sqs, MetricRegistry metricRegistry) {
        this.receiveQueue = receiveQueue;
        this.queueUrl = queueUrl;
        this.sqs = sqs;

        messagePollerMeter = metricRegistry.meter(Constants.METER_POLLER_MESSAGES_POLLED);
        sqsReceiveTimer = metricRegistry.timer(Constants.TIMER_POLLER_SQS_RECEIVE);
        messageErrorsCounter = metricRegistry.counter(Constants.COUNTER_POLLER_SQS_RECEIVE_ERROR);
    }

    @Override
    public void run() {
        try {
            while (!cancelled) {
                List<SQSMessage> messages = pollBatchFromSQS();
                messagePollerMeter.mark(messages.size());
                for (SQSMessage m : messages) {
                    receiveQueue.put(m);
                }
            }
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }


    private List<SQSMessage> pollBatchFromSQS() {
        ReceiveMessageResult r = null;
        Timer.Context timer = sqsReceiveTimer.time();
        try {
             r = sqs.receiveMessage(new ReceiveMessageRequest(queueUrl));
        }
        catch (Exception e) {
            messageErrorsCounter.inc();
        }
        finally {
            timer.stop();
        }
        List<SQSMessage> retval = new ArrayList<SQSMessage>(r.getMessages().size());
        for (Message m : r.getMessages()) {
            retval.add(new SQSMessage(m.getBody(), m.getReceiptHandle()));
        }
        return retval;
    }

    public void cancel() {
        this.cancelled = true;
    }
}
