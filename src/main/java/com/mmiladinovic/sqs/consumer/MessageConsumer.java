package com.mmiladinovic.sqs.consumer;

import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.*;
import com.codahale.metrics.Counter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.mmiladinovic.sqs.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;

/**
 * Created with IntelliJ IDEA.
 * User: miroslavmiladinovic
 * Date: 26/04/2014
 * Time: 18:22
 * To change this template use File | Settings | File Templates.
 */
public class MessageConsumer implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(MessageConsumer.class);

    private final BlockingQueue<SQSMessage> receiveQueue;
    private final String queueUrl;
    private final AmazonSQSClient sqs;

    private final Meter messageDeletedMeter;
    private final Counter deleteRequestErrorsCounter;
    private final Counter messageDeleteFailedCounter;
    private final Timer sqsDeleteTimer;

    private volatile boolean cancelled;

    public MessageConsumer(BlockingQueue<SQSMessage> receiveQueue, String queueUrl, AmazonSQSClient sqs, MetricRegistry metricRegistry) {
        this.receiveQueue = receiveQueue;
        this.queueUrl = queueUrl;
        this.sqs = sqs;

        messageDeletedMeter = metricRegistry.meter(Constants.METER_CONSUMER_MESSAGES_CONSUMED);
        deleteRequestErrorsCounter = metricRegistry.counter(Constants.COUNTER_CONSUMER_SQS_DELETE_ERROR);
        sqsDeleteTimer = metricRegistry.timer(Constants.TIMER_CONSUMER_SQS_DELETE);
        messageDeleteFailedCounter = metricRegistry.counter(Constants.COUNTER_CONSUMER_SQS_MESSAGE_DELETE_FAILED);
    }

    @Override
    public void run() {
        while (!cancelled) {
            List<SQSMessage> batch = new ArrayList<SQSMessage>(10);
            receiveQueue.drainTo(batch, 10);
            long deleted = deleteMessages(batch);
            messageDeletedMeter.mark(deleted);
        }
    }


    private long deleteMessages(List<SQSMessage> messages) {
        if (messages == null || messages.isEmpty()) {
            return 0;
        }
        List<DeleteMessageBatchRequestEntry> entries = new ArrayList<DeleteMessageBatchRequestEntry>(messages.size());
        int i = 0;
        for (SQSMessage m : messages) {
            entries.add(new DeleteMessageBatchRequestEntry(String.valueOf(i++), m.getMessageHandle()));
        }
        DeleteMessageBatchRequest request = new DeleteMessageBatchRequest(queueUrl, entries);
        DeleteMessageBatchResult r = null;
        Timer.Context timer = sqsDeleteTimer.time();
        try {
            r = sqs.deleteMessageBatch(request);
            if (!r.getFailed().isEmpty()) {
                messageDeleteFailedCounter.inc(r.getFailed().size());
            }
        }
        catch (Exception e) {
            deleteRequestErrorsCounter.inc();
        }
        finally {
            timer.stop();
        }
        return r != null ? r.getSuccessful().size() : 0;
    }

    public void cancel() {
        this.cancelled = true;
    }
}
