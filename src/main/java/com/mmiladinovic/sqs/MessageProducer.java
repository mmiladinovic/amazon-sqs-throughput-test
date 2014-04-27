package com.mmiladinovic.sqs;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created with IntelliJ IDEA.
 * User: miroslavmiladinovic
 * Date: 19/04/2014
 * Time: 18:42
 * To change this template use File | Settings | File Templates.
 */
public class MessageProducer implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(MessageProducer.class);
    private final BlockingQueue<SimpleMessage> messageQ;
    private final AtomicInteger counter = new AtomicInteger(0);

    private volatile boolean cancelled;

    public MessageProducer(BlockingQueue<SimpleMessage> messageQ) {
        this.messageQ = messageQ;
    }

    @Override
    public void run() {
        try {
            while (!cancelled) {
                counter.incrementAndGet();
                generateMessage();
            }
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        finally {
            logger.info("bowing out. produced {} messages", counter.get());
        }
    }

    public void cancel() {
        this.cancelled = true;
    }

    private void generateMessage() throws InterruptedException {
        String userId = UUID.randomUUID().toString();
        String objectId = UUID.randomUUID().toString();
        messageQ.put(new SimpleMessage(objectId, userId));
    }
}
