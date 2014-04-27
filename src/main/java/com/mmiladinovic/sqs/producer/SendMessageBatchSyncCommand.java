package com.mmiladinovic.sqs.producer;

import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.SendMessageBatchRequestEntry;
import com.amazonaws.services.sqs.model.SendMessageBatchResult;
import com.mmiladinovic.sqs.Constants;
import com.netflix.hystrix.HystrixCommand;
import com.netflix.hystrix.HystrixCommandGroupKey;
import com.netflix.hystrix.HystrixCommandProperties;
import com.netflix.hystrix.HystrixThreadPoolProperties;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static com.netflix.hystrix.HystrixCommand.Setter.withGroupKey;

/**
 * Created with IntelliJ IDEA.
 * User: miroslavmiladinovic
 * Date: 20/04/2014
 * Time: 19:29
 * To change this template use File | Settings | File Templates.
 */
public class SendMessageBatchSyncCommand extends HystrixCommand<SendMessageBatchResult> {
    private final AmazonSQSClient sqs;
    private final String queueUrl;
    private final List<String> messages;

    public SendMessageBatchSyncCommand(List<String> messages, String queueUrl, AmazonSQSClient sqs) {
        super(withGroupKey(HystrixCommandGroupKey.Factory.asKey("com.mmiladinovic.sqs.producer.SendMessageBatchSyncCommand"))
                .andCommandPropertiesDefaults(HystrixCommandProperties.Setter()
                        .withExecutionIsolationThreadTimeoutInMilliseconds(Constants.SEND_COMMAND_TIMEOUT))
                .andThreadPoolPropertiesDefaults(HystrixThreadPoolProperties.Setter().withCoreSize(Constants.SEND_COMMAND_POOL_SIZE))
                .andThreadPoolPropertiesDefaults(HystrixThreadPoolProperties.Setter().withMaxQueueSize(Constants.BOUND)));
        this.sqs = sqs;
        this.queueUrl = queueUrl;
        this.messages = messages;
    }

    @Override
    protected SendMessageBatchResult run() throws Exception {
        final List<SendMessageBatchRequestEntry> entries = new ArrayList<SendMessageBatchRequestEntry>(messages.size());
        final Iterator<String> iter = messages.iterator();
        for (int i = 0; iter.hasNext(); i++) {
            entries.add(new SendMessageBatchRequestEntry(String.valueOf(i), iter.next()));
        }
        return sqs.sendMessageBatch(queueUrl, entries);
    }
}
