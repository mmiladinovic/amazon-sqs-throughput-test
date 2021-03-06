package com.mmiladinovic.sqs.producer;

import com.amazonaws.services.sqs.AmazonSQSClient;
import com.mmiladinovic.sqs.Constants;
import com.netflix.hystrix.HystrixCommand;
import com.netflix.hystrix.HystrixCommandGroupKey;
import com.netflix.hystrix.HystrixCommandProperties;
import com.netflix.hystrix.HystrixThreadPoolProperties;

import static com.netflix.hystrix.HystrixCommand.Setter.withGroupKey;

/**
 * Created with IntelliJ IDEA.
 * User: miroslavmiladinovic
 * Date: 19/04/2014
 * Time: 19:33
 * To change this template use File | Settings | File Templates.
 */
public class SendMessageSyncCommand extends HystrixCommand<String> {

    private final AmazonSQSClient sqs;
    private final String queueUrl;
    private final String message;

    public SendMessageSyncCommand(String message, String queueUrl, AmazonSQSClient sqs) {
        super(withGroupKey(HystrixCommandGroupKey.Factory.asKey("com.mmiladinovic.sqs.producer.SendMessageSyncCommand"))
                .andCommandPropertiesDefaults(HystrixCommandProperties.Setter()
                        .withExecutionIsolationThreadTimeoutInMilliseconds(Constants.SEND_COMMAND_TIMEOUT))
                .andThreadPoolPropertiesDefaults(HystrixThreadPoolProperties.Setter().withCoreSize(Constants.SEND_COMMAND_POOL_SIZE))
                .andThreadPoolPropertiesDefaults(HystrixThreadPoolProperties.Setter().withQueueSizeRejectionThreshold(Constants.BOUND))
                .andThreadPoolPropertiesDefaults(HystrixThreadPoolProperties.Setter().withMaxQueueSize(Constants.BOUND)));
        this.sqs = sqs;
        this.queueUrl = queueUrl;
        this.message = message;
    }

    @Override
    protected String run() throws Exception {
        return sqs.sendMessage(queueUrl, message).getMessageId();
    }
}
