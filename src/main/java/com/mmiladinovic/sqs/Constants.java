package com.mmiladinovic.sqs;

/**
 * Created with IntelliJ IDEA.
 * User: miroslavmiladinovic
 * Date: 19/04/2014
 * Time: 20:07
 * To change this template use File | Settings | File Templates.
 */
public interface Constants {
    public static final int BOUND = 100;

    public static final int SEND_COMMAND_TIMEOUT = 5000;
    public static final int SEND_COMMAND_POOL_SIZE = 20;

    public static final String AWS_REGION = "us-west-2";

    // message producer metrics
    public static final String METER_PRODUCER_MESSAGES_DEQUEUED = "meter-producer-messages-dequeued";
    public static final String METER_PRODUCER_SENT_TO_SQS = "meter-producer-sent-to-sqs";
    public static final String METER_PRODUCER_SQS_SEND_FAIL = "meter-producer-sqs-send-fail";
    public static final String TIMER_PRODUCER_SQS_SEND = "timer-producer-sqs-send";

    // message poller metrics
    public static final String METER_POLLER_MESSAGES_POLLED = "meter-poller-messages-polled";
    public static final String TIMER_POLLER_SQS_RECEIVE = "timer-poller-sqs-receive";
    public static final String COUNTER_POLLER_SQS_RECEIVE_ERROR = "counter-poller-sqs-receieve-error";

    public static final String GAUGE_CONSUMER_QUEUE_SIZE = "gauge-consumer-queue-size";
    public static final String HISTOGRAM_MESSAGE_LATENCY_ACROSS_SQS = "histogram-message-latency-x-sqs";

    // message consumer (delete) metrics
    public static final String METER_CONSUMER_MESSAGES_CONSUMED = "meter-consumer-messages-consumed";
    public static final String TIMER_CONSUMER_SQS_DELETE = "timer-consumer-sqs-delete";
    public static final String COUNTER_CONSUMER_SQS_DELETE_ERROR = "couner-consumer-sqs-delete-error";
    public static final String COUNTER_CONSUMER_SQS_MESSAGE_DELETE_FAILED = "counter-consumer-sqs-message-delete-failed";

}
