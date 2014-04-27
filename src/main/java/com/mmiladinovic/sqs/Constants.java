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
    public static final int NO_OF_SENDERS = 20;

    public static final int SEND_COMMAND_TIMEOUT = 5000;
    public static final int SEND_COMMAND_POOL_SIZE = 20;

    public static final String AWS_REGION = "us-west-2";

    public static final String SENT_TO_SQS_METER = "sentToSQSMeter";
    public static final String ERROR_FROM_SQS_METER = "errorFromSQSMeter";

}
