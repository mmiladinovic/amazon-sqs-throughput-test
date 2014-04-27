package com.mmiladinovic.sqs.consumer;

/**
 * Created with IntelliJ IDEA.
 * User: miroslavmiladinovic
 * Date: 26/04/2014
 * Time: 17:56
 * To change this template use File | Settings | File Templates.
 */
public class SQSMessage {
    private final String messageBody;
    private final String messageHandle;

    public SQSMessage(String messageBody, String messageHandle) {
        this.messageBody = messageBody;
        this.messageHandle = messageHandle;
    }

    public String getMessageBody() {
        return messageBody;
    }

    public String getMessageHandle() {
        return messageHandle;
    }
}
