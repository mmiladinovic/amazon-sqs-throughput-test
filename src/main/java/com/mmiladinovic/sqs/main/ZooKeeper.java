package com.mmiladinovic.sqs.main;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;

/**
 * Created with IntelliJ IDEA.
 * User: miroslavmiladinovic
 * Date: 04/05/2014
 * Time: 18:39
 * To change this template use File | Settings | File Templates.
 */
public class ZooKeeper {

    static final CuratorFramework startForZkServer(String zkConnString) {
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
        CuratorFramework client = CuratorFrameworkFactory.newClient(zkConnString, retryPolicy);
        client.start();
        return client;
    }

}
