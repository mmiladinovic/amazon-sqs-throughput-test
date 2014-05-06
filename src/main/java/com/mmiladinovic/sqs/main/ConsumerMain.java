package com.mmiladinovic.sqs.main;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.internal.StaticCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;
import com.codahale.metrics.CsvReporter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Slf4jReporter;
import com.google.common.base.Throwables;
import com.mmiladinovic.sqs.CmdOptions;
import com.mmiladinovic.sqs.Constants;
import com.mmiladinovic.sqs.consumer.MessageConsumer;
import com.mmiladinovic.sqs.consumer.MessagePoller;
import com.mmiladinovic.sqs.consumer.SQSMessage;
import org.apache.commons.io.FileUtils;
import org.apache.curator.framework.recipes.barriers.DistributedDoubleBarrier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

import static com.codahale.metrics.MetricRegistry.name;

/**
 * Created with IntelliJ IDEA.
 * User: miroslavmiladinovic
 * Date: 26/04/2014
 * Time: 18:20
 * To change this template use File | Settings | File Templates.
 */
public class ConsumerMain {
    private static final Logger logger = LoggerFactory.getLogger(ConsumerMain.class);

    private final MetricRegistry metricRegistry = new MetricRegistry();

    private AmazonSQSClient sqs;

    private List<MessagePoller> pollers;
    private List<MessageConsumer> consumers;

    private final CmdOptions opts;
    private String queueUrl;

    public ConsumerMain(CmdOptions opts) {
        this.opts = opts;
        logger.info("SQS poller pool size is {}", opts.getWorkerPool());
    }

    public void initSQS() {
        if (sqs != null) {
            throw new IllegalStateException("sqs already initialized");
        }
        ClientConfiguration config = new ClientConfiguration();

        Region usWest2 = Region.getRegion(Regions.fromName(Constants.AWS_REGION));
        sqs = usWest2.createClient(AmazonSQSClient.class, new StaticCredentialsProvider(new BasicAWSCredentials(opts.getAwsAccessKey(), opts.getAwsSecretKey())), config);

        if (opts.hasQueueUrl()) {
            this.queueUrl = opts.getQueueUrl();
            logger.info("polling from SQS queue {}", queueUrl);
        }
        else {
            throw new RuntimeException("queueURL must be supplied");
        }
    }

    public void startThreads() {
        if (pollers != null || consumers != null) {
            throw new IllegalStateException("threads already started");
        }
        final BlockingQueue<SQSMessage> queue = new LinkedBlockingDeque<SQSMessage>(100000);
        metricRegistry.register(Constants.GAUGE_CONSUMER_QUEUE_SIZE, new Gauge<Integer>() {
            @Override
            public Integer getValue() {
                return queue.size();
            }
        });

        pollers = new ArrayList<MessagePoller>(opts.getWorkerPool());
        for (int i = 0; i < opts.getWorkerPool(); i++) {
            MessagePoller r = new MessagePoller(queue, queueUrl, sqs, metricRegistry);
            Thread t = new Thread(r); t.start();
            pollers.add(r);
        }

        consumers = new ArrayList<MessageConsumer>(opts.getWorkerPool());
        for (int i = 0; i < opts.getWorkerPool(); i++) {
            MessageConsumer r = new MessageConsumer(queue, queueUrl, sqs, metricRegistry);
            Thread t = new Thread(r); t.start();
            consumers.add(r);
        }

    }

    public void startMetricReporter() {
        if (opts.hasFileReporter()) {
            File subDir = null;
            try {
                subDir = new File(opts.getReportToFile(), UUID.randomUUID().toString());
                FileUtils.forceMkdir(subDir);
            }
            catch (IOException e) {
                Throwables.propagate(e);
            }
            final CsvReporter reporter = CsvReporter.forRegistry(metricRegistry)
                    .formatFor(Locale.UK)
                    .convertRatesTo(TimeUnit.SECONDS)
                    .convertDurationsTo(TimeUnit.MILLISECONDS)
                    .build(subDir);
            reporter.start(opts.getReportIntervalSec(), TimeUnit.SECONDS);
        }
        else {
            final Slf4jReporter reporter = Slf4jReporter.forRegistry(metricRegistry)
                    .outputTo(LoggerFactory.getLogger("com.mmiladinovic.metrics"))
                    .convertRatesTo(TimeUnit.SECONDS)
                    .convertDurationsTo(TimeUnit.MILLISECONDS)
                    .build();
            reporter.start(opts.getReportIntervalSec(), TimeUnit.SECONDS);
        }
    }

    public void stopThreads() {
        logger.info("shutting down pollers and consumers");

        for (MessagePoller poller : pollers) {
            poller.cancel();
        }

        for (MessageConsumer c : consumers) {
            c.cancel();
        }
    }

    public long getTotalMessagesPolled() {
        return metricRegistry.meter(Constants.METER_POLLER_MESSAGES_POLLED).getCount();
    }

    public long getTotalMessagesDeleted() {
        return metricRegistry.meter(Constants.METER_CONSUMER_MESSAGES_CONSUMED).getCount();
    }

    public long getTotalErrors() {
        return metricRegistry.counter(Constants.COUNTER_POLLER_SQS_RECEIVE_ERROR).getCount();
    }

    public static void main(String[] args) throws Exception {
        CmdOptions opts = new CmdOptions();
        JCommander cmder = new JCommander(opts);
        try {
            cmder.parse(args);
        }
        catch (ParameterException e) {
            cmder.usage();
        }


        ConsumerMain m = new ConsumerMain(opts);
        m.initSQS();

        DistributedDoubleBarrier barrier = null;
        if (opts.isWaitRequired()) {
            barrier = new DistributedDoubleBarrier(ZooKeeper.startForZkServer(opts.getZk()), "/sqs-test", opts.getNodes());

            logger.info("about to enter Zk Barrier to wait for {} test nodes", opts.getNodes());
            barrier.enter();
            logger.info("released from Zk Barrier. Off we go...");
        }

        m.startThreads();
        m.startMetricReporter();

        logger.info("running SQS test for {} seconds", opts.getRunTimeSec());
        Thread.sleep(TimeUnit.SECONDS.toMillis(opts.getRunTimeSec()));

        m.stopThreads();

        Thread.sleep(TimeUnit.SECONDS.toMillis(opts.getReportIntervalSec()+1));

        logger.info("total messages polled {}, at rate {}/second. errors count {}",
                m.getTotalMessagesPolled(), m.getTotalMessagesPolled() / opts.getRunTimeSec(), m.getTotalErrors());
        logger.info("total messages deleted {}, at rate {}/second. errors count {}",
                m.getTotalMessagesDeleted(), m.getTotalMessagesDeleted() / opts.getRunTimeSec(), m.getTotalErrors());
        if (barrier != null) {
            barrier.leave();
        }
    }

}
