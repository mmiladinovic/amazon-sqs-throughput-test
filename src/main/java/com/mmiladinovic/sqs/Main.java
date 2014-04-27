package com.mmiladinovic.sqs;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.DeleteQueueRequest;
import com.beust.jcommander.JCommander;
import com.codahale.metrics.CsvReporter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Slf4jReporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.*;
import java.util.concurrent.*;

/**
 * Created with IntelliJ IDEA.
 * User: miroslavmiladinovic
 * Date: 19/04/2014
 * Time: 20:02
 * To change this template use File | Settings | File Templates.
 */
public class Main {

    private static final Logger logger = LoggerFactory.getLogger(Main.class);

    private final MetricRegistry metricRegistry = new MetricRegistry();

    private AmazonSQSClient sqs;
    private boolean deleteQueueAfterwards = true;

    private MessageProducer producer;
    private Thread producerThread;
    private List<MessageSender> consumers;
    private List<Thread> consumerThreads;

    private final CmdOptions opts;
    private int totalMessagesSent;
    private String queueUrl;




    public Main(CmdOptions opts) {
        this.opts = opts;
        logger.info("sender pool size is {}", opts.getSenderPool());
    }

    public void initSQS() {
        if (sqs != null) {
            throw new IllegalStateException("sqs already initialized");
        }
        ClientConfiguration config = new ClientConfiguration();

        Region usWest2 = Region.getRegion(Regions.fromName(Constants.AWS_REGION));
        sqs = usWest2.createClient(AmazonSQSClient.class, new DefaultAWSCredentialsProviderChain(), config);

        if (opts.hasQueueUrl()) {
            CreateQueueRequest createQueueRequest = new CreateQueueRequest(UUID.randomUUID().toString());
            this.queueUrl = sqs.createQueue(createQueueRequest).getQueueUrl();
            logger.info("created SQS queue {}", queueUrl);
        }
        else {
            this.queueUrl = opts.getQueueUrl(); this.deleteQueueAfterwards = false;
            logger.info("using existing SQS queue {}", queueUrl);
        }
    }

    public void cleanupSQS() {
        if (sqs == null) {
            throw new IllegalStateException("sqs already cleaned up");
        }
        if (deleteQueueAfterwards) {
            logger.info("about to delete queue {}", queueUrl);
            sqs.deleteQueue(new DeleteQueueRequest(queueUrl));
            logger.info("queue {} deleted", queueUrl);
        }
        queueUrl = null;
        sqs = null;
    }

    public void startThreads() {
        if (producerThread != null) {
            throw new IllegalStateException("threads already started");
        }
        BlockingQueue<SimpleMessage> queue = new LinkedBlockingDeque<SimpleMessage>(Constants.BOUND);
        BlockingQueue<String> dlq = new ArrayBlockingQueue<String>(Constants.BOUND);

        producer = new MessageProducer(queue);
        producerThread = new Thread(producer);
        producerThread.start();

        consumers = new ArrayList<MessageSender>();
        consumerThreads = new ArrayList<Thread>();
        for (int i = 0; i < opts.getSenderPool(); i++) {
            MessageSender r = new MessageSender(queueUrl, sqs, queue, metricRegistry);

            Thread t = new Thread(r); t.start();
            consumers.add(r);
            consumerThreads.add(t);
        }

    }

    public void startMetricReporter() {
        if (opts.hasFileReporter()) {
            final CsvReporter reporter = CsvReporter.forRegistry(metricRegistry)
                    .formatFor(Locale.UK)
                    .convertRatesTo(TimeUnit.SECONDS)
                    .convertDurationsTo(TimeUnit.MILLISECONDS)
                    .build(new File(System.getProperty("user.home") + "/"));
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
        logger.info("shutting down producers and senders");
        producer.cancel();
        producerThread.interrupt();
        for (MessageSender sender : consumers) {
            sender.cancel();
        }
        for (Thread t : consumerThreads) {
            t.interrupt();
        }
    }

    public long getTotalMessagesSent() {
        return metricRegistry.meter(Constants.SENT_TO_SQS_METER).getCount();
    }

    public long getTotalErrors() {
        return metricRegistry.meter(Constants.ERROR_FROM_SQS_METER).getCount();
    }

    public static void main(String[] args) throws Exception {
        CmdOptions opts = new CmdOptions();
        new JCommander(opts, args);

        Main m = new Main(opts);
        m.initSQS();

        if (!opts.isNoWaitBeforeStart()) {
            long sleepTime = sleepTimeMillis();
            logger.info("senderPoolSize {}, runTimeInSeconds {}, waiting for another {} seconds until {} to kick off the test",
                    opts.getSenderPool(), opts.getRunTimeSec(), TimeUnit.SECONDS.convert(sleepTime, TimeUnit.MILLISECONDS), new Date(System.currentTimeMillis()+sleepTime));
            Thread.sleep(sleepTime);
        }

        m.startThreads();
        m.startMetricReporter();

        logger.info("running SQS test for {} seconds", opts.getRunTimeSec());
        Thread.sleep(TimeUnit.SECONDS.toMillis(opts.getRunTimeSec()));

        m.stopThreads();
        m.cleanupSQS();

        Thread.sleep(TimeUnit.SECONDS.toMillis(opts.getReportIntervalSec()+1));

        logger.info("total messages sent {}, at rate {}/second. errors count {}",
                m.getTotalMessagesSent(), m.getTotalMessagesSent() / opts.getRunTimeSec(), m.getTotalErrors());
    }

    private static long sleepTimeMillis() {
        long now = System.currentTimeMillis();
        Calendar cal = Calendar.getInstance();
        cal.setTimeInMillis(now);
        cal.clear(Calendar.SECOND); cal.clear(Calendar.MILLISECOND);
        cal.add(Calendar.MINUTE, 2);
        return cal.getTimeInMillis() - now;
    }


}
