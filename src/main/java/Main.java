import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.DeleteQueueRequest;
import com.codahale.metrics.CsvReporter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Slf4jReporter;
import com.google.common.base.Strings;
import com.google.common.util.concurrent.RateLimiter;
import com.netflix.hystrix.strategy.HystrixPlugins;
import com.netflix.hystrix.strategy.concurrency.HystrixConcurrencyStrategy;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.swing.text.DateFormatter;
import java.io.File;
import java.text.SimpleDateFormat;
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

    private AmazonSQSClient sqs;
    private String queueUrl;
    private boolean deleteQueueAfterwards = true;

    private MessageProducer producer;
    private Thread producerThread;
    private List<MessageSender> consumers;
    private List<Thread> consumerThreads;

    private final int senderPoolSize;
    private int totalMessagesSent;


    public Main(int senderPoolSize) {
        if (senderPoolSize < 0 || senderPoolSize > 200) {
            this.senderPoolSize = 20;
        }
        else {
            this.senderPoolSize = senderPoolSize;
        }
        logger.info("sender pool size is {}", senderPoolSize);
    }

    public void initSQS(String queueUrl) {
        if (sqs != null) {
            throw new IllegalStateException("sqs already initialized");
        }
        ClientConfiguration config = new ClientConfiguration();

        Region usWest2 = Region.getRegion(Regions.fromName(Constants.AWS_REGION));
        sqs = usWest2.createClient(AmazonSQSClient.class, new DefaultAWSCredentialsProviderChain(), config);

        if (Strings.isNullOrEmpty(queueUrl)) {
            CreateQueueRequest createQueueRequest = new CreateQueueRequest(UUID.randomUUID().toString());
            this.queueUrl = sqs.createQueue(createQueueRequest).getQueueUrl();
            logger.info("created SQS queue {}", queueUrl);
        }
        else {
            this.queueUrl = queueUrl; this.deleteQueueAfterwards = false;
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

    public void startThreads(int reportIntervalSeconds) {
        if (producerThread != null) {
            throw new IllegalStateException("threads already started");
        }
        BlockingQueue<SimpleMessage> queue = new LinkedBlockingDeque<SimpleMessage>(Constants.BOUND);
        BlockingQueue<String> dlq = new ArrayBlockingQueue<String>(Constants.BOUND);

        producer = new MessageProducer(queue);
        producerThread = new Thread(producer);
        producerThread.start();

        MetricRegistry metricRegistry = new MetricRegistry();
        consumers = new ArrayList<MessageSender>();
        consumerThreads = new ArrayList<Thread>();
        for (int i = 0; i < senderPoolSize; i++) {
            MessageSender r = new MessageSender(queueUrl, sqs, queue, metricRegistry);

            Thread t = new Thread(r); t.start();
            consumers.add(r);
            consumerThreads.add(t);
        }

//        final Slf4jReporter reporter = Slf4jReporter.forRegistry(metricRegistry)
//                .outputTo(LoggerFactory.getLogger("com.mmiladinovic.metrics"))
//                .convertRatesTo(TimeUnit.SECONDS)
//                .convertDurationsTo(TimeUnit.MILLISECONDS)
//                .build();
//        reporter.start(reportIntervalSeconds, TimeUnit.SECONDS);
        final CsvReporter reporter = CsvReporter.forRegistry(metricRegistry)
                .formatFor(Locale.UK)
                .convertRatesTo(TimeUnit.SECONDS)
                .convertDurationsTo(TimeUnit.MILLISECONDS)
                .build(new File(System.getProperty("user.home")+"/"));
        reporter.start(reportIntervalSeconds, TimeUnit.SECONDS);
    }

    public void stopThreads() {
        logger.info("shutting down producers and senders");
        producer.cancel();
        producerThread.interrupt();
        int totalSent = 0;
        for (MessageSender sender : consumers) {
            sender.cancel();
            totalSent += sender.getMessagesSent();
        }
        for (Thread t : consumerThreads) {
            t.interrupt();
        }
        this.totalMessagesSent = totalSent;
    }

    public int getTotalMessagesSent() {
        return totalMessagesSent;
    }



    public static void main(String[] args) throws Exception {
        int senderPoolSize = Integer.valueOf(args[0]);
        int runTimeSeconds = Integer.valueOf(args[1]);
        String queueURL = args[2];
        int reportIntervalSeconds = args[3] != null ? Integer.valueOf(args[3]) : 1;

        Main m = new Main(senderPoolSize);
        m.initSQS(StringUtils.startsWith(queueURL, "'") ? StringUtils.remove(queueURL, "'") : queueURL);

        long sleepTime = sleepTimeMillis();
        logger.info("senderPoolSize {}, runTimeInSeconds {}, waiting for another {} seconds until {} to kick off the test",
                senderPoolSize, runTimeSeconds, TimeUnit.SECONDS.convert(sleepTime, TimeUnit.MILLISECONDS), new Date(System.currentTimeMillis()+sleepTime));

        Thread.sleep(sleepTime);

        m.startThreads(reportIntervalSeconds);

        logger.info("running SQS test for {} seconds", runTimeSeconds);
        Thread.sleep(TimeUnit.SECONDS.toMillis(runTimeSeconds));

        m.stopThreads();
        m.cleanupSQS();

        Thread.sleep(TimeUnit.SECONDS.toMillis(reportIntervalSeconds+1));

        logger.info("total messages sent {}, at rate {}/second", m.getTotalMessagesSent(), m.getTotalMessagesSent() / runTimeSeconds);
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
