package com.mmiladinovic.sqs;

import com.beust.jcommander.IValueValidator;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.converters.FileConverter;
import com.google.common.base.Strings;

import java.io.File;

/**
 * Created with IntelliJ IDEA.
 * User: miroslavmiladinovic
 * Date: 26/04/2014
 * Time: 15:30
 * To change this template use File | Settings | File Templates.
 */
public class CmdOptions {

    public static class RangeValidator implements IValueValidator<Integer> {
        @Override
        public void validate(String name, Integer value) throws ParameterException {
            if ("-workerPool".equalsIgnoreCase(name) && (value < 1 || value >= 200)) {
                throw new ParameterException("workerPool option should be positive and less than 200");
            }
            if ("-runTimeSec".equalsIgnoreCase(name) && (value < 1 || value >= 3600)) {
                throw new ParameterException("runTimeSec option should be positive and less than 3600");
            }
        }
    }


    @Parameter(names = "-zk", description = "ZooKeeper connections string, comma separated host:port")
    private String zk;

    @Parameter(names = "-nodes", description = "number of test nodes to wait for")
    private int nodes = 1;

    @Parameter(names = "-awsAccessKey", description = "AWS access key", required = true)
    private String awsAccessKey;

    @Parameter(names = "-awsSecretKey", description = "AWS secret key", required = true)
    private String awsSecretKey;

    @Parameter(names = "-workerPool", description = "number of threads in SQS sender pool", validateValueWith = RangeValidator.class)
    private int workerPool = 20;

    @Parameter(names = "-runTimeSec", description = "test run time in seconds", validateValueWith = RangeValidator.class)
    private int runTimeSec = 10;

    @Parameter(names = "-useQueueUrl", description = "existing SQS queue to use")
    private String queueUrl;

    @Parameter(names = "-reportTo", description = "file path to send metrics report to; " +
            "if not specified then console reporter is used", converter = FileConverter.class)
    private File reportToFile;

    @Parameter(names = "-reportIntervalSec", description = "interval in seconds at which to send metrics report")
    private int reportIntervalSec = 10;

    @Parameter(names = "-noWait", description = "do not wait until the following minute before starting test run. " +
            "wait is useful for syncing up with other test clients.")
    private boolean noWaitBeforeStart = false;

    @Parameter(names = "-help", help = true)
    private boolean help;

    public String getAwsAccessKey() {
        return awsAccessKey;
    }

    public String getAwsSecretKey() {
        return awsSecretKey;
    }

    public int getWorkerPool() {
        return workerPool;
    }

    public int getRunTimeSec() {
        return runTimeSec;
    }

    public String getQueueUrl() {
        return queueUrl;
    }

    public boolean hasQueueUrl() {
        return !Strings.isNullOrEmpty(queueUrl);
    }

    public File getReportToFile() {
        return reportToFile;
    }

    public boolean hasFileReporter() {
        return reportToFile != null;
    }

    public int getReportIntervalSec() {
        return reportIntervalSec;
    }


    public boolean isWaitRequired() {
        return !noWaitBeforeStart;
    }

    public String getZk() {
        return zk;
    }

    public int getNodes() {
        return nodes;
    }
}
