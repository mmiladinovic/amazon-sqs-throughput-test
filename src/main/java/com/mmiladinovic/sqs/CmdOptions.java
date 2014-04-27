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
            if ("-senderPool".equalsIgnoreCase(name) && (value < 1 || value >= 200)) {
                throw new ParameterException("senderPool option should be positive and less than 200");
            }
            if ("-runTimeSec".equalsIgnoreCase(name) && (value < 1 || value >= 3600)) {
                throw new ParameterException("runTimeSec option should be positive and less than 3600");
            }
        }
    }

    @Parameter(names = "-senderPool", description = "number of threads in SQS sender pool", validateValueWith = RangeValidator.class)
    private int senderPool = 20;

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

    @Parameter(names = "--help", help = true)
    private boolean help;

    public int getSenderPool() {
        return senderPool;
    }

    public int getRunTimeSec() {
        return runTimeSec;
    }

    public String getQueueUrl() {
        return queueUrl;
    }

    public boolean hasQueueUrl() {
        return Strings.isNullOrEmpty(queueUrl);
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

    public boolean isNoWaitBeforeStart() {
        return noWaitBeforeStart;
    }
}
