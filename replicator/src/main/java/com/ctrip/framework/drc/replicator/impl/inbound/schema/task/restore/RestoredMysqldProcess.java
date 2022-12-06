package com.ctrip.framework.drc.replicator.impl.inbound.schema.task.restore;

import com.wix.mysql.config.MysqldConfig;
import com.wix.mysql.io.NotifyingStreamProcessor;
import de.flapdoodle.embed.process.config.IRuntimeConfig;
import de.flapdoodle.embed.process.distribution.Distribution;
import de.flapdoodle.embed.process.distribution.Platform;
import de.flapdoodle.embed.process.io.StreamToLineProcessor;
import de.flapdoodle.embed.process.io.file.Files;
import de.flapdoodle.embed.process.runtime.IStopable;
import de.flapdoodle.embed.process.runtime.Processes;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static com.wix.mysql.utils.Utils.closeCloseables;
import static com.wix.mysql.utils.Utils.readToString;
import static de.flapdoodle.embed.process.distribution.Platform.Windows;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * @Author limingdong
 * @create 2022/10/28
 */
public class RestoredMysqldProcess implements IStopable {

    private static Logger logger = LoggerFactory.getLogger(RestoredMysqldProcess.class);

    private final static long MYSQLADMIN_SHUTDOWN_TIMEOUT_SECONDS = 10;

    private final MysqldConfig config;

    private final IRuntimeConfig runtimeConfig;

    private final RestoredMysqldExecutable executable;

    private long processId;

    private boolean stopped = false;

    private final Distribution distribution;

    private final File pidFile;

    private NotifyingStreamProcessor outputWatch;

    public RestoredMysqldProcess(Distribution distribution, MysqldConfig config, IRuntimeConfig runtimeConfig, RestoredMysqldExecutable executable)
            throws IOException {
        this.config = config;
        this.runtimeConfig = runtimeConfig;
        this.executable = executable;
        this.distribution = distribution;
        // pid file needs to be set before ProcessBuilder is called
        this.pidFile = pidFile(this.executable.getFile().executable());
        this.processId = getPidFromFile(pidFile);
        logger.info("Restore mysqld of pid {}", processId);
        outputWatch = new NotifyingStreamProcessor(StreamToLineProcessor.wrap(runtimeConfig.getProcessOutput().getOutput()));
    }

    @Override
    public synchronized final void stop() {
        if (!stopped) {
            stopped = true;
            stopInternal();
            if (!Files.forceDelete(pidFile)) {
                logger.warn("Could not delete pid file: {}", pidFile);
            }
        }
    }

    protected File pidFile(File executeableFile) {
        return new File(executeableFile.getParentFile(),executableBaseName(executeableFile.getName())+".pid");
    }

    private String executableBaseName(String name) {
        int idx=name.lastIndexOf('.');
        if (idx!=-1) {
            return name.substring(0,idx);
        }
        return name;
    }

    protected synchronized void stopInternal() {
        logger.info("try to stop mysqld");
        if (!stopUsingMysqldadmin()) {
            logger.warn("could not stop mysqld via mysqladmin, try next");
            if (!attemptToKillProcessAndWaitForItToDie(this::sendKillToProcess, 5000)) {
                logger.warn("could not stop mysqld, try next");
                if (!attemptToKillProcessAndWaitForItToDie(this::sendTermToProcess, 5000)) {
                    logger.warn("could not stop mysqld, try next");
                    if (!attemptToKillProcessAndWaitForItToDie(this::tryKillToProcess, 5000)) {
                        logger.warn("could not stop mysqld the second time, try one last thing");
                    }
                }
            }
        }
        logger.info("mysqld stopped.");
    }

    private boolean attemptToKillProcessAndWaitForItToDie(Supplier<Boolean> killCode, int millisecondsToWait){
        boolean result = killCode.get();
        if (!result) {
            return false;
        }
        Instant start = Instant.now();
        Instant timeoutPoint= start.plus(Duration.ofMillis(millisecondsToWait));
        logger.debug("Checking if process dies...");
        do {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            if (!isProcessRunning()) {
                return true;
            }
            logger.debug("Process still up after {} milliseconds", Duration.between(start, Instant.now()).toMillis());
        } while(Instant.now().isBefore(timeoutPoint));
        return false;
    }

    public boolean isProcessRunning() {
        if (processId > 0) {
            return Processes.isProcessRunning(distribution.getPlatform(), processId);
        }
        return false;
    }

    private boolean stopUsingMysqldadmin() {
        NotifyingStreamProcessor.ResultMatchingListener shutdownListener = outputWatch.addListener(new NotifyingStreamProcessor.ResultMatchingListener(": Shutdown complete"));
        boolean retValue = false;
        Reader stdErr = null;
        try {
            String cmd = Paths.get(executable.getFile().baseDir().getAbsolutePath(), "bin", "mysqladmin").toString();
            ProcessBuilder pb = new ProcessBuilder(Arrays.asList(cmd, "--no-defaults", "--protocol=tcp",
                    format("-u%s", MysqldConfig.SystemDefaults.USERNAME),
                    format("--port=%s", config.getPort()),
                    "shutdown"));
            Process p = pb.start();
            logger.debug("mysqladmin started");
            stdErr = new InputStreamReader(p.getErrorStream());
            retValue = p.waitFor(MYSQLADMIN_SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            if (!retValue) {
                p.destroy();
                logger.error("mysql shutdown timed out after {} seconds", MYSQLADMIN_SHUTDOWN_TIMEOUT_SECONDS);
            }
            else {
                retValue = p.exitValue() == 0;
                if (retValue) {
                    shutdownListener.waitForResult(config.getTimeout(MILLISECONDS));

                    //TODO: figure out a better strategy for this. It seems windows does not actually shuts down process after it says it does.
                    if (Platform.detect() == Windows) {
                        Thread.sleep(2000);
                    }

                    if (!shutdownListener.isInitWithSuccess()) {
                        logger.error("mysql shutdown failed. Expected to find in output: 'Shutdown complete', got: " + shutdownListener.getFailureFound());
                        retValue = false;
                    } else {
                        logger.debug("mysql shutdown succeeded.");
                        retValue = true;
                    }

                } else {
                    String errOutput = readToString(stdErr);

                    if (errOutput.contains("Can't connect to MySQL server on")) {
                        logger.warn("mysql was already shutdown - no need to add extra shutdown hook - process does it out of the box.");
                        retValue = true;
                    } else {
                        logger.error("mysql shutdown failed with error code: " + p.waitFor() + " and message: " + errOutput);
                    }
                }
            }

        } catch (InterruptedException | IOException e) {
            logger.warn("Encountered error why shutting down process.", e);
        } finally {
            closeCloseables(stdErr);
        }

        return retValue;
    }

    protected boolean sendKillToProcess() {
        if (processId > 0) {
            return Processes.killProcess(config.supportConfig(), distribution.getPlatform(),
                    StreamToLineProcessor.wrap(runtimeConfig.getProcessOutput().getCommands()), processId);
        }
        return false;
    }

    protected boolean sendTermToProcess() {
        if (processId > 0) {
            return Processes.termProcess(config.supportConfig(), distribution.getPlatform(),
                    StreamToLineProcessor.wrap(runtimeConfig.getProcessOutput().getCommands()), processId);
        }
        return false;
    }

    protected boolean tryKillToProcess() {
        if (processId > 0) {
            return Processes.tryKillProcess(config.supportConfig(), distribution.getPlatform(),
                    StreamToLineProcessor.wrap(runtimeConfig.getProcessOutput().getCommands()), processId);
        }
        return false;
    }

    @Override
    public boolean isRegisteredJobKiller() {
        return false;
    }


    public static int getPidFromFile(File pidFile) throws IOException {
        // wait for file to be created
        int tries = 0;
        while (!pidFile.exists() && tries < 5) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e1) {
                // ignore
            }
            tries++;
        }
        // don't check file to be there. want to throw IOException if
        // something happens
        if (!pidFile.exists()) {
            throw new IOException("Could not find pid file " + pidFile);
        }

        // read the file, wait for the pid string to appear
        String fileContent = StringUtils.chomp(StringUtils.strip(new String(java.nio.file.Files.readAllBytes(pidFile.toPath()))));
        tries = 0;
        while (StringUtils.isBlank(fileContent) && tries < 5) {
            fileContent = StringUtils.chomp(StringUtils.strip(new String(java.nio.file.Files.readAllBytes(pidFile.toPath()))));
            try {
                Thread.sleep(100);
            } catch (InterruptedException e1) {
                // ignore
            }
            tries++;
        }
        // check for empty file
        if (StringUtils.isBlank(fileContent)) {
            throw new IOException("Pidfile " + pidFile + "does not contain a pid. Waited for " + tries * 100 + "ms.");
        }
        // pidfile exists and has content
        try {
            return Integer.parseInt(fileContent);
        } catch (NumberFormatException e) {
            throw new IOException("Pidfile " + pidFile + "does not contain a valid pid. Content: " + fileContent);
        }
    }

    public synchronized void destroy() {
        logger.info("[Invoke] RestoredMysqldProcess.destroy");
        executable.destroy();
    }
}
