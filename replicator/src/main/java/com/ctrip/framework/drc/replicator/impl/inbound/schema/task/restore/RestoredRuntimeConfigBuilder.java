package com.ctrip.framework.drc.replicator.impl.inbound.schema.task.restore;

import com.wix.mysql.config.DownloadConfig;
import com.wix.mysql.config.MysqldConfig;
import de.flapdoodle.embed.process.config.io.ProcessOutput;
import de.flapdoodle.embed.process.io.IStreamProcessor;
import de.flapdoodle.embed.process.runtime.ICommandLinePostProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static de.flapdoodle.embed.process.io.Processors.logTo;
import static de.flapdoodle.embed.process.io.Slf4jLevel.INFO;

/**
 * @Author limingdong
 * @create 2022/10/26
 */
public class RestoredRuntimeConfigBuilder extends de.flapdoodle.embed.process.config.RuntimeConfigBuilder {

    private Logger logger = LoggerFactory.getLogger(RestoredRuntimeConfigBuilder.class);

    private IStreamProcessor log = logTo(logger, INFO);

    public RestoredRuntimeConfigBuilder defaults(
            final MysqldConfig mysqldConfig,
            final DownloadConfig downloadConfig) throws Exception {

        processOutput().setDefault(new ProcessOutput(log, log, log));
        commandLinePostProcessor().setDefault(new ICommandLinePostProcessor.Noop());
        daemonProcess(false);
        artifactStore().setDefault(new RestoredArtifactStoreBuilder().defaults(mysqldConfig, downloadConfig).build());
        return this;
    }
}
