package com.ctrip.framework.drc.replicator.impl.inbound.schema.task.restore;

import com.wix.mysql.config.MysqldConfig;
import de.flapdoodle.embed.process.config.IRuntimeConfig;
import de.flapdoodle.embed.process.distribution.Distribution;
import de.flapdoodle.embed.process.extract.IExtractedFileSet;
import de.flapdoodle.embed.process.runtime.Executable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * @Author limingdong
 * @create 2022/10/27
 */
public class RestoredMysqldExecutable extends Executable<MysqldConfig, RestoredMysqldProcess> {

    private final static Logger logger = LoggerFactory.getLogger(RestoredMysqldExecutable.class);

    public RestoredMysqldExecutable(
            final Distribution distribution,
            final MysqldConfig config,
            final IRuntimeConfig runtimeConfig,
            final IExtractedFileSet executable) {
        super(distribution, config, runtimeConfig, executable);
    }

    @Override
    protected RestoredMysqldProcess start(
            final Distribution distribution,
            final MysqldConfig config,
            final IRuntimeConfig runtime) throws IOException {
        return new RestoredMysqldProcess(distribution, config, runtime, this);
    }

    @Override
    public synchronized void stop() {
        logger.info("[Skip] stop MysqldExecutable");
    }

    public synchronized void destroy() {
        super.stop();
    }
}
