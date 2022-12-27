package com.ctrip.framework.drc.replicator.impl.inbound.schema.task.restore;

import com.wix.mysql.config.MysqldConfig;
import de.flapdoodle.embed.process.config.IRuntimeConfig;
import de.flapdoodle.embed.process.distribution.Distribution;
import de.flapdoodle.embed.process.exceptions.DistributionException;
import de.flapdoodle.embed.process.extract.IExtractedFileSet;
import de.flapdoodle.embed.process.runtime.Starter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * @Author limingdong
 * @create 2022/10/27
 */
public class RestoredMysqldStarter extends Starter<MysqldConfig, RestoredMysqldExecutable, RestoredMysqldProcess> {

    private static Logger logger = LoggerFactory.getLogger(RestoredMysqldStarter.class);

    private IRuntimeConfig runtime;

    public RestoredMysqldStarter(final IRuntimeConfig config) {
        super(config);
        this.runtime = config;
    }

    public RestoredMysqldExecutable prepare(MysqldConfig config, Distribution distribution) {
        try {
            IExtractedFileSet files = runtime.getArtifactStore().extractFileSet(distribution);
            return newExecutable(config, distribution, runtime, files);
        } catch (IOException iox) {
            String messageOnException = config.supportConfig().messageOnException(getClass(), iox);
            if (messageOnException==null) {
                messageOnException="prepare executable";
            }
            logger.error(messageOnException, iox);
            throw new DistributionException(distribution,iox);
        }
    }

    @Override
    protected RestoredMysqldExecutable newExecutable(
            final MysqldConfig config,
            final Distribution distribution,
            final IRuntimeConfig runtime,
            final IExtractedFileSet exe) {
        return new RestoredMysqldExecutable(distribution, config, runtime, exe);
    }
}