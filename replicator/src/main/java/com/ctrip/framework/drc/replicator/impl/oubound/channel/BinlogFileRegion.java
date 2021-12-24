package com.ctrip.framework.drc.replicator.impl.oubound.channel;

import io.netty.channel.DefaultFileRegion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.channels.FileChannel;

/**
 * @Author limingdong
 * @create 2020/12/4
 */
public class BinlogFileRegion extends DefaultFileRegion {

    private static final Logger logger = LoggerFactory.getLogger(BinlogFileRegion.class);

    private String registryKey;

    private String fileName;

    public BinlogFileRegion(FileChannel file, long position, long count) {
        super(file, position, count);
    }

    public BinlogFileRegion(FileChannel file, long position, long count, String registryKey, String fileName) {
        super(file, position, count);
        this.registryKey = registryKey;
        this.fileName = fileName;
    }

    @Override
    protected void deallocate() {
        super.deallocate();
        logger.info("[deallocate] called for {}:{}", registryKey, fileName);
    }

}