package com.ctrip.framework.drc.replicator.impl.oubound.channel;

import io.netty.channel.MessageSizeEstimator;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.channels.FileChannel;

import static org.junit.Assert.*;

/**
 * @Author limingdong
 * @create 2020/12/8
 */
public class FileRegionMessageSizeEstimatorTest extends AbstractFileAllocator {

    private MessageSizeEstimator fileRegionMessageSizeEstimator;

    @Before
    public void setUp() throws Exception {
        fileRegionMessageSizeEstimator = FileRegionMessageSizeEstimator.DEFAULT;
    }

    @Test
    public void testSize() throws IOException {
        FileChannel fileChannel = getFileChannel();
        fileChannel.position(data.length);
        BinlogFileRegion region = new BinlogFileRegion(fileChannel, 0, data.length, RandomStringUtils.random(10), RandomStringUtils.random(10));
        try {
            int size = fileRegionMessageSizeEstimator.newHandle().size(region);
            assertEquals(size, data.length);
        } finally {
            region.release();
            file.delete();
        }
    }
}