package com.ctrip.framework.drc.replicator.impl.oubound.channel;

import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Test;

import java.io.*;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;

import static org.junit.Assert.assertEquals;

/**
 * @Author limingdong
 * @create 2020/12/5
 */
public class BinlogFileRegionTest extends AbstractFileAllocator {

    private static final byte[] data = "drc".getBytes();

    @Test
    public void transferZero() throws IOException {
        FileChannel fileChannel = getFileChannel();
        fileChannel.position(data.length);
        BinlogFileRegion region = new BinlogFileRegion(fileChannel, data.length, 0, RandomStringUtils.random(10), RandomStringUtils.random(10));
        try {
            assertEquals(0, region.count());
            assertEquals(0, region.transferred());

            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            WritableByteChannel channel = Channels.newChannel(outputStream);

            assertEquals(0, region.transferTo(channel, 0));
            assertEquals(0, region.count());
            assertEquals(0, region.transferred());
        } finally {
            region.release();
            file.delete();
        }
    }

    @Test
    public void transferNonZero() throws IOException {
        FileChannel fileChannel = getFileChannel();
        BinlogFileRegion region = new BinlogFileRegion(fileChannel, 0, data.length);

        try {
            assertEquals(data.length, region.count());
            assertEquals(0, region.transferred());

            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            WritableByteChannel channel = Channels.newChannel(outputStream);

            assertEquals(data.length, region.transferTo(channel, 0));
            assertEquals(data.length, region.count());
            assertEquals(data.length, region.transferred());
        } finally {
            region.release();
            file.delete();
        }
    }

}