package com.ctrip.framework.drc.replicator.impl.oubound.channel;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;

/**
 * @Author limingdong
 * @create 2020/12/8
 */
public abstract class AbstractFileAllocator {

    protected static final byte[] data = "drc".getBytes();

    protected File file;

    protected FileChannel getFileChannel() throws IOException {
        file = newFile();
        RandomAccessFile randomAccessFile = new RandomAccessFile(file, "r");
        FileChannel fileChannel = randomAccessFile.getChannel();
        return fileChannel;
    }

    private static File newFile() throws IOException {
        File file = File.createTempFile("drc-", ".tmp");
        file.deleteOnExit();

        final FileOutputStream out = new FileOutputStream(file);
        out.write(data);
        out.close();
        return file;
    }
}
