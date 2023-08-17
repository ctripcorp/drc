package com.ctrip.framework.drc.replicator.store;

import com.ctrip.framework.drc.core.driver.binlog.impl.TransactionContext;
import com.ctrip.framework.drc.core.driver.command.netty.codec.FileManager;
import com.ctrip.xpipe.lifecycle.AbstractLifecycle;
import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;

/**
 * for mysql binlog dump command
 * Created by mingdongli
 * 2019/9/17 下午8:15.
 */
public class DumpResponseEventWriter extends AbstractLifecycle implements EventWriter {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private FileManager fileManager;

    public DumpResponseEventWriter(FileManager fileManager) {
        this.fileManager = fileManager;
    }

    @Override
    public void write(Collection<ByteBuf> byteBuf) {
        this.write(byteBuf, new TransactionContext(false));
    }

    @Override
    public void write(Collection<ByteBuf> byteBuf, TransactionContext context) {
        try {
            fileManager.append(byteBuf, context);
        } catch (IOException e) {
            logger.info("append {} error", byteBuf.toString(), e);
        }
    }

    @Override
    protected void doDispose() throws Exception {
        if (fileManager.getLifecycleState().canDispose()) {
            fileManager.dispose();
        }
    }

    @Override
    public void flush() throws IOException {
        fileManager.flush();
    }

    @Override
    public void destroy() throws Exception {

    }
}
