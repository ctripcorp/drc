package com.ctrip.framework.drc.replicator.store;

import com.ctrip.framework.drc.core.driver.binlog.LogEvent;
import com.ctrip.framework.drc.core.driver.binlog.gtid.GtidManager;
import com.ctrip.framework.drc.core.driver.binlog.manager.SchemaManager;
import com.ctrip.framework.drc.core.server.config.replicator.ReplicatorConfig;
import com.ctrip.framework.drc.replicator.container.zookeeper.UuidOperator;
import com.ctrip.framework.drc.replicator.store.manager.file.DefaultFileManager;
import com.ctrip.framework.drc.replicator.store.manager.file.FileManager;
import com.ctrip.framework.drc.replicator.store.manager.gtid.DefaultGtidManager;
import com.ctrip.xpipe.lifecycle.AbstractLifecycle;
import com.ctrip.xpipe.lifecycle.LifecycleHelper;
import com.ctrip.xpipe.utils.VisibleForTesting;
import io.netty.buffer.ByteBuf;

import java.io.IOException;
import java.util.Collection;

/**
 * Created by mingdongli
 * 2019/9/18 上午9:00.
 */
public class FilePersistenceEventStore extends AbstractLifecycle implements EventStore {

    private EventWriter writerDelegate;

    private FileManager fileManager;

    private GtidManager gtidManager;

    public FilePersistenceEventStore(SchemaManager schemaManager, UuidOperator uuidOperator, ReplicatorConfig replicatorConfig) {
        fileManager = new DefaultFileManager(schemaManager, replicatorConfig.getRegistryKey());
        gtidManager = new DefaultGtidManager(fileManager, uuidOperator, replicatorConfig);
        this.writerDelegate = new DumpResponseEventWriter(fileManager);
    }

    @VisibleForTesting
    public FilePersistenceEventStore(FileManager fileManager, GtidManager gtidManager) {
        this.fileManager = fileManager;
        this.gtidManager = gtidManager;
        this.writerDelegate = new DumpResponseEventWriter(fileManager);
    }

    @Override
    protected void doInitialize() throws Exception{
        super.doInitialize();
        LifecycleHelper.initializeIfPossible(fileManager);
        fileManager.setGtidManager(gtidManager);
        LifecycleHelper.initializeIfPossible(gtidManager);
        LifecycleHelper.initializeIfPossible(writerDelegate);
    }

    @Override
    protected void doStart() throws Exception{
        super.doStart();
        LifecycleHelper.startIfPossible(fileManager);
        LifecycleHelper.startIfPossible(gtidManager);
        LifecycleHelper.startIfPossible(writerDelegate);
    }

    @Override
    public void write(byte[] data) {
        writerDelegate.write(data);
    }

    @Override
    public void write(Collection<ByteBuf> byteBuf) {
        this.write(byteBuf, false);
    }

    @Override
    public void write(Collection<ByteBuf> byteBuf, boolean isDdl) {
        writerDelegate.write(byteBuf, isDdl);
    }

    @Override
    public void write(LogEvent logEvent) {
        writerDelegate.write(logEvent);
    }

    @Override
    protected void doStop() throws Exception{
        super.doStop();
        LifecycleHelper.stopIfPossible(writerDelegate);
        LifecycleHelper.stopIfPossible(gtidManager);
        LifecycleHelper.stopIfPossible(fileManager);
    }

    @Override
    protected void doDispose() throws Exception {
        super.doDispose();
        LifecycleHelper.disposeIfPossible(writerDelegate);
        LifecycleHelper.disposeIfPossible(gtidManager);
        LifecycleHelper.disposeIfPossible(fileManager);
    }

    @Override
    public void flush() throws IOException {
        writerDelegate.flush();
    }

    @Override
    public void destroy() throws Exception {
        fileManager.destroy();
    }

    @Override
    public FileManager getFileManager() {
        return fileManager;
    }

    @Override
    public GtidManager getGtidManager() {
        return gtidManager;
    }
}
