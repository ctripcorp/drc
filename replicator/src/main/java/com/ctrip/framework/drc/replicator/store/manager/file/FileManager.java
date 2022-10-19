package com.ctrip.framework.drc.replicator.store.manager.file;

import com.ctrip.framework.drc.core.driver.binlog.gtid.GtidManager;
import com.ctrip.framework.drc.core.driver.binlog.gtid.GtidReader;
import com.ctrip.framework.drc.core.driver.binlog.gtid.GtidSet;
import com.ctrip.framework.drc.core.driver.binlog.impl.TransactionContext;
import com.ctrip.framework.drc.core.server.observer.gtid.GtidObservable;
import com.ctrip.xpipe.api.lifecycle.Destroyable;
import com.ctrip.xpipe.api.lifecycle.Lifecycle;
import io.netty.buffer.ByteBuf;

import java.io.File;
import java.io.Flushable;
import java.io.IOException;
import java.util.Collection;

/**
 * Created by mingdongli
 * 2019/9/17 下午8:51.
 */
public interface FileManager extends GtidReader, GtidObservable, Flushable, Destroyable, Lifecycle {

    boolean append(Collection<ByteBuf> byteBufs, TransactionContext context) throws IOException;

    boolean append(ByteBuf byteBuf) throws IOException;

    File getDataDir();

    File getCurrentLogFile();

    File getFirstLogFile();

    File getNextLogFile(File current);

    boolean gtidExecuted(File currentFile, GtidSet executedGtid);

    /**
     * 文件第一条Previous_Gtid_Event由GtidManager生成
     * @param gtidManager
     */
    void setGtidManager(GtidManager gtidManager);

    void rollLog() throws IOException;

    void purge();

}
