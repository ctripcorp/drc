package com.ctrip.framework.drc.replicator.store;

import com.ctrip.framework.drc.replicator.store.manager.file.FileManager;
import com.ctrip.framework.drc.core.driver.binlog.gtid.GtidManager;

/**
 * Created by mingdongli
 * 2019/9/17 下午7:55.
 */
public interface EventStore extends EventWriter {

    FileManager getFileManager();

    GtidManager getGtidManager();
}
