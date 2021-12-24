package com.ctrip.framework.drc.replicator.store.manager.gtid;

import com.ctrip.framework.drc.core.driver.binlog.gtid.GtidManager;
import com.ctrip.framework.drc.core.driver.binlog.gtid.GtidSet;
import com.ctrip.framework.drc.core.server.observer.gtid.GtidObservable;
import com.ctrip.framework.drc.replicator.store.manager.file.FileManager;
import com.ctrip.xpipe.api.observer.Observable;
import com.ctrip.xpipe.lifecycle.AbstractLifecycle;
import com.ctrip.xpipe.lifecycle.LifecycleHelper;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import java.io.File;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by mingdongli
 * 2019/9/18 上午12:07.
 */
public class DefaultGtidManager extends AbstractLifecycle implements GtidManager {

    private GtidSet executed_gtid = new GtidSet(Maps.newLinkedHashMap());

    private GtidSet purged_gtid = new GtidSet(Maps.newLinkedHashMap());

    private Set<String> uuids = Sets.newHashSet();

    private FileManager fileManager;

    public DefaultGtidManager(FileManager fileManager) {
        this.fileManager = fileManager;
    }

    @Override
    public GtidSet getExecutedGtids() {
        return executed_gtid.clone();
    }

    @Override
    public void updateExecutedGtids(GtidSet gtidSet) {
        this.executed_gtid = gtidSet;
    }

    @Override
    public void updatePurgedGtids(GtidSet gtidSet) {
        this.purged_gtid = gtidSet;
    }

    @Override
    public GtidSet getPurgedGtids() {
        return purged_gtid.clone();
    }

    @Override
    public File getFirstLogNotInGtidSet(GtidSet gtidSet, boolean onlyLocalUuids) {
        return fileManager.getFirstLogNotInGtidSet(gtidSet, onlyLocalUuids);
    }

    @Override
    public boolean addExecutedGtid(String gtid) {
        return executed_gtid.add(gtid);
    }

    @Override
    public void setUuids(Set<String> uuids) {
        this.uuids = uuids;
    }

    @Override
    public Set<String> getUuids() {
        return new HashSet<>(uuids);
    }

    @Override
    protected void doInitialize() throws Exception {
        super.doInitialize();
        LifecycleHelper.initializeIfPossible(fileManager);
        refreshGtidSet();  //restore from file manager
        logger.info("initialize executed_gtid to {}", executed_gtid);
        logger.info("initialize purged_gtid to {}", purged_gtid);
    }

    private void refreshGtidSet() {
        executed_gtid = fileManager.getExecutedGtids();
        purged_gtid = fileManager.getPurgedGtids();
    }

    @Override
    protected void doStart() throws Exception {
        super.doStart();
        LifecycleHelper.startIfPossible(fileManager);

    }

    @Override
    protected void doStop() throws Exception{
        super.doStop();
        LifecycleHelper.stopIfPossible(fileManager);

    }

    @Override
    protected void doDispose() throws Exception {
        super.doDispose();
        LifecycleHelper.disposeIfPossible(fileManager);

    }

    @Override
    public void update(Object args, Observable observable) {
        if (observable instanceof GtidObservable) {
            String gtid = (String) args;
            boolean res = addExecutedGtid(gtid);
            if (logger.isDebugEnabled()) {
                logger.debug("add {} to executedGtid with result {}", gtid, res);
            }
        }
        if (logger.isDebugEnabled()) {
            logger.debug("Executed gtid set is {}", executed_gtid.toString());
        }
    }
}
