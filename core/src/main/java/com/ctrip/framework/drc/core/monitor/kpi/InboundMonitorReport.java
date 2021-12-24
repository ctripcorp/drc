package com.ctrip.framework.drc.core.monitor.kpi;

import com.ctrip.framework.drc.core.monitor.entity.TrafficEntity;
import com.ctrip.framework.drc.core.monitor.reporter.DefaultEventMonitorHolder;
import com.google.common.collect.Maps;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import static com.ctrip.framework.drc.core.server.config.SystemConfig.DRC_MONITOR_SCHEMA_NAME;

/**
 * @Author limingdong
 * @create 2020/3/13
 */
public class InboundMonitorReport extends AbstractMonitorReport {

    private static final String INBOUND_GTID = "DRC.replicator.inbound.gtid";

    private static final String INBOUND_DB = "DRC.replicator.inbound.db";

    private static final String INBOUND_TABLE = "DRC.replicator.inbound.table";

    private static final String GTID_FILTER = "DRC.replicator.inbound.gtid.filter";

    private static final String GTID_STORE = "DRC.replicator.inbound.gtid.store";

    private static final String DB_FILTER = "DRC.replicator.inbound.db.filter";

    private static final String GHOST_DB_FILTER = "DRC.replicator.inbound.db.ghost";

    private static final String XID_FAKE = "DRC.replicator.inbound.xid.fake";

    private static final String INBOUND_XID = "DRC.replicator.inbound.xid";

    //Cat
    private AtomicLong inboundGtid = new AtomicLong(0);

    private AtomicLong gtidStore = new AtomicLong(0);

    private AtomicLong gtidFilter = new AtomicLong(0);

    private AtomicLong xidFake = new AtomicLong(0);

    private AtomicLong inboundXid = new AtomicLong(0);

    private Map<String, AtomicLong> dbCount = Maps.newConcurrentMap();

    private Map<String, AtomicLong> tableCount = Maps.newConcurrentMap();

    private Map<String, AtomicLong> dbFilter = Maps.newConcurrentMap();

    private Map<String, AtomicLong> ghostDbFilter = Maps.newConcurrentMap();

    public InboundMonitorReport(long domain, TrafficEntity trafficEntity) {
        super(domain, trafficEntity);
    }

    @Override
    protected void doMonitor() {
        //Cat
        for (Map.Entry<String, AtomicLong> entry : dbCount.entrySet()) {
            AtomicLong atomicLong = entry.getValue();
            DefaultEventMonitorHolder.getInstance().logEvent(INBOUND_DB, entry.getKey(), atomicLong.getAndSet(0));
        }

        for (Map.Entry<String, AtomicLong> entry : tableCount.entrySet()) {
            AtomicLong atomicLong = entry.getValue();
            DefaultEventMonitorHolder.getInstance().logEvent(INBOUND_TABLE, entry.getKey(), atomicLong.getAndSet(0));
        }


        for (Map.Entry<String, AtomicLong> entry : dbFilter.entrySet()) {
            AtomicLong atomicLong = entry.getValue();
            DefaultEventMonitorHolder.getInstance().logEvent(DB_FILTER, entry.getKey(), atomicLong.getAndSet(0));
        }

        for (Map.Entry<String, AtomicLong> entry : ghostDbFilter.entrySet()) {
            AtomicLong atomicLong = entry.getValue();
            DefaultEventMonitorHolder.getInstance().logEvent(GHOST_DB_FILTER, entry.getKey(), atomicLong.getAndSet(0));
        }

        DefaultEventMonitorHolder.getInstance().logEvent(INBOUND_GTID, clusterName, inboundGtid.getAndSet(0));
        DefaultEventMonitorHolder.getInstance().logEvent(GTID_FILTER, clusterName, gtidFilter.getAndSet(0));
        DefaultEventMonitorHolder.getInstance().logEvent(GTID_STORE, clusterName, gtidStore.getAndSet(0));
        DefaultEventMonitorHolder.getInstance().logEvent(INBOUND_XID, clusterName, inboundXid.getAndSet(0));
        DefaultEventMonitorHolder.getInstance().logEvent(XID_FAKE, clusterName, xidFake.getAndSet(0));
    }

    public void addInboundGtid(long ig) {
        inboundGtid.addAndGet(ig);
    }

    public void addInboundMonitorGtid(String gtid) {
        delayMonitorReport.receiveMonitorGtid(gtid);
    }

    public void addGtidFilter(long ig) {
        gtidFilter.addAndGet(ig);
    }

    public void addXidFake(long ig) {
        xidFake.addAndGet(ig);
    }

    public void addGtidStore(long ig, String gtid) {
        gtidStore.addAndGet(ig);
        delayMonitorReport.receiveGtid(gtid);
    }

    public void addInboundXid(long ig) {
        inboundXid.addAndGet(ig);
    }

    public void addDb(String db, String gtid) {
        AtomicLong atomicLong = dbCount.get(db);
        if(atomicLong == null) {
            atomicLong = new AtomicLong(0);
            dbCount.put(db, atomicLong);
        }
        atomicLong.addAndGet(1);
        if (DRC_MONITOR_SCHEMA_NAME.equalsIgnoreCase(db)) {
            addInboundMonitorGtid(gtid);
        }
    }

    public void addTable(String table) {
        AtomicLong atomicLong = tableCount.get(table);
        if(atomicLong == null) {
            atomicLong = new AtomicLong(0);
            tableCount.put(table, atomicLong);
        }
        atomicLong.addAndGet(1);
    }

    public void addDbFilter(String db) {
        AtomicLong atomicLong = dbFilter.get(db);
        if(atomicLong == null) {
            atomicLong = new AtomicLong(0);
            dbFilter.put(db, atomicLong);
        }
        atomicLong.addAndGet(1);
    }

    public void addGhostDbFilter(String db) {
        AtomicLong atomicLong = ghostDbFilter.get(db);
        if(atomicLong == null) {
            atomicLong = new AtomicLong(0);
            ghostDbFilter.put(db, atomicLong);
        }
        atomicLong.addAndGet(1);
    }

    public String getClusterName() {
        return trafficEntity.getClusterName();
    }

}
