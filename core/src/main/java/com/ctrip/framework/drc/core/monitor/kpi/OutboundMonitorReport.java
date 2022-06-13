package com.ctrip.framework.drc.core.monitor.kpi;

import com.ctrip.framework.drc.core.driver.schema.data.TableKey;
import com.ctrip.framework.drc.core.monitor.entity.RowsFilterEntity;
import com.ctrip.framework.drc.core.monitor.entity.TrafficEntity;
import com.ctrip.framework.drc.core.monitor.reporter.DefaultEventMonitorHolder;
import com.google.common.collect.Maps;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @Author limingdong
 * @create 2020/3/14
 */
public class OutboundMonitorReport extends AbstractMonitorReport {

    private static final String OUTBOUND_GTID = "DRC.replicator.outbound.gtid";

    private Map<String, AtomicLong> outboundGtid = Maps.newConcurrentMap();

    private Map<TableKey, RowsFilterEntity> rowsFilterEntityMap = Maps.newConcurrentMap();

    public OutboundMonitorReport(long domain, TrafficEntity trafficEntity) {
        super(domain, trafficEntity);
    }

    @Override
    protected void doMonitor() {

        for (Map.Entry<String, AtomicLong> entry : outboundGtid.entrySet()) {
            AtomicLong atomicLong = entry.getValue();
            DefaultEventMonitorHolder.getInstance().logEvent(OUTBOUND_GTID, entry.getKey(), atomicLong.getAndSet(0));
        }

        for (Map.Entry<TableKey, RowsFilterEntity> entry : rowsFilterEntityMap.entrySet()) {
            RowsFilterEntity rowsFilterEntity = entry.getValue();
            hickwallReporter.reportRowsFilter(rowsFilterEntity);
            rowsFilterEntity.clearCount();
        }
    }

    public void addOutboundGtid(String applierName, String gtid) {
        AtomicLong atomicLong = outboundGtid.get(applierName);
        if(atomicLong == null) {
            atomicLong = new AtomicLong(0);
            outboundGtid.put(applierName, atomicLong);
        }
        atomicLong.addAndGet(1);
        delayMonitorReport.sendGtid(gtid);
    }

    public void updateFilteredRows(String dbName, String tableName, int beforeSize, int afterSize) {
        RowsFilterEntity rowsFilterEntity = getRowsFilterEntity(dbName, tableName);
        rowsFilterEntity.updateCount(beforeSize, afterSize);
    }

    private RowsFilterEntity getRowsFilterEntity(String dbName, String tableName) {
        TableKey tableKey = TableKey.from(dbName, tableName);
        RowsFilterEntity rowsFilterEntity = rowsFilterEntityMap.get(tableKey);
        if (rowsFilterEntity == null) {
            rowsFilterEntity = new RowsFilterEntity.Builder()
                    .dcName(this.trafficEntity.getDcName())
                    .buName(this.trafficEntity.getBuName())
                    .mha(this.trafficEntity.getMhaName())
                    .clusterAppId(this.trafficEntity.getClusterAppId())
                    .registryKey(this.trafficEntity.getRegistryKey())
                    .clusterName(this.trafficEntity.getClusterName())
                    .mysqlIp(this.trafficEntity.getIp())
                    .mysqlPort(this.trafficEntity.getPort())
                    .dbName(dbName)
                    .tableName(tableName).build();
            rowsFilterEntityMap.put(tableKey, rowsFilterEntity);
        }
        return rowsFilterEntity;
    }

    public String getClusterName() {
        return trafficEntity.getClusterName();
    }

}
