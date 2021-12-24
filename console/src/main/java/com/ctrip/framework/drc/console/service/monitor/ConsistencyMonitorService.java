package com.ctrip.framework.drc.console.service.monitor;

import com.ctrip.framework.drc.console.dao.entity.DataConsistencyMonitorTbl;
import com.ctrip.framework.drc.console.dao.entity.DataInconsistencyHistoryTbl;
import com.ctrip.framework.drc.console.monitor.delay.config.ConsistencyMonitorConfig;
import com.ctrip.framework.drc.console.monitor.delay.config.DelayMonitorConfig;
import com.ctrip.framework.drc.console.monitor.delay.config.FullDataConsistencyCheckTestConfig;
import com.ctrip.framework.drc.console.monitor.delay.config.FullDataConsistencyMonitorConfig;
import com.ctrip.framework.drc.console.vo.MhaGroupPair;
import org.springframework.web.bind.annotation.PathVariable;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created by jixinwang on 2020/12/29
 */
public interface ConsistencyMonitorService {

    void addDataConsistencyMonitor(String mhaA, String mhaB, DelayMonitorConfig delayMonitorConfig) throws SQLException;

    List<DataConsistencyMonitorTbl> getDataConsistencyMonitor(String mhaA, String mhaB) throws SQLException;

    void deleteDataConsistencyMonitor(int id) throws SQLException;

    Map<String, Boolean> switchDataConsistencyMonitor(List<ConsistencyMonitorConfig> consistencyMonitorConfigs, String status);

    boolean switchUnitVerification(MhaGroupPair mhaGroupPair, String status);

    void addFullDataConsistencyMonitor(FullDataConsistencyMonitorConfig fullDataConsistencyMonitorConfig);

    Map<String, Object> getCurrentFullInconsistencyRecord(String mhaA, String mhaB, String dbName, String tableName, String key, String checkTime) throws SQLException;

    Map<String, Object> getCurrentFullInconsistencyRecordForTest(FullDataConsistencyCheckTestConfig testConfig, String checkTime) throws SQLException;

    List<DataInconsistencyHistoryTbl> getIncrementInconsistencyHistory(int pageNo, int pageSize, String dbName, String tableName, String startTime, String endTime) throws SQLException;

    long getIncrementInconsistencyHistoryCount(String dbName, String tableName, String startTime, String endTime) throws SQLException;

    Map<String, Object> getCurrentIncrementInconsistencyRecord(long mhaGroupId, String dbName, String tableName, String key, String keyValue) throws SQLException;

    void handleInconsistency(Map<String, String> updateInfo) throws SQLException;

    int[] deleteDataInconsistency(Set<Long> idSet);

    boolean addUnitVerification(Long mhaGroupId);

    boolean deleteUnitVerification(Long mhaGroupId);

    //ForInternalTest
    void addFullDataConsistencyCheck(FullDataConsistencyCheckTestConfig fullDataConsistencyCheckTestConfig);

    Map<String, Object> getFullDataCheckStatusForTest(String schema, String table, String key) throws SQLException;
}
