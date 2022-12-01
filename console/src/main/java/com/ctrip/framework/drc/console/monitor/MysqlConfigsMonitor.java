package com.ctrip.framework.drc.console.monitor;

import com.ctrip.framework.drc.console.config.DefaultConsoleConfig;
import com.ctrip.framework.drc.console.monitor.delay.config.DbClusterSourceProvider;
import com.ctrip.framework.drc.console.monitor.delay.config.MonitorTableSourceProvider;
import com.ctrip.framework.drc.console.monitor.delay.impl.execution.GeneralSingleExecution;
import com.ctrip.framework.drc.console.monitor.delay.impl.operator.WriteSqlOperatorWrapper;
import com.ctrip.framework.drc.console.monitor.task.AliBinlogRetentionTimeQueryTask;
import com.ctrip.framework.drc.console.monitor.task.AwsBinlogRetentionTimeQueryTask;
import com.ctrip.framework.drc.console.monitor.task.BtdhsQueryTask;
import com.ctrip.framework.drc.console.pojo.MetaKey;
import com.ctrip.framework.drc.console.task.AbstractAllMySQLEndPointObserver;
import com.ctrip.framework.drc.core.driver.binlog.manager.task.NamedCallable;
import com.ctrip.framework.drc.core.driver.binlog.manager.task.RetryTask;
import com.ctrip.framework.drc.core.driver.command.netty.endpoint.MySqlEndpoint;
import com.ctrip.framework.drc.core.monitor.entity.BaseEndpointEntity;
import com.ctrip.framework.drc.core.monitor.operator.ReadResource;
import com.ctrip.framework.drc.core.monitor.reporter.DefaultReporterHolder;
import com.ctrip.framework.drc.core.monitor.reporter.Reporter;
import com.ctrip.framework.drc.core.server.observer.endpoint.MasterMySQLEndpointObserver;
import com.ctrip.framework.drc.core.server.observer.endpoint.SlaveMySQLEndpointObserver;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.DependsOn;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

import static com.ctrip.framework.drc.console.enums.LogTypeEnum.*;
import static com.ctrip.framework.drc.console.monitor.delay.config.MonitorTableSourceProvider.SWITCH_STATUS_ON;
import static com.ctrip.framework.drc.core.monitor.enums.MeasurementEnum.BINLOG_TRANSACTION_DEPENDENCY_HISTORY_SIZE_MEASUREMENT;
import static com.ctrip.framework.drc.core.server.config.SystemConfig.CONSOLE_MYSQL_LOG;

/**
 * @Author: hbshen
 * @Date: 2021/4/26
 */
@Order(2)
@Component
@DependsOn("dbClusterSourceProvider")
public class MysqlConfigsMonitor extends AbstractAllMySQLEndPointObserver implements MasterMySQLEndpointObserver, SlaveMySQLEndpointObserver {

    public Logger logger = LoggerFactory.getLogger(CONSOLE_MYSQL_LOG);

    private Reporter reporter = DefaultReporterHolder.getInstance();

    @Autowired
    private DbClusterSourceProvider sourceProvider;

    @Autowired
    private DefaultCurrentMetaManager currentMetaManager;

    @Autowired
    private MonitorTableSourceProvider monitorTableSourceProvider;
    
    @Autowired
    private DefaultConsoleConfig consoleConfig;

    public static final String BINLOG_RETENTION_TIME_MEASUREMENT = "fx.drc.binlog.retention.time";

    private Map<Endpoint, BaseEndpointEntity> entityMap = Maps.newConcurrentMap();
    
    
    @Override
    public void initialize() {
        super.initialize();
        currentMetaManager.addObserver(this);
    }

    @Override
    public void scheduledTask() {
        if (isRegionLeader) {
            logger.info("[[monitor=mysqlConfigs]] is a leader,going to monitor");
            String mysqlConfigsMonitorSwitch = monitorTableSourceProvider.getMysqlConfigsMonitorSwitch();
            if(!SWITCH_STATUS_ON.equalsIgnoreCase(mysqlConfigsMonitorSwitch)) {
                logger.info("[[monitor=mysqlConfigs]]  switch close");
                return;
            }
            for (Map.Entry<MetaKey, MySqlEndpoint> entry : masterMySQLEndpointMap.entrySet()) {
                monitorBtdhs(entry);
            }
            for (Map.Entry<MetaKey, MySqlEndpoint> entry : slaveMySQLEndpointMap.entrySet()) {
                monitorBtdhs(entry);
            }
            if (consoleConfig.getPublicCloudRegion().contains(regionName)) {
                for (Map.Entry<MetaKey, MySqlEndpoint> entry : masterMySQLEndpointMap.entrySet()) {
                    monitorBinlogRetentionTime(entry);
                }
            }
            
        } else {
            reporter.removeRegister(BINLOG_TRANSACTION_DEPENDENCY_HISTORY_SIZE_MEASUREMENT.getMeasurement());
            reporter.removeRegister(BINLOG_RETENTION_TIME_MEASUREMENT);
            logger.info("[[monitor=mysqlConfigs]] not leader,remove monitor");
        }
        
    }

    private void monitorBtdhs(Map.Entry<MetaKey, MySqlEndpoint> entry) {
        MetaKey metaKey = entry.getKey();
        MySqlEndpoint mySqlEndpoint = entry.getValue();
        WriteSqlOperatorWrapper sqlOperatorWrapper = getSqlOperatorWrapper(mySqlEndpoint);
        BaseEndpointEntity entity = getEntity(mySqlEndpoint, metaKey);
        Map<String, String> entityTags = entity.getTags();
        try {
            Long binlogTxDependencyHistSize = new RetryTask<>(new BtdhsQueryTask(sqlOperatorWrapper),1).call();
            reporter.resetReportCounter(
                    entityTags,
                    binlogTxDependencyHistSize == null ? 0: binlogTxDependencyHistSize, 
                    BINLOG_TRANSACTION_DEPENDENCY_HISTORY_SIZE_MEASUREMENT.getMeasurement()
            );
            cLog(entityTags, "BTDHS="+binlogTxDependencyHistSize, INFO, null);
        } catch (Throwable t) {
            cLog(entityTags, "Fail to get binlog_transaction_dependency_history_size", ERROR, t);
            removeSqlOperator(mySqlEndpoint);
        }
    }
    
    private void monitorBinlogRetentionTime(Map.Entry<MetaKey, MySqlEndpoint> entry) {
        MetaKey metaKey = entry.getKey();
        MySqlEndpoint mySqlEndpoint = entry.getValue();
        WriteSqlOperatorWrapper sqlOperatorWrapper = getSqlOperatorWrapper(mySqlEndpoint);
        BaseEndpointEntity entity = getEntity(mySqlEndpoint, metaKey);
        Map<String, String> entityTags = entity.getTags();
        try {
            Long retentionHours = getBinlogRetentionTime(sqlOperatorWrapper);
            reporter.resetReportCounter(entityTags, 
                    retentionHours == null ? 0: retentionHours,
                    BINLOG_RETENTION_TIME_MEASUREMENT);
            cLog(entityTags,"BINLOG_RETENTION_TIME=" + retentionHours , INFO, null);
        } catch (SQLException e) {
            cLog(entityTags,"BINLOG_RETENTION_TIME query error" , ERROR, e);
            removeSqlOperator(mySqlEndpoint);
        }
    }
    
    
    private Long getBinlogRetentionTime(WriteSqlOperatorWrapper sqlOperatorWrapper) throws SQLException {
        Long res = new RetryTask<>(new AwsBinlogRetentionTimeQueryTask(sqlOperatorWrapper), 1).call();
        if (res  == null) {
            res = new RetryTask<>(new AliBinlogRetentionTimeQueryTask(sqlOperatorWrapper), 1).call();
        } 
        return res;
    }
    

    protected BaseEndpointEntity getEntity(Endpoint endpoint, MetaKey metaKey) {
        BaseEndpointEntity entity;
        if(null == (entity = entityMap.get(endpoint))) {
            entity = new BaseEndpointEntity.Builder()
                    .dcName(metaKey.getDc())
                    .clusterName(metaKey.getClusterName())
                    .mhaName(metaKey.getMhaName())
                    .registryKey(metaKey.getClusterId())
                    .ip(endpoint.getHost())
                    .port(endpoint.getPort())
                    .build();
            entityMap.put(endpoint, entity);
        }
        return entity;
    }

    @Override
    public void clearResource(Endpoint endpoint, MetaKey metaKey) {
        reporter.removeRegister(getEntity(endpoint,metaKey).getTags(),BINLOG_TRANSACTION_DEPENDENCY_HISTORY_SIZE_MEASUREMENT.getMeasurement());
    }

    @Override
    public void setLocalDcName() {
        localDcName = sourceProvider.getLocalDcName();
    }

    @Override
    public void setLocalRegionInfo() {
        regionName = consoleConfig.getRegion();
        dcsInRegion = consoleConfig.getDcsInLocalRegion();
    }

    @Override
    public void setOnlyCarePart() {
        onlyCarePart = true;
    }

    @Override
    public boolean isCare(MetaKey metaKey) {
        return dcsInRegion.contains(metaKey.getDc());
    }
    
}
