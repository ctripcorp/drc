package com.ctrip.framework.drc.console.monitor;

import com.ctrip.framework.drc.console.enums.ActionEnum;
import com.ctrip.framework.drc.console.monitor.delay.config.DbClusterSourceProvider;
import com.ctrip.framework.drc.console.monitor.delay.config.MonitorTableSourceProvider;
import com.ctrip.framework.drc.console.monitor.delay.impl.execution.GeneralSingleExecution;
import com.ctrip.framework.drc.console.monitor.delay.impl.operator.WriteSqlOperatorWrapper;
import com.ctrip.framework.drc.console.pojo.MetaKey;
import com.ctrip.framework.drc.core.driver.command.netty.endpoint.MySqlEndpoint;
import com.ctrip.framework.drc.core.monitor.entity.BaseEndpointEntity;
import com.ctrip.framework.drc.core.monitor.operator.ReadResource;
import com.ctrip.framework.drc.core.monitor.reporter.DefaultReporterHolder;
import com.ctrip.framework.drc.core.monitor.reporter.Reporter;
import com.ctrip.framework.drc.core.server.observer.endpoint.MasterMySQLEndpointObservable;
import com.ctrip.framework.drc.core.server.observer.endpoint.MasterMySQLEndpointObserver;
import com.ctrip.framework.drc.core.server.observer.endpoint.SlaveMySQLEndpointObservable;
import com.ctrip.framework.drc.core.server.observer.endpoint.SlaveMySQLEndpointObserver;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.api.observer.Observable;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.DependsOn;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import org.unidal.tuple.Triple;

import java.sql.ResultSet;
import java.util.Map;

import static com.ctrip.framework.drc.console.enums.LogTypeEnum.ERROR;
import static com.ctrip.framework.drc.console.enums.LogTypeEnum.INFO;
import static com.ctrip.framework.drc.console.monitor.delay.config.MonitorTableSourceProvider.SWITCH_STATUS_ON;
import static com.ctrip.framework.drc.core.monitor.enums.MeasurementEnum.BINLOG_TRANSACTION_DEPENDENCY_HISTORY_SIZE_MEASUREMENT;
import static com.ctrip.framework.drc.core.server.config.SystemConfig.CONSOLE_MYSQL_LOG;
import static com.ctrip.framework.drc.core.server.config.SystemConfig.CONSOLE_MYSQL_LOGGER;

/**
 * @Author: hbshen
 * @Date: 2021/4/26
 */
@Order(2)
@Component
@DependsOn("dbClusterSourceProvider")
public class BtdhsMonitor extends AbstractMonitor implements MasterMySQLEndpointObserver, SlaveMySQLEndpointObserver {

    public Logger logger = LoggerFactory.getLogger(CONSOLE_MYSQL_LOG);

    private Reporter reporter = DefaultReporterHolder.getInstance();

    @Autowired
    private DbClusterSourceProvider sourceProvider;

    @Autowired
    private DefaultCurrentMetaManager currentMetaManager;

    @Autowired
    private MonitorTableSourceProvider monitorTableSourceProvider;

    private static final String BINLOG_TRANSACTION_DEPENDENCY_HISTORY_SIZE = "show global variables like \"binlog_transaction_dependency_history_size\";";

    private static final int BINLOG_TRANSACTION_DEPENDENCY_HISTORY_SIZE_INDEX = 2;

    private Map<Endpoint, BaseEndpointEntity> entityMap = Maps.newConcurrentMap();

    protected Map<MetaKey, MySqlEndpoint> masterMySQLEndpointMap = Maps.newConcurrentMap();

    protected Map<MetaKey, MySqlEndpoint> slaveMySQLEndpointMap = Maps.newConcurrentMap();

    private String localDcName;

    @Override
    public void initialize() {
        super.initialize();
        localDcName = sourceProvider.getLocalDcName();
        currentMetaManager.addObserver(this);
    }

    @Override
    public void scheduledTask() {
        String btdhsMonitorSwitch = monitorTableSourceProvider.getBtdhsMonitorSwitch();
        if(!SWITCH_STATUS_ON.equalsIgnoreCase(btdhsMonitorSwitch)) {
            return;
        }

        for (Map.Entry<MetaKey, MySqlEndpoint> entry : masterMySQLEndpointMap.entrySet()) {
            monitorBtdhs(entry);
        }
        for (Map.Entry<MetaKey, MySqlEndpoint> entry : slaveMySQLEndpointMap.entrySet()) {
            monitorBtdhs(entry);
        }
    }

    private void monitorBtdhs(Map.Entry<MetaKey, MySqlEndpoint> entry) {
        MetaKey metaKey = entry.getKey();
        MySqlEndpoint mySqlEndpoint = entry.getValue();
        WriteSqlOperatorWrapper sqlOperatorWrapper = getSqlOperatorWrapper(mySqlEndpoint);
        BaseEndpointEntity entity = getEntity(mySqlEndpoint, metaKey);
        Map<String, String> entityTags = entity.getTags();
        try {
            long binlogTxDependencyHistSize = getBinlogTxDependencyHistSize(sqlOperatorWrapper);
            reporter.reportResetCounter(entityTags, binlogTxDependencyHistSize, BINLOG_TRANSACTION_DEPENDENCY_HISTORY_SIZE_MEASUREMENT);
            cLog(entityTags, "BTDHS="+binlogTxDependencyHistSize, INFO, null);
        } catch (Throwable t) {
            cLog(entityTags, "Fail to get binlog_transaction_dependency_history_size", ERROR, t);
            removeSqlOperator(mySqlEndpoint);
        }
    }

    protected long getBinlogTxDependencyHistSize(WriteSqlOperatorWrapper sqlOperatorWrapper) throws Throwable {
        ReadResource readResource = null;
        try {
            GeneralSingleExecution execution = new GeneralSingleExecution(BINLOG_TRANSACTION_DEPENDENCY_HISTORY_SIZE);
            readResource = sqlOperatorWrapper.select(execution);
            ResultSet rs = readResource.getResultSet();
            if(rs == null) {
                return -1;
            }
            rs.next();
            return rs.getLong(BINLOG_TRANSACTION_DEPENDENCY_HISTORY_SIZE_INDEX);
        } finally {
            if(readResource != null) {
                readResource.close();
            }
        }
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
    public void update(Object args, Observable observable) {
        if (observable instanceof MasterMySQLEndpointObservable) {
            updateMySQLEndpointMap((Triple<MetaKey, MySqlEndpoint, ActionEnum>) args, masterMySQLEndpointMap);
        } else if (observable instanceof SlaveMySQLEndpointObservable) {
            updateMySQLEndpointMap((Triple<MetaKey, MySqlEndpoint, ActionEnum>) args, slaveMySQLEndpointMap);
        }
    }

    private void updateMySQLEndpointMap(Triple<MetaKey, MySqlEndpoint, ActionEnum> msg, Map<MetaKey, MySqlEndpoint> mySQLEndpointMap) {

        MetaKey metaKey = msg.getFirst();
        MySqlEndpoint mySQLEndpoint = msg.getMiddle();
        ActionEnum action = msg.getLast();

        if(!metaKey.getDc().equalsIgnoreCase(localDcName)) {
            CONSOLE_MYSQL_LOGGER.warn("[OBSERVE][{}] {} not interested in {}({})", getClass().getName(), localDcName, metaKey, mySQLEndpoint.getSocketAddress());
            return;
        }

        if(ActionEnum.ADD.equals(action) || ActionEnum.UPDATE.equals(action)) {
            CONSOLE_MYSQL_LOGGER.info("[OBSERVE][{}] {} {}({})", getClass().getName(), action.name(), metaKey, mySQLEndpoint.getSocketAddress());
            MySqlEndpoint oldEndpoint = mySQLEndpointMap.get(metaKey);
            if (oldEndpoint != null) {
                CONSOLE_MYSQL_LOGGER.info("[OBSERVE][{}] {} clear old {}({})", getClass().getName(), action.name(), metaKey, oldEndpoint.getSocketAddress());
                removeSqlOperator(oldEndpoint);
                reporter.removeRegister(getEntity(oldEndpoint,metaKey).getTags(),BINLOG_TRANSACTION_DEPENDENCY_HISTORY_SIZE_MEASUREMENT.getMeasurement());
            }
            mySQLEndpointMap.put(metaKey, mySQLEndpoint);
        } else if (ActionEnum.DELETE.equals(action)) {
            CONSOLE_MYSQL_LOGGER.info("[OBSERVE][{}] {} {}", getClass().getName(), action.name(), metaKey);
            MySqlEndpoint oldEndpoint = mySQLEndpointMap.remove(metaKey);
            if (oldEndpoint != null) {
                CONSOLE_MYSQL_LOGGER.info("[OBSERVE][{}] {} clear old {}({})", getClass().getName(), action.name(), metaKey, oldEndpoint.getSocketAddress());
                removeSqlOperator(oldEndpoint);
                reporter.removeRegister(getEntity(oldEndpoint,metaKey).getTags(),BINLOG_TRANSACTION_DEPENDENCY_HISTORY_SIZE_MEASUREMENT.getMeasurement());
            }
        }
    }
}
