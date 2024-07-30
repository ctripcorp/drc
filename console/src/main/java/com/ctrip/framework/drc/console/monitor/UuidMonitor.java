package com.ctrip.framework.drc.console.monitor;


import com.ctrip.framework.drc.console.config.DefaultConsoleConfig;
import com.ctrip.framework.drc.console.dao.entity.MachineTbl;
import com.ctrip.framework.drc.console.monitor.delay.config.DataCenterService;
import com.ctrip.framework.drc.console.monitor.delay.config.MonitorTableSourceProvider;
import com.ctrip.framework.drc.console.pojo.MetaKey;
import com.ctrip.framework.drc.console.service.v2.CentralService;
import com.ctrip.framework.drc.console.task.AbstractAllMySQLEndPointObserver;
import com.ctrip.framework.drc.console.utils.MySqlUtils;
import com.ctrip.framework.drc.core.driver.command.netty.endpoint.MySqlEndpoint;
import com.ctrip.framework.drc.core.monitor.entity.BaseEndpointEntity;
import com.ctrip.framework.drc.core.monitor.reporter.DefaultReporterHolder;
import com.ctrip.framework.drc.core.monitor.reporter.Reporter;
import com.ctrip.framework.drc.core.server.observer.endpoint.MasterMySQLEndpointObserver;
import com.ctrip.framework.drc.core.server.observer.endpoint.SlaveMySQLEndpointObserver;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.DependsOn;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static com.ctrip.framework.drc.console.monitor.delay.config.MonitorTableSourceProvider.SWITCH_STATUS_ON;


/**
 * @ClassName UuidMonitor
 * @Author haodongPan
 * @Date 2021/7/21 16:34
 * @Version: 1$
 */
@Order(2)
@Component
@DependsOn("dbClusterSourceProvider")
public class UuidMonitor extends AbstractAllMySQLEndPointObserver implements MasterMySQLEndpointObserver, SlaveMySQLEndpointObserver {
    
    private Reporter reporter = DefaultReporterHolder.getInstance();

    @Autowired
    private DataCenterService dataCenterService;

    @Autowired
    private DefaultCurrentMetaManager currentMetaManager;

    @Autowired
    private DefaultConsoleConfig consoleConfig;

    @Autowired
    private MonitorTableSourceProvider monitorTableSourceProvider;
    
    @Autowired
    private CentralService centralService;
    
    private static final String UUID_ERROR_NUM_MEASUREMENT = "fx.drc.uuid.errorNums";

    private Map<Endpoint, BaseEndpointEntity> entityMap = Maps.newConcurrentMap();

    public  final int INITIAL_DELAY = 30;

    public  final int PERIOD = 300;

    public  final TimeUnit TIME_UNIT = TimeUnit.SECONDS;

    @Override
    public void initialize() {
        setInitialDelay(INITIAL_DELAY);
        setPeriod(PERIOD);
        setTimeUnit(TIME_UNIT);
        super.initialize();
        currentMetaManager.addObserver(this);
    }

    @Override
    public void scheduledTask() {
        if (isRegionLeader) {
            logger.info("[[monitor=UUIDMonitor]] is a leader,going to monitor");
            String uuidMonitorSwitch = monitorTableSourceProvider.getUuidMonitorSwitch();
            if (!SWITCH_STATUS_ON.equalsIgnoreCase(uuidMonitorSwitch)) {
                logger.info("[[monitor=UUIDMonitor]] uuidMonitor switch close");
                return;
            }
            for (Map.Entry<MetaKey, MySqlEndpoint> entry : masterMySQLEndpointMap.entrySet()) {
                monitorUuid(entry,true);
            }
            for (Map.Entry<MetaKey, MySqlEndpoint> entry : slaveMySQLEndpointMap.entrySet()) {
                monitorUuid(entry,false);
            }
        } else {
            reporter.removeRegister(UUID_ERROR_NUM_MEASUREMENT);
            logger.info("[[monitor=UUIDMonitor]] not leader,remove monitor");
        }
        
    }

    private void monitorUuid(Map.Entry<MetaKey, MySqlEndpoint> entry,boolean isMaster) {
        MetaKey metaKey = entry.getKey();
        MySqlEndpoint mySqlEndpoint = entry.getValue();
        String ip = mySqlEndpoint.getIp();
        int port = mySqlEndpoint.getPort();
        BaseEndpointEntity entity = getEntity(mySqlEndpoint, metaKey);
        Map<String, String> entityTags = entity.getTags();
        
        try {
            String uuidFromCommand = getUUIDFromCommand(mySqlEndpoint,isMaster);
            String uuidStringFromDB = getUUIDFromMetaDB(metaKey.getMhaName(),ip,port);
            if (uuidFromCommand == null || uuidStringFromDB == null) {
                logger.info("[[monitor=UUIDMonitor]] No such Db:" + ip + ":" + port);
                return;
            }
            List<String> uuidsFromMetaDB = StringUtils.isBlank(uuidStringFromDB)? Lists.newArrayList() : Lists.newArrayList(uuidStringFromDB.split(","));
            boolean uuidCorrect = uuidsFromMetaDB.contains(uuidFromCommand);
            reporter.resetReportCounter(entityTags, uuidCorrect? 0L : 1L, UUID_ERROR_NUM_MEASUREMENT);
            if (!uuidCorrect) {
                logger.info("[[monitor=UUIDMonitor]] mysql {}:{},RealUUID:{},ErrorUUID:{}", ip,port,uuidFromCommand, uuidStringFromDB);
                if (SWITCH_STATUS_ON.equalsIgnoreCase(monitorTableSourceProvider.getUuidCorrectSwitch())) {
                    autoCorrect(ip, port, uuidsFromMetaDB, uuidFromCommand);
                }
            }
        } catch (Exception e) {
            logger.error("[[monitor=UUIDMonitor]] monitorUuid error", e);
        }
        
    }
    
    private String getUUIDFromMetaDB(String mha,String ip, int port) throws SQLException {
        return centralService.getUuidInMetaDb(mha, ip, port);
    }

    private String getUUIDFromCommand(Endpoint endpoint, boolean master) throws SQLException {
        return MySqlUtils.getUuid(endpoint, master);
    }
    
    private void autoCorrect(String ip, int port, List<String> uuidsFromMetaDB,String uuidFromCommand) {
        try {
            Set<String> publicCloudRegion = consoleConfig.getPublicCloudRegion();
            MachineTbl correctMachine = new MachineTbl();
            correctMachine.setIp(ip);
            correctMachine.setPort(port);
            if (publicCloudRegion.contains(regionName)) {
                uuidsFromMetaDB.add(uuidFromCommand);
                correctMachine.setUuid(StringUtils.join(uuidsFromMetaDB, ","));
            } else {
                correctMachine.setUuid(uuidFromCommand);
            }
            // update
            Integer affectRow = centralService.correctMachineUuid(correctMachine);
            logger.info("[[monitor=UUIDMonitor]] update Machine {}:{} UUID change from {} to {},affectRows:{}",
                    ip, port, uuidsFromMetaDB, uuidFromCommand,affectRow);
        } catch (SQLException e) {
            logger.error("[[monitor=UUIDMonitor]] SQLException occur in update errorUUID", e);
        }
    }
    
    protected BaseEndpointEntity getEntity(Endpoint endpoint, MetaKey metaKey) {
        BaseEndpointEntity entity;
        if (null == (entity = entityMap.get(endpoint))) {
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
        reporter.removeRegister(getEntity(endpoint,metaKey).getTags(),UUID_ERROR_NUM_MEASUREMENT);
    }

    @Override
    public void setLocalDcName() {
        localDcName = dataCenterService.getDc();
    }
    
    @Override
    public void setLocalRegionInfo() {
        this.regionName = consoleConfig.getRegion();
        this.dcsInRegion = consoleConfig.getDcsInLocalRegion();
    }

    @Override
    public void setOnlyCarePart() {
        this.onlyCarePart = true;
    }

    @Override
    public boolean isCare(MetaKey metaKey) {
        return this.dcsInRegion.contains(metaKey.getDc());
    }
    
}
