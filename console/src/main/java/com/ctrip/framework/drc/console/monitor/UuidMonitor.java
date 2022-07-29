package com.ctrip.framework.drc.console.monitor;


import com.ctrip.framework.drc.console.config.DefaultConsoleConfig;
import com.ctrip.framework.drc.console.dao.MachineTblDao;
import com.ctrip.framework.drc.console.dao.entity.MachineTbl;
import com.ctrip.framework.drc.console.enums.ActionEnum;
import com.ctrip.framework.drc.console.ha.LeaderSwitchable;
import com.ctrip.framework.drc.console.monitor.delay.config.DbClusterSourceProvider;
import com.ctrip.framework.drc.console.monitor.delay.config.MonitorTableSourceProvider;
import com.ctrip.framework.drc.console.monitor.delay.impl.execution.GeneralSingleExecution;
import com.ctrip.framework.drc.console.monitor.delay.impl.operator.WriteSqlOperatorWrapper;
import com.ctrip.framework.drc.console.pojo.MetaKey;
import com.ctrip.framework.drc.console.service.impl.openapi.OpenService;
import com.ctrip.framework.drc.console.task.AbstractAllMySQLEndPointObserver;
import com.ctrip.framework.drc.core.service.utils.Constants;
import com.ctrip.framework.drc.console.utils.DalUtils;
import com.ctrip.framework.drc.console.vo.response.AbstractResponse;
import com.ctrip.framework.drc.console.vo.response.UuidResponseVo;
import com.ctrip.framework.drc.core.driver.command.netty.endpoint.MySqlEndpoint;
import com.ctrip.framework.drc.core.monitor.entity.BaseEndpointEntity;
import com.ctrip.framework.drc.core.monitor.operator.ReadResource;
import com.ctrip.framework.drc.core.monitor.reporter.DefaultReporterHolder;
import com.ctrip.framework.drc.core.monitor.reporter.Reporter;
import com.ctrip.framework.drc.core.server.observer.endpoint.MasterMySQLEndpointObservable;
import com.ctrip.framework.drc.core.server.observer.endpoint.MasterMySQLEndpointObserver;
import com.ctrip.framework.drc.core.server.observer.endpoint.SlaveMySQLEndpointObservable;
import com.ctrip.framework.drc.core.server.observer.endpoint.SlaveMySQLEndpointObserver;
import com.ctrip.xpipe.api.cluster.LeaderAware;
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
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static com.ctrip.framework.drc.console.monitor.delay.config.MonitorTableSourceProvider.SWITCH_STATUS_ON;
import static com.ctrip.framework.drc.core.server.config.SystemConfig.CONSOLE_MYSQL_LOGGER;


/**
 * @ClassName UuidMonitor
 * @Author haodongPan
 * @Date 2021/7/21 16:34
 * @Version: 1$
 */
@Order(2)
@Component
@DependsOn("dbClusterSourceProvider")
public class UuidMonitor extends AbstractAllMySQLEndPointObserver implements MasterMySQLEndpointObserver, SlaveMySQLEndpointObserver, LeaderSwitchable {

    public static final Logger uuidLogger = LoggerFactory.getLogger(UuidMonitor.class);

    private Reporter reporter = DefaultReporterHolder.getInstance();

    @Autowired
    private DbClusterSourceProvider sourceProvider;

    @Autowired
    private DefaultCurrentMetaManager currentMetaManager;

    @Autowired
    private DefaultConsoleConfig consoleConfig;

    @Autowired
    private MonitorTableSourceProvider monitorTableSourceProvider;
    
    @Autowired
    private OpenService openService;

    private MachineTblDao machineTblDao = DalUtils.getInstance().getMachineTblDao();
    
    private static final String ALI_DC = "shali";
    private static final String AWS_DC = "fraaws";
    private static final String UUID_ERROR_NUM_MEASUREMENT = "fx.drc.uuid.errorNums";
    protected static final String ALI_RDS = "/*FORCE_MASTER*/";
    private static final String UUIDCommand = "show  global  variables  like \"server_uuid\";";
    private static final int UUID_INDEX = 2;

    private Map<Endpoint, BaseEndpointEntity> entityMap = Maps.newConcurrentMap();

<<<<<<< Updated upstream
=======
    protected Map<MetaKey, MySqlEndpoint> masterMySQLEndpointMap = Maps.newConcurrentMap();

    protected Map<MetaKey, MySqlEndpoint> slaveMySQLEndpointMap = Maps.newConcurrentMap();

>>>>>>> Stashed changes
    private volatile boolean isRegionLeader = false;

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
<<<<<<< Updated upstream
    
=======

    

>>>>>>> Stashed changes
    @Override
    public void scheduledTask() {
        if (isRegionLeader) {
            uuidLogger.info("[[monitor=UUIDMonitor]] is a leader,going to monitor");
            String uuidMonitorSwitch = monitorTableSourceProvider.getUuidMonitorSwitch();
            if (!SWITCH_STATUS_ON.equalsIgnoreCase(uuidMonitorSwitch)) {
                uuidLogger.info("[[monitor=UUIDMonitor]] uuidMonitor switch close");
                return;
            }
            for (Map.Entry<MetaKey, MySqlEndpoint> entry : masterMySQLEndpointMap.entrySet()) {
                monitorUuid(entry);
            }
            for (Map.Entry<MetaKey, MySqlEndpoint> entry : slaveMySQLEndpointMap.entrySet()) {
                monitorUuid(entry);
            }
        } else {
            reporter.removeRegister(UUID_ERROR_NUM_MEASUREMENT);
            uuidLogger.info("[[monitor=UUIDMonitor]] not leader,remove monitor");
        }
        
    }

    private void monitorUuid(Map.Entry<MetaKey, MySqlEndpoint> entry) {
        MetaKey metaKey = entry.getKey();
        MySqlEndpoint mySqlEndpoint = entry.getValue();
        String ip = mySqlEndpoint.getIp();
        int port = mySqlEndpoint.getPort();
        
        WriteSqlOperatorWrapper sqlOperatorWrapper = getSqlOperatorWrapper(mySqlEndpoint);
        BaseEndpointEntity entity = getEntity(mySqlEndpoint, metaKey);
        Map<String, String> entityTags = entity.getTags();
        String uuidFromCommand = null;
        List<String> uuidFromDB = null;
        String uuidStringFromDB = null;
        try {
            uuidFromCommand = getUUIDFromCommand(sqlOperatorWrapper);
            uuidStringFromDB = getUUIDFromDB(ip,port);
            if(uuidStringFromDB != null) {
                uuidFromDB = Arrays.asList(uuidStringFromDB.split(","));
            }
        } catch (SQLException e) {
            uuidLogger.error("[[monitor=UUIDMonitor]] SQLException occur in getUUID", e);
        }
        if (uuidFromCommand == null || uuidFromDB == null) {
            uuidLogger.info("[[monitor=UUIDMonitor]] No such Db:" + ip + ":" + port);
        } else {
            if (uuidFromDB.contains(uuidFromCommand)) {
                reporter.resetReportCounter(entityTags, 0L, UUID_ERROR_NUM_MEASUREMENT);
                uuidLogger.info("[[monitor=UUIDMonitor]] mysql {}:{} ,uuid correct",ip,port);
            } else {
                reporter.resetReportCounter(entityTags, 1L, UUID_ERROR_NUM_MEASUREMENT);
                uuidLogger.info("[[monitor=UUIDMonitor]] mysql {}:{},RealUUID:{},ErrorUUID:{}", ip,port,uuidFromCommand, uuidStringFromDB);
                
                // auto-correct
                if (monitorTableSourceProvider.getUuidCorrectSwitch().equalsIgnoreCase(SWITCH_STATUS_ON)) {
                    uuidLogger.info("[[monitor=UUIDMonitor]] uuidCorrectSwitch already on");
                    autoCorrect(ip,port,uuidStringFromDB,uuidFromCommand);
                }
                
            }
        }
    }
    
    private String getUUIDFromDB(String ip, int port) throws SQLException {// select from db
        Set<String> publicCloudDc = consoleConfig.getPublicCloudDc();
        MachineTbl machineTbl;
        
        if (publicCloudDc.contains(localDcName)) {
            machineTbl = getUUIDFromRemoteDB(ip, port, localDcName);
            uuidLogger.info("[[monitor=UUIDMonitor]] get UUID from remote dc,current dc is {}",localDcName);
        } else {
            machineTbl = getUUIDFromLocalDB(ip,port);
            uuidLogger.info("[[monitor=UUIDMonitor]] get UUID from local dc,current dc is {}",localDcName);
        }
        return machineTbl != null ? machineTbl.getUuid() : null;
    }
    
    private MachineTbl getUUIDFromRemoteDB(String ip, int port, String localDcName) {
        Map<String, String> consoleDcInfos = consoleConfig.getConsoleDcInfos();
        
        if(consoleDcInfos.size() != 0) {
            for(Map.Entry<String, String> entry : consoleDcInfos.entrySet()) {
                if(!entry.getKey().equalsIgnoreCase(localDcName)) {
                    try {
                        String uri = String.format("%s/api/drc/v1/monitor/uuid?ip={ip}&port={port}", entry.getValue());
                        Map<String, Object> params = Maps.newHashMap();
                        params.put("ip", ip);
                        params.put("port", port);
                        UuidResponseVo uuidResponseVo = openService.getUUIDFromRemoteDC(uri,params);
                        if (Constants.zero.equals(uuidResponseVo.getStatus())) {
                            return uuidResponseVo.getData();
                        }
                    } catch (Throwable t) {
                        logger.warn("[[monitor=UUIDMonitor]] fail get uuid from remote dc:{}",entry.getKey(),t);
                    }
                }
            }
        }
        uuidLogger.warn(" [[monitor=UUIDMonitor]] no such mysqlMachine {}:{} in db",ip,port);
        return null;
    }
    
    private MachineTbl getUUIDFromLocalDB(String ip, int port) throws SQLException {
        MachineTbl machineTbl = machineTblDao.queryByIpPort(ip, port);
        if (machineTbl == null) {
            uuidLogger.warn(" [[monitor=UUIDMonitor]] no such mysqlMachine {}:{} in db",ip,port);
        }
        return machineTbl;
    }

    private String getUUIDFromCommand(WriteSqlOperatorWrapper sqlOperatorWrapper) throws SQLException { // show  global  variables  like  'server_uuid';
        ReadResource readResource = null;
        try {
            String command;
            if (localDcName.equalsIgnoreCase("shali")) {
                command = ALI_RDS + UUIDCommand;
            }
            else {
                command = UUIDCommand;
            }
            GeneralSingleExecution execution = new GeneralSingleExecution(command);
            readResource = sqlOperatorWrapper.select(execution);
            ResultSet rs = readResource.getResultSet();
            if (rs == null) {
                return null;
            }
            rs.next();
            String res = rs.getString(UUID_INDEX);
            return res;
        } finally {
            if (readResource != null) {
                readResource.close();
            }
        }
    }

    private boolean autoCorrect(String ip, int port, String uuidStringFromDB,String uuidFromCommand) {
        try { // update UUID
            Set<String> publicCloudDc = consoleConfig.getPublicCloudDc();
            MachineTbl sample;
            // get Pk and uuid
            if (publicCloudDc.contains(localDcName)) {
                sample = getUUIDFromRemoteDB(ip, port, localDcName);
            } else {
                sample = getUUIDFromLocalDB(ip, port);
            }
            if (sample == null) {
                uuidLogger.warn("[[monitor=UUIDMonitor]] error occur in update errorUUID when getUUid");
                return false;
            }
            // correct
            if ((localDcName.equalsIgnoreCase(ALI_DC) || localDcName.equalsIgnoreCase(AWS_DC)) && uuidStringFromDB != null) {
                String uuidString = uuidStringFromDB + "," + uuidFromCommand;
                sample.setUuid(uuidString);
            } else {
                sample.setUuid(uuidFromCommand);
            }
            
            // update
            if (updateUuid(sample)) {
                uuidLogger.info("[[monitor=UUIDMonitor]] update Machine {}:{} UUID change from {} to {}", ip, port, uuidStringFromDB, sample.getUuid());
                return true;
            }
            uuidLogger.warn("[[monitor=UUIDMonitor]] error occur in update errorUUID");
        } catch (SQLException e) {
            uuidLogger.error("[[monitor=UUIDMonitor]] SQLException occur in update errorUUID", e);
        }
        return false;
    }
    
    private boolean updateUuid(MachineTbl machineTblWithUuid) throws SQLException {
        // update
        Set<String> publicCloudDc = consoleConfig.getPublicCloudDc();
        if (publicCloudDc.contains(localDcName)) {
            Map<String, String> consoleDcInfos = consoleConfig.getConsoleDcInfos();
            
            if(consoleDcInfos.size() != 0) {
                for(Map.Entry<String, String> entry : consoleDcInfos.entrySet()) {
                    if(!entry.getKey().equalsIgnoreCase(localDcName)) {
                        try {
                            String uri = String.format("%s/api/drc/v1/monitor/uuid", entry.getValue());
                            AbstractResponse<String> response = openService.updateUuidByMachineTbl(uri, machineTblWithUuid);
                            if (Constants.zero.equals(response.getStatus())) {
                                return true;
                            }
                        } catch (Throwable t) {
                            logger.warn("[[monitor=UUIDMonitor]] fail get uuid from remote dc:{}",entry.getKey(),t);
                        }
                    }
                }
            }
            return false;
            
        } else {
            int update = machineTblDao.update(machineTblWithUuid);
            return update == 1;
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
        localDcName = sourceProvider.getLocalDcName();
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
<<<<<<< Updated upstream
    }
    
    @Override
    public void isleader() {
        isRegionLeader = true;
        this.switchToStart();
    }
=======
    }
    
    @Override
    public void isleader() {
        isRegionLeader = true;
        this.switchToStart();
    }
>>>>>>> Stashed changes

    @Override
    public void notLeader() {
        isRegionLeader = false;
        this.switchToStop();
    }
    
    @Override
    public void doSwitchToStart() throws Throwable {
        this.scheduledTask();
    }

    @Override
    public void doSwitchToStop() throws Throwable {
        this.scheduledTask();
    }

    
}
