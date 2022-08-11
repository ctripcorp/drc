package com.ctrip.framework.drc.console.task;

import com.ctrip.framework.drc.console.config.DefaultConsoleConfig;
import com.ctrip.framework.drc.console.dao.entity.ClusterTbl;
import com.ctrip.framework.drc.console.dao.entity.MhaGroupTbl;
import com.ctrip.framework.drc.console.monitor.AbstractLeaderAwareMonitor;
import com.ctrip.framework.drc.core.service.beacon.RegisterDto;
import com.ctrip.framework.drc.console.enums.TableEnum;
import com.ctrip.framework.drc.core.service.beacon.BeaconResult;
import com.ctrip.framework.drc.console.monitor.delay.config.MonitorTableSourceProvider;
import com.ctrip.framework.drc.console.service.impl.DalServiceImpl;
import com.ctrip.framework.drc.console.service.impl.HealthServiceImpl;
import com.ctrip.framework.drc.console.service.impl.MetaInfoServiceImpl;
import com.ctrip.framework.drc.core.driver.command.packet.ResultCode;
import com.ctrip.framework.foundation.Env;
import com.ctrip.framework.foundation.Foundation;
import com.ctrip.platform.dal.dao.DalPojo;
import com.ctrip.xpipe.tuple.Pair;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.ArrayUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.DependsOn;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.ctrip.framework.drc.console.monitor.delay.config.MonitorTableSourceProvider.SWITCH_STATUS_ON;

/**
 * @author shenhaibo
 * @version 1.0
 * date: 2020-10-10
 */
@Component
@Order(2)
@DependsOn({"metaInfoServiceImpl"})
public class PeriodicalRegisterBeaconTask extends AbstractLeaderAwareMonitor {

    @Autowired
    private MonitorTableSourceProvider monitorTableSourceProvider;

    @Autowired
    private DalServiceImpl dalService;

    @Autowired
    private HealthServiceImpl healthService;

    @Autowired
    private DefaultConsoleConfig defaultConsoleConfig;

    @Autowired
    private MetaInfoServiceImpl metaInfoService;

    public static final String DEFAULT_MYSQL_ALL_DOWN_SYSTEM_NAME = "drc";
    
    public static final String DEFAULT_MYSQL_ALL_DOWN_TYPE_NAME = "drc-mysql";

    public static final String DEFAULT_HIGH_LATENCY_SYSTEM_NAME = "drc-delay";

    public static final int REGISTER_INITIAL_DELAY = 0;

    public static final int REGISTER_PERIOD = 5;

    private Env env = Foundation.server().getEnv();

    protected void setEnv(Env env) {
        this.env = env;
    }
    
    @Override
    public void initialize() {
        setInitialDelay(REGISTER_INITIAL_DELAY);
        setPeriod(REGISTER_PERIOD);
        setTimeUnit(TimeUnit.MINUTES);
    }

    @Override
    public void scheduledTask() {
        if (isRegionLeader) {
            logger.info("[Beacon] is leader register beacon");
            updateBeaconRegistration();
        } else {
            logger.info("[Beacon] not a leader, do nothing");
        }
    }

    protected Pair<Set<String>, Set<String>> updateBeaconRegistration() {
        Set<String> mysqlShouldRegisterDalClusters = Sets.newHashSet();
        Set<String> delayShouldRegisterDalClusters = Sets.newHashSet();
        if(monitorTableSourceProvider.getBeaconRegisterSwitch().equalsIgnoreCase(SWITCH_STATUS_ON)) {
            try {
                List<DalPojo> allPojos = TableEnum.CLUSTER_TABLE.getAllPojos();
                logger.debug("All cluster size:{}", allPojos.size());
                String[] filterOutClusters = monitorTableSourceProvider.getBeaconFilterOutCluster();
                String[] filterOutMhasForMysql = monitorTableSourceProvider.getBeaconFilterOutMhaForMysql();
                String[] filterOutMhasForDelay = monitorTableSourceProvider.getBeaconFilterOutMhaForDelay();
                Set<String> mysqlRealRegisteredDalClusters = Sets.newHashSet();
                Set<String> delayRealRegisteredDalClusters = Sets.newHashSet();
                for(DalPojo pojo : allPojos) {
                    ClusterTbl clusterTbl = (ClusterTbl) pojo;
                    if (isFilteredOut(Collections.singletonList(clusterTbl.getClusterName()), filterOutClusters)) {
                        continue;
                    }
                    logger.debug("Try register for {}", clusterTbl.getClusterName());
                    List<String> allMhaNamesInCluster = metaInfoService.getAllMhaNamesInCluster(clusterTbl.getId());
                    Map<String, List<String>> realDalClusterMap = metaInfoService.getRealDalClusterMap(clusterTbl.getClusterName(), allMhaNamesInCluster);
                    if(!isFilteredOut(allMhaNamesInCluster, filterOutMhasForMysql)) {
                        mysqlShouldRegisterDalClusters.addAll(realDalClusterMap.keySet());
                        if (monitorTableSourceProvider.getBeaconRegisterMySqlSwitch().equalsIgnoreCase(SWITCH_STATUS_ON)) {
                            List<String> mysqlRegisteredDalClusters = registerDalClusterInBeacon(realDalClusterMap, DEFAULT_MYSQL_ALL_DOWN_SYSTEM_NAME);
                            mysqlRealRegisteredDalClusters.addAll(mysqlRegisteredDalClusters);
                        }
                    }
                    if(!isFilteredOut(allMhaNamesInCluster, filterOutMhasForDelay)) {
                        delayShouldRegisterDalClusters.addAll(realDalClusterMap.keySet());
                        if (monitorTableSourceProvider.getBeaconRegisterDelaySwitch().equalsIgnoreCase(SWITCH_STATUS_ON)) {
                            List<String> delayRegisteredDalClusters = registerDalClusterInBeacon(realDalClusterMap, DEFAULT_HIGH_LATENCY_SYSTEM_NAME);
                            delayRealRegisteredDalClusters.addAll(delayRegisteredDalClusters);
                        }
                    }
                }
                if(monitorTableSourceProvider.getBeaconRegisterMySqlSwitch().equalsIgnoreCase(SWITCH_STATUS_ON)) {
                    logger.info("[Beacon] should register dalcluster for mysql: {}, registered dalcluster: {}", mysqlShouldRegisterDalClusters, mysqlRealRegisteredDalClusters);
                    List<String> mysqlShouldDeregisterDalClusters = getDeregisterDalCluster(mysqlShouldRegisterDalClusters, DEFAULT_MYSQL_ALL_DOWN_SYSTEM_NAME);
                    List<String> mysqlDeregisteredDalClusters = healthService.deRegister(mysqlShouldDeregisterDalClusters, DEFAULT_MYSQL_ALL_DOWN_SYSTEM_NAME);
                    logger.info("[Beacon] should deregister dalcluster for mysql: {}, deregistered dal clusters: {}", mysqlShouldDeregisterDalClusters, mysqlDeregisteredDalClusters);
                }

                if(monitorTableSourceProvider.getBeaconRegisterDelaySwitch().equalsIgnoreCase(SWITCH_STATUS_ON)) {
                    logger.info("[Beacon] should register dalcluster for delay: {}, registered dalcluster: {}", delayShouldRegisterDalClusters, delayRealRegisteredDalClusters);
                    List<String> delayShouldDeregisterDalClusters = getDeregisterDalCluster(delayShouldRegisterDalClusters, DEFAULT_HIGH_LATENCY_SYSTEM_NAME);
                    List<String> delayDeregisteredDalClusters = healthService.deRegister(delayShouldDeregisterDalClusters, DEFAULT_HIGH_LATENCY_SYSTEM_NAME);
                    logger.info("[Beacon] should deregister dalcluster for delay: {}, deregistered dal clusters: {}", delayShouldDeregisterDalClusters, delayDeregisteredDalClusters);
                }
            } catch (Throwable t) {
                logger.error("[Beacon] fail get all dalClusters from meta db, do nothing and try next round", t);
            }
        }
        return new Pair<>(mysqlShouldRegisterDalClusters, delayShouldRegisterDalClusters);
    }

    protected List<String> registerDalClusterInBeacon(Map<String, List<String>> realDalClusterMap, String systemName) {
        List<String> registeredDalClusters = Lists.newArrayList();
        logger.info("[Beacon] register {} for {}", systemName, realDalClusterMap.keySet());

        if(!DEFAULT_MYSQL_ALL_DOWN_SYSTEM_NAME.equalsIgnoreCase(systemName) && !DEFAULT_HIGH_LATENCY_SYSTEM_NAME.equalsIgnoreCase(systemName)) {
            logger.debug("[[dalCluster={},system={}]][Beacon] no such system", realDalClusterMap.keySet(), systemName);
            return registeredDalClusters;
        }

        registerDalClusterLoop:
        for(Map.Entry<String, List<String>> entry : realDalClusterMap.entrySet()) {
            String dalCluster = entry.getKey();
            List<String> mhas = entry.getValue();
            logger.info("[Beacon] register {} for {}({})", systemName, dalCluster, mhas);

            try {
                List<RegisterDto.NodeGroup> nodeGroups = Lists.newArrayList();

                for (String mha : mhas) {
                    String dc = dalService.getDc(mha, env);
                    logger.debug("[[dalCluster={},mhas={},system={}]][Beacon] {} in {}", dalCluster, mhas, systemName, mha, dc);
                    List<String> nodes;
                    if (DEFAULT_MYSQL_ALL_DOWN_SYSTEM_NAME.equalsIgnoreCase(systemName)) {
                        nodes = metaInfoService.getMachines(mha);
                        logger.debug("[[dalCluster={},mhas={},system={}]][Beacon] nodes in {}: {}", dalCluster, mhas, systemName, mha, nodes);
                    } else {
                        Map<String, String> consoleDcInfos = defaultConsoleConfig.getConsoleDcEndpointInfos();
                        String consoleEndpoint = consoleDcInfos.get(dc);
                        if(null == consoleEndpoint) {
                            logger.warn("[[dalCluster={},mhas={},system={}]][Beacon] {} no ", dalCluster, mhas, systemName, mha);
                            continue registerDalClusterLoop;
                        }
                        nodes = Collections.singletonList(consoleEndpoint);
                        logger.debug("[[dalCluster={},mhas={},system={}]][Beacon] nodes in {}: {}", dalCluster, mhas, systemName, mha, nodes);
                    }
                    logger.debug("[Beacon] nodes for {}, {}-{}-{}: {}", systemName, dalCluster, mha, dc, nodes);
                    nodeGroups.add(new RegisterDto.NodeGroup(mha, dc, nodes));
                }

                RegisterDto.Extra extra = new RegisterDto.Extra();
                if (DEFAULT_MYSQL_ALL_DOWN_SYSTEM_NAME.equalsIgnoreCase(systemName)) {
                    MhaGroupTbl mhaGroupForMha = metaInfoService.getMhaGroupForMha(mhas.get(0));
                    extra.setUsername(mhaGroupForMha.getMonitorUser());
                    extra.setPassword(mhaGroupForMha.getMonitorPassword());
                    extra.setType(DEFAULT_MYSQL_ALL_DOWN_TYPE_NAME);
                } else {
                    extra.setUsername("");
                    extra.setPassword("");
                    extra.setType(DEFAULT_HIGH_LATENCY_SYSTEM_NAME);
                }
                logger.debug("[Beacon] extra for {}, {}: {}", systemName, dalCluster, extra);

                BeaconResult result = healthService.doRegister(dalCluster, new RegisterDto(nodeGroups, extra), systemName);
                if (result.getCode() == ResultCode.HANDLE_SUCCESS.getCode()) {
                    registeredDalClusters.add(dalCluster);
                }
            } catch (Exception e) {
                logger.error("Fail register {}, ", dalCluster, e);
            }

        }
        return registeredDalClusters;
    }

    protected List<String> getDeregisterDalCluster(Set<String> shouldRegisterDalClusters, String systemName) {
        List<String> registeredDalClusterNames = healthService.getRegisteredClusters(systemName);
        logger.info("shouldRegisterDalClusterNames: {}, current registered DalClusterNames in beacon: {}", shouldRegisterDalClusters, registeredDalClusterNames);
        return registeredDalClusterNames.stream().filter(p -> !shouldRegisterDalClusters.contains(p)).collect(Collectors.toList());
    }

    protected boolean isFilteredOut(List<String> names, String[] filterOutNames) {
        for(String mhaName : names) {
            if(ArrayUtils.contains(filterOutNames, mhaName)) {
                return true;
            }
        }
        return false;
    }
    
    @Override
    public void switchToLeader() throws Throwable {
        this.scheduledTask();
    }

    @Override
    public void switchToSlave() throws Throwable {
        // nothing to do
    }
}
