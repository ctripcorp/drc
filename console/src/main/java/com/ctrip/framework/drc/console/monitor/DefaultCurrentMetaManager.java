package com.ctrip.framework.drc.console.monitor;

import com.ctrip.framework.drc.console.enums.ActionEnum;
import com.ctrip.framework.drc.console.monitor.comparator.MySQLEndpointComparator;
import com.ctrip.framework.drc.console.monitor.comparator.MySQLMetaComparator;
import com.ctrip.framework.drc.console.monitor.delay.config.MonitorTableSourceProvider;
import com.ctrip.framework.drc.console.pojo.MetaKey;
import com.ctrip.framework.drc.console.pojo.MonitorMetaInfo;
import com.ctrip.framework.drc.console.service.v2.CacheMetaService;
import com.ctrip.framework.drc.core.driver.command.netty.endpoint.MySqlEndpoint;
import com.ctrip.framework.drc.core.meta.comparator.MetaComparator;
import com.ctrip.framework.drc.core.server.observer.endpoint.MasterMySQLEndpointObservable;
import com.ctrip.framework.drc.core.server.observer.endpoint.MasterMySQLEndpointObserver;
import com.ctrip.framework.drc.core.server.observer.endpoint.SlaveMySQLEndpointObservable;
import com.ctrip.framework.drc.core.server.observer.endpoint.SlaveMySQLEndpointObserver;
import com.ctrip.framework.drc.core.server.utils.ThreadUtils;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.api.observer.Observer;
import com.ctrip.xpipe.utils.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Primary;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import org.unidal.tuple.Triple;

import javax.annotation.PostConstruct;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * @Author: hbshen
 * @Date: 2021/4/22
 */
@Order(1)
@Component
@Primary
public class DefaultCurrentMetaManager implements CurrentMetaManager, MasterMySQLEndpointObservable, SlaveMySQLEndpointObservable {

    private static final int DEFAULT_UPDATE_MONITOR_INITIAL_DELAY = 10;

    private static final int UPDATE_MYSQL_MONITOR_DELAY = 30;

    private static final String SWITCH_STATUS_ON = "on";

    private Logger logger = LoggerFactory.getLogger(getClass());

    private ScheduledExecutorService updateMySQLMonitorMetaScheduledExecutorService = ThreadUtils.newSingleThreadScheduledExecutor("update-mysql-meta-scheduledExecutorService");

    @Autowired protected CacheMetaService cacheMetaService;

    @Autowired private MonitorTableSourceProvider monitorTableSourceProvider;

    private List<Observer> observers = Lists.newCopyOnWriteArrayList();
    private DefaultMasterMySQLEndpointObservable masterMySQLEndpointObservable = new DefaultMasterMySQLEndpointObservable();
    private DefaultSlaveMySQLEndpointObservable slaveMySQLEndpointObservable = new DefaultSlaveMySQLEndpointObservable();

    protected Map<MetaKey, MySqlEndpoint> masterMySQLEndpoint = Maps.newConcurrentMap();
    protected Map<MetaKey, MySqlEndpoint> slaveMySQLEndpoint = Maps.newConcurrentMap();

    public synchronized void init() throws SQLException {
        if(masterMySQLEndpoint.isEmpty()) {
            MonitorMetaInfo monitorMetaInfo = getMonitorMetaInfo();
            masterMySQLEndpoint = monitorMetaInfo.getMasterMySQLEndpoint();
            logger.info("masterMySQLEndpoint size is: {}", masterMySQLEndpoint.size());
            slaveMySQLEndpoint = monitorMetaInfo.getSlaveMySQLEndpoint();
            logger.info("slaveMySQLEndpoint size is: {}", slaveMySQLEndpoint.size());
        }
    }

    @PostConstruct
    private void updateMySQLMonitorMeta() {
        updateMySQLMonitorMetaScheduledExecutorService.scheduleWithFixedDelay(() -> {
            try {
                final String updateMonitorMetaInfoSwitch = monitorTableSourceProvider.getUpdateMonitorMetaInfoSwitch();
                logger.info("[[monitor=monitors]] update monitor meta info switch switch is: {}", updateMonitorMetaInfoSwitch);
                if(SWITCH_STATUS_ON.equalsIgnoreCase(updateMonitorMetaInfoSwitch)) {
                    try {
                        MonitorMetaInfo theNewestMonitorMetaInfo = getMonitorMetaInfo();
                        checkMasterMySQLChange(masterMySQLEndpoint, theNewestMonitorMetaInfo.getMasterMySQLEndpoint());
                        checkSlaveMySQLChange(slaveMySQLEndpoint, theNewestMonitorMetaInfo.getSlaveMySQLEndpoint());
                    } catch (SQLException e) {
                        logger.error("[[monitor=monitors]] DefaultCurrentMetaManager.updateMySQLMonitorMeta(): ", e);
                    }
                }
            } catch (Throwable t) {
                logger.error("update mySQL monitor meta error", t);
            }
        }, DEFAULT_UPDATE_MONITOR_INITIAL_DELAY, UPDATE_MYSQL_MONITOR_DELAY, TimeUnit.SECONDS);
    }

    protected MonitorMetaInfo getMonitorMetaInfo() throws SQLException {
        return cacheMetaService.getMonitorMetaInfo();
    }

    @VisibleForTesting
    protected void checkMasterMySQLChange(Map<MetaKey, MySqlEndpoint> current, Map<MetaKey, MySqlEndpoint> future) {
        MySQLEndpointComparator comparator = compareMySQLMeta(current, future);
        handleMasterMySQLChange(comparator, current, future);
    }

    @VisibleForTesting
    protected void checkSlaveMySQLChange(Map<MetaKey, MySqlEndpoint> current, Map<MetaKey, MySqlEndpoint> future) {
        MySQLEndpointComparator comparator = compareMySQLMeta(current, future);
        handleSlaveMySQLChange(comparator, current, future);
    }

    @VisibleForTesting
    protected MySQLEndpointComparator compareMySQLMeta(Map<MetaKey, MySqlEndpoint> current, Map<MetaKey, MySqlEndpoint> future) {
        MySQLEndpointComparator comparator = new MySQLEndpointComparator(current, future);
        comparator.compare();
        return comparator;
    }

    private void handleMasterMySQLChange(MySQLEndpointComparator comparator, Map<MetaKey, MySqlEndpoint> current, Map<MetaKey, MySqlEndpoint> future) {
        for(MetaKey added : comparator.getAdded()) {
            MySqlEndpoint mySqlEndpointToAdd = future.get(added);
            notifyMasterMySQLEndpoint(added, mySqlEndpointToAdd, ActionEnum.ADD);
            masterMySQLEndpoint.put(added, mySqlEndpointToAdd);
        }

        for(MetaKey removed : comparator.getRemoved()) {
            MySqlEndpoint mySqlEndpointToRemove = current.get(removed);
            notifyMasterMySQLEndpoint(removed, mySqlEndpointToRemove, ActionEnum.DELETE);
            masterMySQLEndpoint.remove(removed, mySqlEndpointToRemove);
        }

        for(@SuppressWarnings("rawtypes") MetaComparator modifiedComparator : comparator.getMofified()) {
            MySQLMetaComparator mySQLMetaComparator = (MySQLMetaComparator) modifiedComparator;
            MetaKey modified = mySQLMetaComparator.getMetaKey();
            MySqlEndpoint mySqlEndpointToRemove = current.get(modified);
            MySqlEndpoint mySqlEndpointToAdd = future.get(modified);

            notifyMasterMySQLEndpoint(modified, mySqlEndpointToRemove, ActionEnum.DELETE);
            masterMySQLEndpoint.remove(modified, mySqlEndpointToRemove);
            notifyMasterMySQLEndpoint(modified, mySqlEndpointToAdd, ActionEnum.ADD);
            masterMySQLEndpoint.put(modified, mySqlEndpointToAdd);
        }
    }

    private void handleSlaveMySQLChange(MySQLEndpointComparator comparator, Map<MetaKey, MySqlEndpoint> current, Map<MetaKey, MySqlEndpoint> future) {
        for(MetaKey added : comparator.getAdded()) {
            MySqlEndpoint mySqlEndpointToAdd = future.get(added);
            notifySlaveMySQLEndpoint(added, mySqlEndpointToAdd, ActionEnum.ADD);
            slaveMySQLEndpoint.put(added, mySqlEndpointToAdd);
        }

        for(MetaKey removed : comparator.getRemoved()) {
            MySqlEndpoint mySqlEndpointToRemove = current.get(removed);
            notifySlaveMySQLEndpoint(removed, mySqlEndpointToRemove, ActionEnum.DELETE);
            slaveMySQLEndpoint.remove(removed, mySqlEndpointToRemove);
        }

        for(@SuppressWarnings("rawtypes") MetaComparator modifiedComparator : comparator.getMofified()) {
            MySQLMetaComparator mySQLMetaComparator = (MySQLMetaComparator) modifiedComparator;
            MetaKey modified = mySQLMetaComparator.getMetaKey();
            MySqlEndpoint mySqlEndpointToRemove = current.get(modified);
            MySqlEndpoint mySqlEndpointToAdd = future.get(modified);

            notifySlaveMySQLEndpoint(modified, mySqlEndpointToRemove, ActionEnum.DELETE);
            slaveMySQLEndpoint.remove(modified, mySqlEndpointToRemove);
            notifySlaveMySQLEndpoint(modified, mySqlEndpointToAdd, ActionEnum.ADD);
            slaveMySQLEndpoint.put(modified, mySqlEndpointToAdd);
        }
    }

    private void initNotify(Observer observer) throws SQLException {
        init();
        if (observer instanceof MasterMySQLEndpointObserver) {
            for(Map.Entry<MetaKey, MySqlEndpoint> entry : masterMySQLEndpoint.entrySet()) {
                observer.update(new Triple<>(entry.getKey(), entry.getValue(), ActionEnum.ADD), masterMySQLEndpointObservable);
            }
        }
        if (observer instanceof SlaveMySQLEndpointObserver) {
            for(Map.Entry<MetaKey, MySqlEndpoint> entry : slaveMySQLEndpoint.entrySet()) {
                observer.update(new Triple<>(entry.getKey(), entry.getValue(), ActionEnum.ADD), slaveMySQLEndpointObservable);
            }
        }
    }

    @VisibleForTesting
    protected void notifyMasterMySQLEndpoint(MetaKey metaKey, MySqlEndpoint endpoint, ActionEnum action) {
        logger.info("[[monitor=monitors]][notifyMaster] mhaName is: {},endPoint is: {},action is: {}",metaKey.getMhaName(),endpoint.getSocketAddress().toString(),action.getCode());
        for (Observer observer : observers) {
            observer.update(new Triple<>(metaKey, endpoint, action), masterMySQLEndpointObservable);
        }
    }

    @VisibleForTesting
    protected void notifySlaveMySQLEndpoint(MetaKey metaKey, MySqlEndpoint endpoint, ActionEnum action) {
        logger.info("[[monitor=monitors]][notifySlave] mhaName is: {},endPoint is: {},action is: {}",metaKey.getMhaName(),endpoint.getSocketAddress().toString(),action.getCode());
        for (Observer observer : observers) {
            observer.update(new Triple<>(metaKey, endpoint, action), slaveMySQLEndpointObservable);
        }
    }

    @Override
    public void updateMasterMySQL(String clusterId, Endpoint endpoint) {
        Map.Entry<MetaKey, MySqlEndpoint> entryInSlave = getEntryByClusterId(slaveMySQLEndpoint, clusterId);
        Map.Entry<MetaKey, MySqlEndpoint> entryInMaster = getEntryByClusterId(masterMySQLEndpoint, clusterId);
        if (entryInMaster == null) {
            logger.warn("[MASTER MYSQL ROLE] UNLIKELY, {}-{} not exist in master map", clusterId, endpoint.getSocketAddress());
            return;
        }
        MetaKey metaKey = entryInMaster.getKey();
        MySqlEndpoint oldMasterMySQLEndpoint = entryInMaster.getValue();
        MySqlEndpoint newMasterMySQLEndpoint = new MySqlEndpoint(endpoint.getHost(), endpoint.getPort(), oldMasterMySQLEndpoint.getUser(), oldMasterMySQLEndpoint.getPassword(), true);

        if (entryInSlave != null && entryInSlave.getValue().equals(newMasterMySQLEndpoint)) {
            logger.info("[MASTER MYSQL ROLE] endpoint transforms from slave to master, {}-{}", clusterId, endpoint.getSocketAddress());
            notifySlaveMySQLEndpoint(metaKey, entryInSlave.getValue(), ActionEnum.DELETE);
            slaveMySQLEndpoint.remove(metaKey);
        }

        if (oldMasterMySQLEndpoint.equals(newMasterMySQLEndpoint)) {
            logger.warn("[MASTER MYSQL ROLE] do nothing, new master endpoint equals old one, {}-{}", clusterId, endpoint.getSocketAddress());
            return;
        }

        logger.info("[MASTER MYSQL ROLE] new master endpoint, {}-{}", clusterId, endpoint.getSocketAddress());
        notifyMasterMySQLEndpoint(metaKey, newMasterMySQLEndpoint, ActionEnum.UPDATE);
        masterMySQLEndpoint.put(metaKey, newMasterMySQLEndpoint);

    }

    @Override
    public void updateSlaveMySQL(String clusterId, Endpoint endpoint) {

    }

    @Override
    public void addSlaveMySQL(String mhaName, Endpoint endpoint) {
        Map.Entry<MetaKey, MySqlEndpoint> entryInSlave = getEntryByMhaName(slaveMySQLEndpoint, mhaName);
        Map.Entry<MetaKey, MySqlEndpoint> entryInMaster = getEntryByMhaName(masterMySQLEndpoint, mhaName);
        if (entryInMaster == null && entryInSlave == null) {
            logger.warn("[SLAVE MYSQL ROLE] UNLIKELY, {} not exist in master or slave map, not able to add {}", mhaName, endpoint.getSocketAddress());
            return;
        }

        Map.Entry<MetaKey, MySqlEndpoint> anyOldEntry = entryInMaster != null ? entryInMaster : entryInSlave;
        MySqlEndpoint mySQLEndpoint = new MySqlEndpoint(endpoint.getHost(), endpoint.getPort(), anyOldEntry.getValue().getUser(), anyOldEntry.getValue().getPassword(), false);
        notifySlaveMySQLEndpoint(anyOldEntry.getKey(), mySQLEndpoint, ActionEnum.ADD);
        slaveMySQLEndpoint.put(anyOldEntry.getKey(), mySQLEndpoint);
    }


    private <T> Map.Entry<MetaKey, T> getEntryByClusterId(Map<MetaKey, T> endpointMap, String clusterId) {
        for (Map.Entry<MetaKey, T> entry : endpointMap.entrySet()) {
            if(clusterId.equalsIgnoreCase(entry.getKey().getClusterId())) {
                return entry;
            }
        }
        return null;
    }

    private <T> Map.Entry<MetaKey, T> getEntryByMhaName(Map<MetaKey, T> endpointMap, String mhaName) {
        for (Map.Entry<MetaKey, T> entry : endpointMap.entrySet()) {
            if(mhaName.equalsIgnoreCase(entry.getKey().getMhaName())) {
                return entry;
            }
        }
        return null;
    }


    @Override
    public void addObserver(Observer observer) {
        if (!observers.contains(observer)) {
            observers.add(observer);
            try {
                initNotify(observer);
            } catch (SQLException e) {
                logger.error("notify error when adding observer, observer is: {}", observer.getClass());
            }
        }
    }

    @Override
    public void removeObserver(Observer observer) {
        observers.remove(observer);
    }
    
    public static class DefaultMasterMySQLEndpointObservable implements MasterMySQLEndpointObservable{
        private List<Observer> observers;

        @Override
        public void addObserver(Observer observer) {
            observers.add(observer);
        }

        @Override
        public void removeObserver(Observer observer) {
            observers.remove(observer);
        }

        public DefaultMasterMySQLEndpointObservable(List<Observer> observers){
            this.observers = observers;
        }
        
        // only for masterAndSalveObservable Holder
        public DefaultMasterMySQLEndpointObservable(){
            
        }
        
    }

    public static class DefaultSlaveMySQLEndpointObservable implements SlaveMySQLEndpointObservable{
        private List<Observer> observers;

        @Override
        public void addObserver(Observer observer) {
            observers.add(observer);
        }

        @Override
        public void removeObserver(Observer observer) {
            observers.remove(observer);
        }
        
        public DefaultSlaveMySQLEndpointObservable(List<Observer> observers){
            this.observers = observers;
        }
        
        // only for masterAndSalveObservable Holder
        public DefaultSlaveMySQLEndpointObservable(){
            
        }
    }
}
