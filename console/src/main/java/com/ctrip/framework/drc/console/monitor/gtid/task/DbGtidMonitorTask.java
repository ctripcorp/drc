package com.ctrip.framework.drc.console.monitor.gtid.task;

import com.ctrip.framework.drc.console.config.DefaultConsoleConfig;
import com.ctrip.framework.drc.console.ha.LeaderSwitchable;
import com.ctrip.framework.drc.console.monitor.CurrentDstMetaManager;
import com.ctrip.framework.drc.console.monitor.delay.config.DataCenterService;
import com.ctrip.framework.drc.console.monitor.delay.config.MonitorTableSourceProvider;
import com.ctrip.framework.drc.console.monitor.delay.config.v2.MetaProviderV2;
import com.ctrip.framework.drc.console.monitor.gtid.function.CheckDbGtid;
import com.ctrip.framework.drc.console.pojo.MetaKey;
import com.ctrip.framework.drc.console.service.v2.CacheMetaService;
import com.ctrip.framework.drc.console.task.AbstractMasterMySQLEndpointObserver;
import com.ctrip.framework.drc.core.entity.Drc;
import com.ctrip.framework.drc.core.monitor.datasource.DataSourceManager;
import com.ctrip.framework.drc.core.server.observer.endpoint.MasterMySQLEndpointObserver;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static com.ctrip.framework.drc.console.monitor.delay.config.MonitorTableSourceProvider.SWITCH_STATUS_ON;
import static com.ctrip.framework.drc.console.monitor.delay.config.MonitorTableSourceProvider.getInstance;

/**
 * @author shenhaibo
 * @version 1.0
 * date: 2019-12-25
 * periodically check whether there is missing gtid
 */
@Order(1)
@Component
public class DbGtidMonitorTask extends AbstractMasterMySQLEndpointObserver implements MasterMySQLEndpointObserver , LeaderSwitchable {

    @Autowired private DataCenterService dataCenterService;

    @Autowired private MonitorTableSourceProvider monitorTableSourceProvider;
    
    @Autowired private DefaultConsoleConfig consoleConfig;

    @Autowired private CacheMetaService cacheMetaService;

    @Autowired private MetaProviderV2 metaProviderV2;

    @Autowired
    private CurrentDstMetaManager currentDstMetaManager;

    public static final int INITIAL_DELAY = 0;

    public static final int PERIOD = getInstance().getGtidMonitorPeriod();

    public static final TimeUnit TIME_UNIT = TimeUnit.SECONDS;

    @Autowired
    private CheckDbGtid checkDbGtid;
    
    @Override
    public void initialize() {
        super.initialize();
        currentDstMetaManager.addObserver(this);
    }

    @Override
    public void clearOldEndpointResource(Endpoint endpoint) {
        DataSourceManager.getInstance().clearDataSource(endpoint);
    }

    @Override
    public void scheduledTask() {
        if (!isRegionLeader) {
            logger.info("[[monitor=db_gtid]] not a Leader, stop monitor");
            checkDbGtid.resourcesRelease();
            return;
        }
        if (!SWITCH_STATUS_ON.equalsIgnoreCase(monitorTableSourceProvider.getGtidMonitorSwitch())) {
            logger.info("[[monitor=db_gtid]] is Leader,but switch is off doNothing");
            return;
        }
        logger.info("[[monitor=db_gtid]] is Leader, going to check gtid");
        Drc drc = metaProviderV2.getDrc();
        Map<String, Map<String, Set<String>>> mhaDbUuidsMap = cacheMetaService.getMhaDbUuidsMap(dcsInRegion, drc);
        checkDbGtid.checkDbGtidGap(mhaDbUuidsMap, masterMySQLEndpointMap);
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

    @Override
    public int getDefaultInitialDelay() {
        return INITIAL_DELAY;
    }

    @Override
    public int getDefaultPeriod(){
        return PERIOD;
    }

    @Override
    public TimeUnit getDefaultTimeUnit() {
        return TIME_UNIT;
    }
    
}
