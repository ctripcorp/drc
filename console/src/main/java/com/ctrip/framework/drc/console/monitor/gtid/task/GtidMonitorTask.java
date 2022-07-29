package com.ctrip.framework.drc.console.monitor.gtid.task;

import com.ctrip.framework.drc.console.config.DefaultConsoleConfig;
import com.ctrip.framework.drc.console.ha.LeaderSwitchable;
import com.ctrip.framework.drc.console.monitor.DefaultCurrentMetaManager;
import com.ctrip.framework.drc.console.monitor.delay.config.DbClusterSourceProvider;
import com.ctrip.framework.drc.console.monitor.delay.config.MonitorTableSourceProvider;
import com.ctrip.framework.drc.console.monitor.gtid.function.CheckGtid;
import com.ctrip.framework.drc.console.pojo.MetaKey;
import com.ctrip.framework.drc.console.service.impl.MetaInfoServiceImpl;
import com.ctrip.framework.drc.console.task.AbstractMasterMySQLEndpointObserver;
import com.ctrip.framework.drc.core.monitor.datasource.DataSourceManager;
import com.ctrip.framework.drc.core.server.observer.endpoint.MasterMySQLEndpointObserver;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import java.util.List;
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
@Order(2)
@Component
public class GtidMonitorTask extends AbstractMasterMySQLEndpointObserver implements MasterMySQLEndpointObserver , LeaderSwitchable {

    @Autowired
    private DbClusterSourceProvider sourceProvider;

    @Autowired
    private MonitorTableSourceProvider monitorTableSourceProvider;
    
    @Autowired
    private DefaultConsoleConfig consoleConfig;

    @Autowired
    private MetaInfoServiceImpl metaInfoService;

    @Autowired
    private DefaultCurrentMetaManager currentMetaManager;

    public static final int INITIAL_DELAY = 0;

    public static final int PERIOD = getInstance().getGtidMonitorPeriod();

    public static final TimeUnit TIME_UNIT = TimeUnit.SECONDS;

    @Autowired
    private CheckGtid checkGtid;

    private volatile boolean isRegionLeader = false;
    
    @Override
    public void initialize() {
        super.initialize();
        currentMetaManager.addObserver(this);
    }

    @Override
    public void clearOldEndpointResource(Endpoint endpoint) {
        DataSourceManager.getInstance().clearDataSource(endpoint);
    }

    @Override
    public void scheduledTask() {
        if (isRegionLeader) {
            final String gtidMonitorSwitch = monitorTableSourceProvider.getGtidMonitorSwitch();
            if (SWITCH_STATUS_ON.equalsIgnoreCase(gtidMonitorSwitch)) {
                logger.info("[[monitor=gtid]] is Leader, going to check gtid");
                Map<String, Set<String>> uuidMapper = metaInfoService.getUuidMap(dcsInRegion);
                checkGtid.checkGtidGap(uuidMapper, masterMySQLEndpointMap);
            } else {
                logger.info("[[monitor=gtid]] is Leader,but switch is off doNothing");
            }
        } else {
            logger.info("[[monitor=gtid]] not a Leader, stop monitor");
            checkGtid.resourcesRelease();
        }
        
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

    @Override
    public void isleader() {
        isRegionLeader = true;
        this.switchToStart();
    }

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
