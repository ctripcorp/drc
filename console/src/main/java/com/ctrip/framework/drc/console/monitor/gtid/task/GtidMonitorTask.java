package com.ctrip.framework.drc.console.monitor.gtid.task;

import com.ctrip.framework.drc.console.monitor.DefaultCurrentMetaManager;
import com.ctrip.framework.drc.console.monitor.delay.config.DbClusterSourceProvider;
import com.ctrip.framework.drc.console.monitor.delay.config.MonitorTableSourceProvider;
import com.ctrip.framework.drc.console.monitor.gtid.function.CheckGtid;
import com.ctrip.framework.drc.console.service.impl.MetaInfoServiceImpl;
import com.ctrip.framework.drc.console.task.AbstractMasterMySQLEndpointObserver;
import com.ctrip.framework.drc.core.monitor.datasource.DataSourceManager;
import com.ctrip.framework.drc.core.server.observer.endpoint.MasterMySQLEndpointObserver;
import com.ctrip.framework.drc.core.server.utils.ThreadUtils;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
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
public class GtidMonitorTask extends AbstractMasterMySQLEndpointObserver implements MasterMySQLEndpointObserver {

    @Autowired
    private DbClusterSourceProvider sourceProvider;

    @Autowired
    private MonitorTableSourceProvider monitorTableSourceProvider;

    @Autowired
    private MetaInfoServiceImpl metaInfoService;

    @Autowired
    private DefaultCurrentMetaManager currentMetaManager;

    private ListeningExecutorService uuidGetExecutorService = MoreExecutors.listeningDecorator(ThreadUtils.newCachedThreadPool("getNonLocalUuid"));

    public static final int INITIAL_DELAY = 0;

    public static final int PERIOD = getInstance().getGtidMonitorPeriod();

    public static final TimeUnit TIME_UNIT = TimeUnit.SECONDS;

    public Map<String, Set<String>> getUuidMapper() {
        return Collections.unmodifiableMap(uuidMapper);
    }

    /**
     * key: mhaName
     * value: a Set of clusterName's MySQL's uuid which are not in local dc
     */
    private Map<String, Set<String>> uuidMapper = new ConcurrentHashMap<>();

    @Autowired
    private CheckGtid checkGtid;

    @Autowired
    private DbClusterSourceProvider dbClusterSourceProvider;

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
        final String gtidMonitorSwitch = monitorTableSourceProvider.getGtidMonitorSwitch();
        if(SWITCH_STATUS_ON.equalsIgnoreCase(gtidMonitorSwitch)) {
            Map<String, Set<String>> uuidMapper = metaInfoService.getUuidMap();
            checkGtid.checkGtidGap(uuidMapper, masterMySQLEndpointMap);
        }
    }

    @Override
    public void setLocalDcName() {
        localDcName = sourceProvider.getLocalDcName();
    }

    @Override
    public void setOnlyCareLocal() {
        this.onlyCareLocal = true;
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
