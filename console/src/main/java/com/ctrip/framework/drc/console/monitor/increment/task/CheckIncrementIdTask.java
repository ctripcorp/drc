package com.ctrip.framework.drc.console.monitor.increment.task;

import com.ctrip.framework.drc.console.aop.PossibleRemote;
import com.ctrip.framework.drc.console.ha.LeaderSwitchable;
import com.ctrip.framework.drc.console.monitor.DefaultCurrentMetaManager;
import com.ctrip.framework.drc.console.monitor.delay.config.DbClusterSourceProvider;
import com.ctrip.framework.drc.console.monitor.delay.config.MonitorTableSourceProvider;
import com.ctrip.framework.drc.console.monitor.delay.impl.execution.GeneralSingleExecution;
import com.ctrip.framework.drc.console.monitor.delay.impl.operator.WriteSqlOperatorWrapper;
import com.ctrip.framework.drc.console.pojo.MetaKey;
import com.ctrip.framework.drc.console.task.AbstractMasterMySQLEndpointObserver;
import com.ctrip.framework.drc.console.utils.MySqlUtils;
import com.ctrip.framework.drc.core.entity.DbCluster;
import com.ctrip.framework.drc.core.monitor.entity.MhaGroupEntity;
import com.ctrip.framework.drc.core.monitor.enums.AutoIncrementEnum;
import com.ctrip.framework.drc.core.monitor.operator.ReadResource;
import com.ctrip.framework.drc.core.monitor.operator.SqlOperator;
import com.ctrip.framework.drc.core.monitor.reporter.DefaultReporterHolder;
import com.ctrip.framework.drc.core.server.observer.endpoint.MasterMySQLEndpointObserver;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.ArrayUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static com.ctrip.framework.drc.console.monitor.delay.config.MonitorTableSourceProvider.SWITCH_STATUS_ON;
import static com.ctrip.framework.drc.core.server.config.SystemConfig.CONSOLE_AUTO_INCREMENT_LOGGER;

/**
 * @author shenhaibo
 * @version 1.0
 * date: 2020-02-04
 */
@Component
public class CheckIncrementIdTask extends AbstractMasterMySQLEndpointObserver implements MasterMySQLEndpointObserver , LeaderSwitchable {

    @Autowired
    private DbClusterSourceProvider dbClusterSourceProvider;

    @Autowired
    private MonitorTableSourceProvider monitorTableSourceProvider;

    @Autowired
    private DefaultCurrentMetaManager currentMetaManager;

    private List<Set<DbClusterSourceProvider.Mha>> mhaGroups;

    private Map<String, MhaGroupEntity> mhaGroupEntityMap = Maps.newConcurrentMap();

    private static final int AUTO_INCREMENT_INCREMENT_SET_SIZE = 1;

    private static final int AUTO_INCREMENT_INDEX = 2;

    public static final int INITIAL_DELAY = 60;

    public static final int PERIOD = 60;

    public static final TimeUnit TIME_UNIT = TimeUnit.SECONDS;

    private static final String CHECK_INCREMENT_SQL = "show global variables like 'auto_increment_increment';";

    private static final String CHECK_OFFSET_SQL = "show global variables like 'auto_increment_offset';";

    private volatile boolean isRegionLeader = false;

    @Override
    public void initialize() {
        super.initialize();
        currentMetaManager.addObserver(this);
        mhaGroups = dbClusterSourceProvider.getMhaGroups();
    }

    @Override
    public void scheduledTask() {
        if (isRegionLeader) {
            String incrementIdMonitorSwitch = monitorTableSourceProvider.getIncrementIdMonitorSwitch();
            if(SWITCH_STATUS_ON.equalsIgnoreCase(incrementIdMonitorSwitch)) {
                CONSOLE_AUTO_INCREMENT_LOGGER.info("[[monitor=autoIncrement]] is Leader,going to check");
                try {
                    for(Set<DbClusterSourceProvider.Mha> mhaGroup : mhaGroups) {
                        if(isFilteredOut(mhaGroup)) {
                            continue;
                        }
                        String mhaGroupKey = getMhaGroupKey(mhaGroup);
                        AutoIncrementEnum isCorrect = checkIncrementId(mhaGroupKey, mhaGroup) ? AutoIncrementEnum.CORRECT : AutoIncrementEnum.INCORRECT;
                        MhaGroupEntity mhaGroupEntity = getMhaGroupEntity(mhaGroupKey, mhaGroup);
                        DefaultReporterHolder.getInstance().reportAutoIncrementId(mhaGroupEntity, isCorrect);
                    }
                } catch (Exception e) {
                    CONSOLE_AUTO_INCREMENT_LOGGER.error("[[monitor=autoIncrement]]Check Increment config: ", e);
                }
            } else {
                CONSOLE_AUTO_INCREMENT_LOGGER.warn("[[monitor=autoIncrement]] is Leader,but switch is off");
            }
        } else {
            DefaultReporterHolder.getInstance().removeRegister("fx.drc.increment.id");
            CONSOLE_AUTO_INCREMENT_LOGGER.info("[[monitor=autoIncrement]] not leader,remove monitor");
        }
        
    }

    /**
     * for every mhaGroup, check if the auto increment id configuration is correct
     */
    protected boolean checkIncrementId(String mhaGroupKey, Set<DbClusterSourceProvider.Mha> mhaGroup) {
        Set<Integer> autoIncrementIncrementSet = new HashSet<>();
        Set<Integer> autoIncrementOffsetSet = new HashSet<>();
        for(DbClusterSourceProvider.Mha mha : mhaGroup) {
            String mhaName = mha.getDbCluster().getMhaName();
            MetaKey metaKey = new MetaKey(mha.getDc(), mha.getDbCluster().getId(), mha.getDbCluster().getName(), mha.getDbCluster().getMhaName());
            Endpoint endpoint = masterMySQLEndpointMap.get(metaKey);
            if (null == endpoint) {
                CONSOLE_AUTO_INCREMENT_LOGGER.warn("UNLIKELY-masterMySQLEndpointMap does not contain endpoint for {}", metaKey);
                return true;
            }
            CONSOLE_AUTO_INCREMENT_LOGGER.debug("{}({})", metaKey, endpoint.getSocketAddress());
            try {
                Integer autoIncrementIncrement = getAutoIncrementStep(CHECK_INCREMENT_SQL, endpoint);
                autoIncrementIncrementSet.add(autoIncrementIncrement);
                Integer autoIncrementOffset = getAutoIncrementOffSet(CHECK_OFFSET_SQL, endpoint);
                autoIncrementOffsetSet.add(autoIncrementOffset);
                CONSOLE_AUTO_INCREMENT_LOGGER.debug("{}({}). INCREMENT:{}, OFFSET:{}", metaKey, endpoint.getSocketAddress(), autoIncrementIncrement, autoIncrementOffset);
            } catch (Throwable t) {
                CONSOLE_AUTO_INCREMENT_LOGGER.warn("Fail check auto increment for {} while checking {}", mhaGroupKey, endpoint.getSocketAddress(), t);
                return false;
            }
        }
        return checkIncrementConfig(autoIncrementIncrementSet, autoIncrementOffsetSet, mhaGroup, mhaGroupKey);
    }
    
    protected Integer getAutoIncrementStep(String mha,Endpoint endpoint) {
        return getAutoIncrement(mha,CHECK_INCREMENT_SQL,AUTO_INCREMENT_INDEX,endpoint);
    }
    
    protected Integer getAutoIncrementOffSet(String mha,Endpoint endpoint)  {
        return getAutoIncrement(mha,CHECK_OFFSET_SQL,AUTO_INCREMENT_INDEX,endpoint);
    }
    
    @PossibleRemote(path="/api/drc/v1/local/sql/integer/query")
    protected Integer getAutoIncrement(String mha,String sql,int index,Endpoint endpoint) {
        return MySqlUtils.getSqlResultInteger(endpoint, sql, index);
    }

    /**
     * given all the AUTO_INCREMENT_INCREMENT and AUTO_INCREMENT_OFFSET in all master db for a mhaGroup
     * check if the settings are correct
     * @param autoIncrementIncrementSet
     * @param autoIncrementOffsetSet
     */
    protected boolean checkIncrementConfig(Set<Integer> autoIncrementIncrementSet, Set<Integer> autoIncrementOffsetSet, Set<DbClusterSourceProvider.Mha> mhaGroup, String mhaGroupKey) {
        int dcCount = mhaGroup.size();
        DbCluster dbCluster = mhaGroup.iterator().next().getDbCluster();
        String cluster = dbCluster.getName();

        if(autoIncrementIncrementSet.size() != AUTO_INCREMENT_INCREMENT_SET_SIZE) {
            CONSOLE_AUTO_INCREMENT_LOGGER.info("[[monitor=autoIncrement, cluster={}, mhaGroupKey={}]] Inconsistent AUTO_INCREMENT_INCREMENT", cluster, mhaGroupKey);
            return false;
        }
        Integer increment = autoIncrementIncrementSet.iterator().next();
        if(increment < dcCount) {
            CONSOLE_AUTO_INCREMENT_LOGGER.info("[[monitor=autoIncrement, cluster={}, mhaGroupKey={}]] AUTO_INCREMENT_INCREMENT should be bigger than or equal to {}", cluster, mhaGroupKey, dcCount);
            return false;
        }
        if(autoIncrementOffsetSet.size() != dcCount) {
            CONSOLE_AUTO_INCREMENT_LOGGER.info("[[monitor=autoIncrement, cluster={}, mhaGroupKey={}]] AUTO_INCREMENT_OFFSET cannot be same between different DCs", cluster, mhaGroupKey);
            return false;
        }
        for(Integer offset : autoIncrementOffsetSet) {
            if(offset < 1 || offset > increment) {
                CONSOLE_AUTO_INCREMENT_LOGGER.info("[[monitor=autoIncrement, cluster={}, mhaGroupKey={}]] Incorrect AUTO_INCREMENT_OFFSET setting", cluster, mhaGroupKey);
                return false;
            }
        }
        CONSOLE_AUTO_INCREMENT_LOGGER.info("[[monitor=autoIncrement, cluster={}, mhaGroupKey={}]] Correct AUTO_INCREMENT setting", cluster, mhaGroupKey);
        return true;
    }

    protected String getMhaGroupKey(Set<DbClusterSourceProvider.Mha> mhaGroup) {
        DbCluster firstDbCluster = mhaGroup.stream().findFirst().get().getDbCluster();
        StringBuffer mhaGroupKeyBuffer = new StringBuffer().append(firstDbCluster.getMhaName());
        mhaGroup.stream().skip(1).forEach(mha -> mhaGroupKeyBuffer.append('.').append(mha.getDbCluster().getMhaName()));
        return mhaGroupKeyBuffer.toString();
    }

    protected MhaGroupEntity getMhaGroupEntity(String mhaGroupKey, Set<DbClusterSourceProvider.Mha> mhaGroup) {
        DbCluster firstDbCluster = mhaGroup.stream().findFirst().get().getDbCluster();

        if(mhaGroupEntityMap.containsKey(mhaGroupKey)) {
            return mhaGroupEntityMap.get(mhaGroupKey);
        }
        MhaGroupEntity mhaGroupEntity = new MhaGroupEntity.Builder()
                .clusterAppId(firstDbCluster.getAppId())
                .buName(firstDbCluster.getBuName())
                .clusterName(firstDbCluster.getName())
                .mhaGroupKey(mhaGroupKey)
                .build();
        mhaGroupEntityMap.put(mhaGroupKey, mhaGroupEntity);
        return mhaGroupEntity;
    }

    protected boolean isFilteredOut(Set<DbClusterSourceProvider.Mha> mhas) {
        String[] filterOutMhasForMultiSide = monitorTableSourceProvider.getFilterOutMhasForMultiSideMonitor();
        for (DbClusterSourceProvider.Mha mha : mhas) {
            if(ArrayUtils.contains(filterOutMhasForMultiSide, mha.getDbCluster().getMhaName())) {
                return true;
            }
        }
        return false;
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
    public void setLocalDcName() {
        // no need
    }

    @Override
    public void setLocalRegionInfo() {
        // no need
    }
    
    @Override
    public void setOnlyCarePart() {
        this.onlyCarePart = false;
    }

    @Override
    public boolean isCare(MetaKey metaKey) {
        return false;
    }

    @Override
    public void clearOldEndpointResource(Endpoint endpoint) {
        removeSqlOperator(endpoint);
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
        // do nothing ,waiting next schedule
    }

    @Override
    public void doSwitchToStop() throws Throwable {
        this.scheduledTask();
    }
}
