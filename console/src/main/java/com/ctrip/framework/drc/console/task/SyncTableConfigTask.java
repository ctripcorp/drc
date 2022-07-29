package com.ctrip.framework.drc.console.task;

import com.ctrip.framework.drc.console.dao.entity.MhaTbl;
import com.ctrip.framework.drc.console.dao.entity.ReplicatorGroupTbl;
import com.ctrip.framework.drc.console.enums.BooleanEnum;
import com.ctrip.framework.drc.console.ha.LeaderSwitchable;
import com.ctrip.framework.drc.console.monitor.AbstractMonitor;
import com.ctrip.framework.drc.console.monitor.delay.config.MonitorTableSourceProvider;
import com.ctrip.framework.drc.console.pojo.TableConfig;
import com.ctrip.framework.drc.console.service.impl.DalServiceImpl;
import com.ctrip.framework.drc.console.service.impl.MetaGenerator;
import com.ctrip.framework.drc.console.service.impl.MetaInfoServiceImpl;
import com.ctrip.framework.drc.console.utils.DalUtils;
import com.ctrip.framework.drc.core.server.config.SystemConfig;
import com.ctrip.framework.foundation.Env;
import com.ctrip.framework.foundation.Foundation;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.DependsOn;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.ctrip.framework.drc.console.monitor.delay.config.MonitorTableSourceProvider.SWITCH_STATUS_ON;

/**
 * @Author limingdong
 * @create 2021/7/8
 */
@Component
@DependsOn({"metaInfoServiceImpl"})
public class SyncTableConfigTask extends AbstractMonitor implements LeaderSwitchable {

    @Autowired
    private DalServiceImpl dalService;

    @Autowired
    private MetaInfoServiceImpl metaInfoService;

    @Autowired
    private MetaGenerator metaService;

    @Autowired
    private MonitorTableSourceProvider monitorTableSourceProvider;

    public static final int REGISTER_INITIAL_DELAY = 0;

    public static final int REGISTER_PERIOD = 5;

    private Env env = Foundation.server().getEnv();

    private DalUtils dalUtils = DalUtils.getInstance();

    private volatile boolean isRegionLeader = false;
    
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
            logger.info("[[task=SyncTableConfigTask]] is leader, going to sync excluded tables");
            String syncMhaSwitch = monitorTableSourceProvider.getSyncTableConfigSwitch();
            if(SWITCH_STATUS_ON.equalsIgnoreCase(syncMhaSwitch)) {
                logger.info("[[task=SyncTableConfigTask]] sync excluded tables");
                updateExcludedTable();
            } else {
                logger.warn("[[task=SyncTableConfigTask]] is leader but switch is off");
            }
        } else {
            logger.info("[[task=SyncTableConfigTask]]not a leader do nothing");
        }
        
    }

    protected void updateExcludedTable() {
        try {
            List<String> mhaNames = Lists.newArrayList();
            Map<String, ReplicatorGroupTbl> mha2ReplicatorGroupTbl = Maps.newHashMap();
            List<ReplicatorGroupTbl> replicatorGroupTbls = metaService.getReplicatorGroupTbls();
            for(ReplicatorGroupTbl replicatorGroupTbl : replicatorGroupTbls) {
                if(BooleanEnum.TRUE.getCode().equals(replicatorGroupTbl.getDeleted())) {
                    continue;
                }
                MhaTbl mhaTbl = metaInfoService.getMha(replicatorGroupTbl.getMhaId());
                if (mhaTbl != null && BooleanEnum.FALSE.getCode().equals(mhaTbl.getDeleted()) && !mhaNames.contains(mhaTbl.getMhaName())) {
                    mhaNames.add(mhaTbl.getMhaName());
                    mha2ReplicatorGroupTbl.put(mhaTbl.getMhaName(), replicatorGroupTbl);
                }
            }

            Map<String, Map<String, List<String>>> mhaDbNames = dalService.getDbNames(mhaNames, env);
            for (Map.Entry<String, Map<String, List<String>>> entry : mhaDbNames.entrySet()) {
                Map<String, List<String>> values = entry.getValue();
                for (Map.Entry<String, List<String>> e : values.entrySet()) {
                    String dalClusterName = e.getKey();
                    List<TableConfig> tableConfigList = dalService.getTableConfigs(dalClusterName);
                    if (tableConfigList == null) {
                        continue;
                    }
                    List<String> dbs = e.getValue();
                    String excludedTables = getExcludedTables(dbs, tableConfigList);
                    ReplicatorGroupTbl replicatorGroupTbl = mha2ReplicatorGroupTbl.get(entry.getKey());
                    String previousExcludedTables = replicatorGroupTbl.getExcludedTables();
                    if (!excludedTables.equalsIgnoreCase(previousExcludedTables)) {
                        replicatorGroupTbl.setExcludedTables(excludedTables);
                        dalUtils.updateReplicatorGroupTbl(replicatorGroupTbl);
                        logger.info("[ReplicatorGroupTbl] update excludedTables for {} from {} -> {}", replicatorGroupTbl.getMhaId(), previousExcludedTables, excludedTables);
                    }
                }
            }
        } catch (Throwable t) {
            logger.error("[updateExcludedTable] fail get all dalClusters from meta db, do nothing and try next round", t);
        }

    }

    private String getExcludedTables(List<String> dbs, List<TableConfig> tableConfigList) {
        StringBuilder excludedTables = new StringBuilder();
        for(String db: dbs) {
            for (TableConfig tableConfig : tableConfigList) {
                if (tableConfig.isIgnoreReplication()) {
                    excludedTables.append(db).append(SystemConfig.DOT).append(tableConfig.getTableName()).append(SystemConfig.COMMA);
                }
            }
        }

        if (excludedTables.length() > 0) {
            return excludedTables.substring(0, excludedTables.length() - 1);
        }
        return StringUtils.EMPTY;
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
        // nothing to do ,wait next schedule
    }

    @Override
    public void doSwitchToStop() throws Throwable {
        // nothing to do
    }
}
