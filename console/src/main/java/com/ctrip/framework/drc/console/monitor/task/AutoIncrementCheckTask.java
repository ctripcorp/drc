package com.ctrip.framework.drc.console.monitor.task;

import com.ctrip.framework.drc.console.config.DefaultConsoleConfig;
import com.ctrip.framework.drc.console.dao.entity.v2.MhaTblV2;
import com.ctrip.framework.drc.console.dto.v2.DoubleSyncMhaInfoDto;
import com.ctrip.framework.drc.console.ha.LeaderSwitchable;
import com.ctrip.framework.drc.console.monitor.AbstractLeaderAwareMonitor;
import com.ctrip.framework.drc.console.monitor.DefaultCurrentMetaManager;
import com.ctrip.framework.drc.console.monitor.delay.config.MonitorTableSourceProvider;
import com.ctrip.framework.drc.console.pojo.MetaKey;
import com.ctrip.framework.drc.console.service.v2.MhaReplicationServiceV2;
import com.ctrip.framework.drc.console.service.v2.MysqlServiceV2;
import com.ctrip.framework.drc.console.task.AbstractMasterMySQLEndpointObserver;
import com.ctrip.framework.drc.console.utils.MultiKey;
import com.ctrip.framework.drc.console.vo.check.v2.AutoIncrementVo;
import com.ctrip.framework.drc.core.monitor.reporter.DefaultReporterHolder;
import com.ctrip.framework.drc.core.server.observer.endpoint.MasterMySQLEndpointObserver;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.ctrip.framework.drc.console.monitor.delay.config.MonitorTableSourceProvider.SWITCH_STATUS_ON;
import static com.ctrip.framework.drc.core.server.config.SystemConfig.CONSOLE_AUTO_INCREMENT_LOGGER;

/**
 * Created by dengquanliang
 * 2023/12/14 16:59
 */
@Component
@Order(2)
public class AutoIncrementCheckTask extends AbstractLeaderAwareMonitor {

    @Autowired
    private MonitorTableSourceProvider monitorTableSourceProvider;
    @Autowired
    private DefaultConsoleConfig consoleConfig;
    @Autowired
    private MhaReplicationServiceV2 mhaReplicationServiceV2;
    @Autowired
    private MysqlServiceV2 mysqlServiceV2;

    public final int INITIAL_DELAY = 30;
    public final int PERIOD = 300;
    public final TimeUnit TIME_UNIT = TimeUnit.SECONDS;
    private static final String AUTO_INCREMENT_MEASUREMENT = "fx.drc.auto.increment";

    @Override
    public void initialize() {
        setInitialDelay(INITIAL_DELAY);
        setPeriod(PERIOD);
        setTimeUnit(TIME_UNIT);
        super.initialize();
    }

    @Override
    public void scheduledTask() {
        if (isRegionLeader && consoleConfig.isCenterRegion()) {
            final String incrementIdMonitorSwitch = monitorTableSourceProvider.getIncrementIdMonitorSwitch();
            if (!SWITCH_STATUS_ON.equalsIgnoreCase(incrementIdMonitorSwitch)) {
                CONSOLE_AUTO_INCREMENT_LOGGER.warn("[[monitor=autoIncrement]] is leader, but switch is off");
                return;
            }
            CONSOLE_AUTO_INCREMENT_LOGGER.info("[[monitor=autoIncrement]] is leader, going to check");
            try {
                DoubleSyncMhaInfoDto doubleSyncMhaInfoDto = mhaReplicationServiceV2.getAllDoubleSyncMhas();
                checkAutoIncrement(doubleSyncMhaInfoDto);
            } catch (Exception e) {

            }
        } else {
            CONSOLE_AUTO_INCREMENT_LOGGER.warn("[[monitor=autoIncrement]] is not leader do nothing");
        }

    }

    private void checkAutoIncrement(DoubleSyncMhaInfoDto doubleSyncMhaInfoDto) {
        List<MultiKey> doubleSyncMultiKeys = doubleSyncMhaInfoDto.getDoubleSyncMultiKeys();
        List<MhaTblV2> mhaTblV2s = doubleSyncMhaInfoDto.getMhaTblV2s();
        Map<Long, String> mhaMap = mhaTblV2s.stream().collect(Collectors.toMap(MhaTblV2::getId, MhaTblV2::getMhaName));

        for (MultiKey multiKey : doubleSyncMultiKeys) {
            String srcMhaName = mhaMap.get(multiKey.getKey(0));
            String dstMhaName = mhaMap.get(multiKey.getKey(1));
            AutoIncrementVo srcIncrement = mysqlServiceV2.getAutoIncrementAndOffset(srcMhaName);
            AutoIncrementVo dstIncrement = mysqlServiceV2.getAutoIncrementAndOffset(dstMhaName);
            boolean correctIncrement = checkAutoIncrement(srcIncrement, dstIncrement);
            if (!correctIncrement) {
                DefaultReporterHolder.getInstance().reportResetCounter(getTags(srcMhaName, dstMhaName), 1L, AUTO_INCREMENT_MEASUREMENT);
            }
        }
    }

    private Map<String, String> getTags(String srcMhaName, String dstMhaName) {
        Map<String, String> tags = new HashMap<>();
        tags.put("srcMhaName", srcMhaName);
        tags.put("dstMhaName", dstMhaName);
        return tags;
    }

    private boolean checkAutoIncrement(AutoIncrementVo srcIncrement, AutoIncrementVo dstIncrement) {
        if (srcIncrement == null || dstIncrement == null) {
            CONSOLE_AUTO_INCREMENT_LOGGER.warn("increment is null");
            return false;
        }
        int srcIncrementStep = srcIncrement.getIncrement();
        int srcOffset = srcIncrement.getOffset();
        int dstIncrementStep = dstIncrement.getIncrement();
        int dstOffset = dstIncrement.getOffset();

        if (srcOffset == dstOffset) {
            return false;
        }
        if (srcIncrementStep == 1 || dstIncrementStep == 1) {
            return false;
        }
        return true;
    }
}
