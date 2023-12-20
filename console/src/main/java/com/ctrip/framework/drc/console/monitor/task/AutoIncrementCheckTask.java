package com.ctrip.framework.drc.console.monitor.task;

import com.ctrip.framework.drc.console.config.DefaultConsoleConfig;
import com.ctrip.framework.drc.console.dao.DcTblDao;
import com.ctrip.framework.drc.console.dao.entity.DcTbl;
import com.ctrip.framework.drc.console.dao.entity.v2.MhaReplicationTbl;
import com.ctrip.framework.drc.console.dao.entity.v2.MhaTblV2;
import com.ctrip.framework.drc.console.dao.v2.MhaReplicationTblDao;
import com.ctrip.framework.drc.console.dao.v2.MhaTblV2Dao;
import com.ctrip.framework.drc.console.enums.BooleanEnum;
import com.ctrip.framework.drc.console.monitor.AbstractLeaderAwareMonitor;
import com.ctrip.framework.drc.console.monitor.delay.config.MonitorTableSourceProvider;
import com.ctrip.framework.drc.console.service.v2.MysqlServiceV2;
import com.ctrip.framework.drc.console.utils.MultiKey;
import com.ctrip.framework.drc.console.vo.check.v2.AutoIncrementVo;
import com.ctrip.framework.drc.core.monitor.reporter.DefaultReporterHolder;
import com.ctrip.framework.drc.core.monitor.reporter.Reporter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StopWatch;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
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
    private MysqlServiceV2 mysqlServiceV2;
    @Autowired
    private DcTblDao dcTblDao;
    @Autowired
    private MhaReplicationTblDao mhaReplicationTblDao;
    @Autowired
    private MhaTblV2Dao mhaTblV2Dao;

    private Reporter reporter = DefaultReporterHolder.getInstance();

    public final int INITIAL_DELAY = 30;
    public final int PERIOD = 300;
    private static final int INCREMENT_SIZE = 1000;
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
            StopWatch stopWatch = new StopWatch();
            try {
                stopWatch.start();
                checkAutoIncrement();
            } catch (Exception e) {
                CONSOLE_AUTO_INCREMENT_LOGGER.error("[[monitor=autoIncrement]] fail, {}", e);
            } finally {
                stopWatch.stop();
                CONSOLE_AUTO_INCREMENT_LOGGER.info("[[monitor=autoIncrement]] task cost: {}", stopWatch.getTotalTimeMillis());
            }
        } else {
            CONSOLE_AUTO_INCREMENT_LOGGER.warn("[[monitor=autoIncrement]] is not leader do nothing");
        }

    }

    protected void checkAutoIncrement() throws SQLException {
        List<String> mhaWhitelist = monitorTableSourceProvider.getIncrementIdMonitorWhitelist();
        List<MhaReplicationTbl> mhaReplicationTbls = mhaReplicationTblDao.queryAllExist().stream()
                .filter(e -> e.getDrcStatus().equals(BooleanEnum.TRUE.getCode())).collect(Collectors.toList());
        List<MhaTblV2> mhaTblV2s = mhaTblV2Dao.queryAllExist();
        List<DcTbl> dcTbls = dcTblDao.queryAllExist();

        List<MultiKey> doubleSyncMultiKeys = getDoubleSyncMultiKeys(mhaReplicationTbls);
        Map<Long, MhaTblV2> mhaMap = mhaTblV2s.stream().collect(Collectors.toMap(MhaTblV2::getId, Function.identity()));
        Map<Long, String> regionMap = dcTbls.stream().collect(Collectors.toMap(DcTbl::getId, DcTbl::getRegionName));
        Map<Long, List<Long>> dstMhaReplicationMap = mhaReplicationTbls.stream().collect(Collectors.groupingBy(
                MhaReplicationTbl::getDstMhaId, Collectors.mapping(MhaReplicationTbl::getSrcMhaId, Collectors.toList())));

        List<MultiKey> checkMultiKeys = new ArrayList<>();
        for (MultiKey multiKey : doubleSyncMultiKeys) {
            long srcMhaId = (long) multiKey.getKey(0);
            long dstMhaId = (long) multiKey.getKey(1);

            MhaTblV2 srcMhaTbl = mhaMap.get(srcMhaId);
            MhaTblV2 dstMhaTbl = mhaMap.get(dstMhaId);
            String srcMhaName = srcMhaTbl.getMhaName();
            String dstMhaName = dstMhaTbl.getMhaName();

            //double sync
            checkAutoIncrement(srcMhaName, dstMhaName, checkMultiKeys, mhaWhitelist);
        }

        //cyclic sync
        for (Map.Entry<Long, List<Long>> entry : dstMhaReplicationMap.entrySet()) {
            List<Long> mhaIds = entry.getValue();
            checkCyclicSyncAutoIncrement(mhaIds, regionMap, mhaMap, checkMultiKeys, mhaWhitelist);
        }
    }

    private void checkCyclicSyncAutoIncrement(List<Long> mhaIds, Map<Long, String> regionMap, Map<Long, MhaTblV2> mhaMap, List<MultiKey> checkMultiKeys, List<String> mhaWhitelist) {
        int size = mhaIds.size();
        for (int i = 0; i < size; i++) {
            long mhaId0 = mhaIds.get(i);
            MhaTblV2 mha0 = mhaMap.get(mhaId0);
            String region0 = regionMap.get(mha0.getDcId());
            for (int j = 1; j < size; j++) {
                long mhaId1 = mhaIds.get(j);
                MhaTblV2 mha1 = mhaMap.get(mhaId1);
                String region1 = regionMap.get(mha1.getDcId());
                if (region0.equals(region1)) {
                    continue;
                }
                checkAutoIncrement(mha0.getMhaName(), mha1.getMhaName(), checkMultiKeys, mhaWhitelist);
            }
        }
    }

    private void checkAutoIncrement(String mhaName0, String mhaName1, List<MultiKey> checkMultiKeys, List<String> mhaWhitelist) {
        if (mhaWhitelist.contains(mhaName0) || mhaWhitelist.contains(mhaName1)) {
            return;
        }
        if (checkMultiKeys.contains(new MultiKey(mhaName0, mhaName1)) || checkMultiKeys.contains(new MultiKey(mhaName1, mhaName0))) {
            return;
        }
        checkMultiKeys.add(new MultiKey(mhaName0, mhaName1));
        AutoIncrementVo increment0 = mysqlServiceV2.getAutoIncrementAndOffset(mhaName0);
        AutoIncrementVo increment1 = mysqlServiceV2.getAutoIncrementAndOffset(mhaName1);
        boolean correctIncrement = checkAutoIncrement(increment0, increment1);
        if (!correctIncrement) {
            CONSOLE_AUTO_INCREMENT_LOGGER.info("[[monitor=autoIncrement]] report autoIncrement, mhaName0: {}, mhaName1: {}", mhaName0, mhaName1);
            reporter.resetReportCounter(getTags(mhaName0, mhaName1), 1L, AUTO_INCREMENT_MEASUREMENT);
        }
    }


    private List<MultiKey> getDoubleSyncMultiKeys(List<MhaReplicationTbl> mhaReplicationTbls) {
        List<MultiKey> multiKeys = new ArrayList<>();
        List<MultiKey> doubleSyncMultiKeys = new ArrayList<>();
        for (MhaReplicationTbl mhaReplicationTbl : mhaReplicationTbls) {
            long srcMhaId = mhaReplicationTbl.getSrcMhaId();
            long dstMhaId = mhaReplicationTbl.getDstMhaId();
            MultiKey multiKey = new MultiKey(srcMhaId, dstMhaId);
            multiKeys.add(multiKey);
            if (multiKeys.contains(new MultiKey(dstMhaId, srcMhaId))) {
                doubleSyncMultiKeys.add(multiKey);
            }
        }
        return doubleSyncMultiKeys;
    }

    private Map<String, String> getTags(String mhaName0, String mhaName1) {
        Map<String, String> tags = new HashMap<>();
        tags.put("mhaName0", mhaName0);
        tags.put("mhaName1", mhaName1);
        return tags;
    }

    private boolean checkAutoIncrement(AutoIncrementVo increment0, AutoIncrementVo increment1) {
        if (increment0 == null || increment1 == null) {
            CONSOLE_AUTO_INCREMENT_LOGGER.warn("increment is null");
            return false;
        }
        int incrementStep0 = increment0.getIncrement();
        int offset0 = increment0.getOffset();
        int incrementStep1 = increment1.getIncrement();
        int offset1 = increment1.getOffset();

        if (offset0 == offset1) {
            return false;
        }
        if (incrementStep0 == 1 || incrementStep1 == 1) {
            return false;
        }

        List<Integer> incrementNumList0 = getAutoIncrementNum(offset0, incrementStep0);
        List<Integer> incrementNumList1 = getAutoIncrementNum(offset1, incrementStep1);
        return !CollectionUtils.containsAny(incrementNumList0, incrementNumList1);
    }

    private static List<Integer> getAutoIncrementNum(int offset, int step) {
        int index = offset;
        List<Integer> list = new ArrayList<>();
        for (int i = 1; i <= INCREMENT_SIZE; i++) {
            list.add(index);
            index += step;
        }
        return list;
    }
}
