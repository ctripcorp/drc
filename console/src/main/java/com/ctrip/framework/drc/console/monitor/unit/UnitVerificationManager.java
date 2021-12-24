package com.ctrip.framework.drc.console.monitor.unit;

import com.ctrip.framework.drc.console.dao.entity.MhaGroupTbl;
import com.ctrip.framework.drc.console.dao.entity.MhaTbl;
import com.ctrip.framework.drc.console.enums.BooleanEnum;
import com.ctrip.framework.drc.console.monitor.AbstractMonitor;
import com.ctrip.framework.drc.console.monitor.delay.config.MonitorTableSourceProvider;
import com.ctrip.framework.drc.console.service.impl.MetaInfoServiceImpl;
import com.ctrip.framework.drc.console.service.impl.MetaGenerator;
import com.ctrip.framework.drc.console.service.monitor.impl.ConsistencyConsistencyMonitorServiceImpl;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static com.ctrip.framework.drc.console.monitor.delay.config.MonitorTableSourceProvider.SWITCH_STATUS_ON;

@Component
public class UnitVerificationManager extends AbstractMonitor {

    @Autowired
    private MetaGenerator metaService;

    @Autowired
    private MetaInfoServiceImpl metaInfoService;

    @Autowired
    private ConsistencyConsistencyMonitorServiceImpl monitorService;

    @Autowired
    private MonitorTableSourceProvider monitorTableSourceProvider;

    protected List<Long> verifyingGroupIds = new ArrayList<>();

    private static final int INITIAL_DELAY = 50;

    private static final int PERIOD = 10;

    @Override
    public void initialize() {
        super.initialize();
        setInitialDelay(INITIAL_DELAY);
        setPeriod(PERIOD);
    }

    @Override
    public void scheduledTask() {
        try {
            if (SWITCH_STATUS_ON.equalsIgnoreCase(monitorTableSourceProvider.getUnitVericationManagerSwitch())) {
                List<MhaGroupTbl> mhaGroupTbls = metaService.getMhaGroupTbls();
                if (null != mhaGroupTbls) {
                    List<Long> shouldVerifyGroupIds = mhaGroupTbls.stream().filter(p -> BooleanEnum.TRUE.getCode().equals(p.getUnitVerificationSwitch())).map(MhaGroupTbl::getId).collect(Collectors.toList());
                    List<Long> toBeAdded = shouldVerifyGroupIds.stream().filter(p -> !verifyingGroupIds.contains(p)).collect(Collectors.toList());
                    List<Long> toBeDeleted = verifyingGroupIds.stream().filter(p -> !shouldVerifyGroupIds.contains(p)).collect(Collectors.toList());
                    logger.info("[Unit] verifying: {}, shouldVerify: {}, toBeAdded: {}, toBeDeleted: {}", verifyingGroupIds, shouldVerifyGroupIds, toBeAdded, toBeDeleted);
                    for (Long id : toBeAdded) {
                        try {
                            List<String> mhaNames = metaInfoService.getMhaTbls(id).stream().map(MhaTbl::getMhaName).collect(Collectors.toList());
                            if (monitorService.addUnitVerification(id)) {
                                logger.info("[Unit] added mhaGroup({}): {}", id, mhaNames);
                                verifyingGroupIds.add(id);
                            }
                        } catch (SQLException e) {
                            logger.error("[Unit] fail add mhaGroup({})", id, e);
                        }
                    }
                    for (Long id : toBeDeleted) {
                        try {
                            List<String> mhaNames = metaInfoService.getMhaTbls(id).stream().map(MhaTbl::getMhaName).collect(Collectors.toList());
                            if (monitorService.deleteUnitVerification(id)) {
                                logger.info("[Unit] deleted mhaGroup({}): {}", id, mhaNames);
                                verifyingGroupIds.remove(id);
                            }
                        } catch (SQLException e) {
                            logger.error("[Unit] fail delete mhaGroup({})", id, e);
                        }
                    }
                }
            }
        } catch (Exception e) {
            logger.warn("Fail execute task, try next round, ", e);
        }
    }
}
