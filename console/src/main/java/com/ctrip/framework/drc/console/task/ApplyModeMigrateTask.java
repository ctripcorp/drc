package com.ctrip.framework.drc.console.task;

import com.ctrip.framework.drc.console.dao.ApplierGroupTblDao;
import com.ctrip.framework.drc.console.dao.MhaTblDao;
import com.ctrip.framework.drc.console.dao.entity.ApplierGroupTbl;
import com.ctrip.framework.drc.console.dao.entity.MhaTbl;
import com.ctrip.framework.drc.console.enums.BooleanEnum;
import com.ctrip.framework.drc.console.monitor.AbstractMonitor;
import com.ctrip.framework.drc.console.monitor.delay.config.MonitorTableSourceProvider;
import com.ctrip.framework.drc.console.utils.DalUtils;
import com.ctrip.xpipe.utils.VisibleForTesting;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;


import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.ctrip.framework.drc.console.monitor.delay.config.MonitorTableSourceProvider.SWITCH_STATUS_ON;

/**
 * @ClassName ApplyModeMigrationTask
 * @Author haodongPan
 * @Date 2022/7/18 11:06
 * @Version: $
 */
@Component
public class ApplyModeMigrateTask extends AbstractMonitor  {
    
    private final int  MIGRATE_INITIAL_DELAY = 0;
    private final int MIGRATE_PERIOD = 10;
    private final TimeUnit MIGRATE_TIME_UNIT = TimeUnit.MINUTES;
    
    private DalUtils dalUtils = DalUtils.getInstance();
    private MhaTblDao mhaTblDao = dalUtils.getMhaTblDao();
    private ApplierGroupTblDao applierGroupTblDao = dalUtils.getApplierGroupTblDao();

    @Autowired
    private MonitorTableSourceProvider monitorTableSourceProvider;
    

    @Override
    public void scheduledTask() {
        String applyModeMigrateSwitch = monitorTableSourceProvider.getApplyModeMigrateSwitch();
        if (SWITCH_STATUS_ON.equalsIgnoreCase(applyModeMigrateSwitch)) {
            try {
                migrate();
            } catch (SQLException e) {
                logger.error("[[task=ApplyModeMigrateTask]] sql error",e);
            }
        }
    }
    
   
    private void migrate() throws SQLException {
        List<MhaTbl> mhaTbls = mhaTblDao.queryByDeleted(BooleanEnum.FALSE.getCode());
        for (MhaTbl mhaTbl : mhaTbls) {
            Integer applyMode = null;
            boolean consistent = true;
            List<ApplierGroupTbl> applierGroupTbls = applierGroupTblDao.queryById(mhaTbl.getId(), BooleanEnum.FALSE.getCode());
            if (!CollectionUtils.isEmpty(applierGroupTbls)) {
                for (ApplierGroupTbl applierGroupTbl : applierGroupTbls) {
                    if (applyMode == null) {
                        applyMode = applierGroupTbl.getApplyMode();
                    } else {
                        if (!applyMode.equals(applierGroupTbl.getApplyMode())) {
                            logger.error("[[task=ApplyModeMigrateTask]] applyMode inconsistent,applierGroupId:{}", applierGroupTbl.getId());
                            consistent = false;
                        }
                    }
                }
            }
            if (consistent) {
                mhaTbl.setApplyMode(applyMode);
                mhaTblDao.update(mhaTbl);
                logger.info("[[task=ApplyModeMigrateTask]] mha:{} set applyMode to {}",mhaTbl.getMhaName(),applyMode);
            }
        }

    }
    
    @Override
    public int getDefaultInitialDelay() {
        return MIGRATE_INITIAL_DELAY;
    }

    @Override
    public int getDefaultPeriod(){
        return MIGRATE_PERIOD;
    }

    @Override
    public TimeUnit getDefaultTimeUnit() {
        return MIGRATE_TIME_UNIT;
    }
    
}
