package com.ctrip.framework.drc.console.monitor.task;

import com.ctrip.framework.drc.console.config.DefaultConsoleConfig;
import com.ctrip.framework.drc.console.dao.entity.v2.ApplicationApprovalTbl;
import com.ctrip.framework.drc.console.dao.entity.v2.ApplicationFormTbl;
import com.ctrip.framework.drc.console.dao.v2.ApplicationApprovalTblDao;
import com.ctrip.framework.drc.console.dao.v2.ApplicationFormTblDao;
import com.ctrip.framework.drc.console.enums.ApprovalResultEnum;
import com.ctrip.framework.drc.console.enums.BooleanEnum;
import com.ctrip.framework.drc.console.monitor.AbstractLeaderAwareMonitor;
import com.ctrip.framework.drc.console.service.v2.DrcApplicationService;
import com.ctrip.framework.drc.core.monitor.reporter.DefaultReporterHolder;
import com.ctrip.framework.drc.core.monitor.reporter.Reporter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.ctrip.framework.drc.core.server.config.SystemConfig.CONSOLE_MONITOR_LOGGER;

/**
 * Created by dengquanliang
 * 2024/2/28 20:01
 */
@Component
@Order(3)
public class DrcApplicationFormCheckTask extends AbstractLeaderAwareMonitor {

    @Autowired
    private ApplicationFormTblDao applicationFormTblDao;
    @Autowired
    private ApplicationApprovalTblDao applicationApprovalTblDao;
    @Autowired
    private DefaultConsoleConfig consoleConfig;
    @Autowired
    private DrcApplicationService drcApplicationService;

    private Reporter reporter = DefaultReporterHolder.getInstance();
    private static final String DRC_CONFIG_APPLICATION_MEASUREMENT = "drc.config.application";
    private static final long TEN_MINUTE = 10 * 60 * 1000L;

    @Override
    public void initialize() {
        setInitialDelay(1);
        setPeriod(5);
        setTimeUnit(TimeUnit.MINUTES);
        super.initialize();
    }

    @Override
    public void scheduledTask() throws Exception {
        if (!isRegionLeader || !consoleConfig.isCenterRegion()) {
            return;
        }
        if (!consoleConfig.getDrcConfigApplicationSwitch()) {
            CONSOLE_MONITOR_LOGGER.info("[[monitor=DrcApplicationFormCheckTask]] is leader, but switch is off");
            return;
        }
        CONSOLE_MONITOR_LOGGER.info("[[monitor=DrcApplicationFormCheckTask]] is leader, going to check");
        check();
    }

    protected void check() throws Exception {
        List<ApplicationFormTbl> applicationFormTbls = applicationFormTblDao.queryByIsSentEmail(BooleanEnum.FALSE.getCode());
        if (CollectionUtils.isEmpty(applicationFormTbls)) {
            return;
        }
        List<ApplicationApprovalTbl> applicationApprovalTbls = applicationApprovalTblDao.queryByApplicationFormIds(applicationFormTbls.stream().map(ApplicationFormTbl::getId).collect(Collectors.toList()), ApprovalResultEnum.APPROVED.getCode());
        if (CollectionUtils.isEmpty(applicationApprovalTbls)) {
            return;
        }
        List<Long> applicationFormIds = applicationApprovalTbls.stream().map(ApplicationApprovalTbl::getApplicationFormId).collect(Collectors.toList());
        applicationFormTbls = applicationFormTbls.stream().filter(e -> applicationFormIds.contains(e.getId())).collect(Collectors.toList());

        for (ApplicationFormTbl applicationFormTbl : applicationFormTbls) {
            CONSOLE_MONITOR_LOGGER.info("[[monitor=DrcApplicationFormCheckTask]] check applicationFormId: {}", applicationFormTbl.getId());
            boolean result = drcApplicationService.sendEmail(applicationFormTbl.getId());
            if (result) {
                CONSOLE_MONITOR_LOGGER.info("[[monitor=DrcApplicationFormCheckTask]] send email success, applicationFormId: {}", applicationFormTbl.getId());
            } else {
                CONSOLE_MONITOR_LOGGER.info("[[monitor=DrcApplicationFormCheckTask]] send email fail, applicationFormId: {}", applicationFormTbl.getId());
                long currentTime = System.currentTimeMillis();
                long diffTime = currentTime - applicationFormTbl.getDatachangeLasttime().getTime();
                if (diffTime >= TEN_MINUTE) {
                    CONSOLE_MONITOR_LOGGER.info("[[monitor=DrcApplicationFormCheckTask]] not send email for 10 minute, applicationFormId: {}", applicationFormTbl.getId());
                    Map<String, String> tags = new HashMap<>();
                    tags.put("dbName", applicationFormTbl.getDbName());
                    tags.put("srcRegion", applicationFormTbl.getSrcRegion());
                    tags.put("dstRegion", applicationFormTbl.getDstRegion());
                    reporter.reportResetCounter(tags, 1L, DRC_CONFIG_APPLICATION_MEASUREMENT);
                }
            }
        }
    }
}
