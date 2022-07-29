package com.ctrip.framework.drc.console.schedule;

import com.ctrip.framework.drc.console.dao.ConflictLogDao;
import com.ctrip.framework.drc.console.dao.entity.ConflictLog;
import com.ctrip.framework.drc.console.ha.LeaderSwitchable;
import com.ctrip.framework.drc.console.monitor.delay.config.MonitorTableSourceProvider;
import com.ctrip.framework.drc.core.service.utils.Constants;
import com.ctrip.framework.drc.core.monitor.reporter.DefaultTransactionMonitorHolder;
import com.ctrip.framework.drc.core.server.utils.ThreadUtils;
import com.ctrip.platform.dal.dao.DalHints;
import com.ctrip.platform.dal.dao.DalQueryDao;
import com.ctrip.platform.dal.dao.DalRowMapper;
import com.ctrip.platform.dal.dao.StatementParameters;
import com.ctrip.platform.dal.dao.helper.DalDefaultJpaMapper;
import com.ctrip.platform.dal.dao.sqlbuilder.FreeSelectSqlBuilder;
import com.ctrip.xpipe.api.monitor.Task;
import com.ctrip.xpipe.lifecycle.AbstractLifecycle;
import org.apache.commons.lang.time.DateUtils;
import org.springframework.stereotype.Component;

import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Created by jixinwang on 2020/11/12
 */
@Component
public class ClearConflictLog extends AbstractLifecycle implements LeaderSwitchable {

    private final int BATCH_SIZE = 100;

    private ConflictLogDao conflictLogDao;

    private DalQueryDao queryDao;

    private MonitorTableSourceProvider configService;

    private ScheduledExecutorService conflictLogClearScheduledExecutorService;

    private DalRowMapper<ConflictLog> conflictLogDalRowMapper;

    private volatile boolean isRegionLeader = false;

    public ClearConflictLog(ConflictLogDao conflictLogDao, DalQueryDao queryDao, MonitorTableSourceProvider configService) throws SQLException {
        this.conflictLogDao = conflictLogDao;
        this.queryDao = queryDao;
        this.configService = configService;
        this.conflictLogClearScheduledExecutorService = ThreadUtils.newSingleThreadScheduledExecutor("-scheduledExecutorService");
        this.conflictLogDalRowMapper = new DalDefaultJpaMapper<>(ConflictLog.class);
        initSchedule();
    }

    protected void initSchedule() {
        final long initialDelay = Constants.sixty * Constants.sixty * Constants.twentyFour - DateUtils.getFragmentInSeconds(Calendar.getInstance(), Calendar.DATE);
        final long period = Constants.sixty * Constants.sixty * Constants.twentyFour;
        conflictLogClearScheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    if (isRegionLeader) {
                        final String conflictLogClearSwitch = configService.getConflictLogClearSwitch();
                        if ("on".equals(conflictLogClearSwitch)){
                            logger.info("[[task=clearConflict]is leader going to clear confilct log");
                            DefaultTransactionMonitorHolder.getInstance().logTransaction("Schedule", "ClearConflictLog", new Task() {
                                @Override
                                public void go() throws Exception {
                                    deleteConflictLog();
                                }
                            });
                            
                        } else {
                            logger.warn("[[task=clearConflict]] is leader but switch is off");
                        }
                    } else {
                        logger.info("[[task=clearConflict]]not a leader do nothing");
                    }
                    
                } catch (Throwable t) {
                    logger.info("[[task=clearConflict]] log error", t);
                }
            }

        }, initialDelay, period, TimeUnit.SECONDS);
    }

    public void deleteConflictLog() throws SQLException {
        int total = conflictLogDao.count();
        int batch = total / BATCH_SIZE;
        String timeStampCondition = getPastDate(configService.getConflictLogReserveDay());
        for (int i = 0; i < batch; i++) {
            String sql = "select * from conflict_log where datachange_lasttime < '" + timeStampCondition + "'" + " limit " + BATCH_SIZE;
            List<ConflictLog> conflictLogs = getBatchConflictLog(sql);
            conflictLogDao.delete(conflictLogs);
        }
    }

    public String getPastDate(int past) {
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
        Calendar c = Calendar.getInstance();
        c.setTime(new Date());
        c.add(Calendar.DATE, - past);
        Date d = c.getTime();
        String day = format.format(d);
        return (day + " 00:00:00");
    }

    public List<ConflictLog> getBatchConflictLog(String sql) throws SQLException {
        FreeSelectSqlBuilder<List<ConflictLog>> builder = new FreeSelectSqlBuilder<>();
        builder.setTemplate(sql);
        StatementParameters parameters = new StatementParameters();
        builder.mapWith(conflictLogDalRowMapper);
        return queryDao.query(builder, parameters, new DalHints());
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
        // nothing to do
    }

    @Override
    public void doSwitchToStop() throws Throwable {
        // nothing to do
    }
}
