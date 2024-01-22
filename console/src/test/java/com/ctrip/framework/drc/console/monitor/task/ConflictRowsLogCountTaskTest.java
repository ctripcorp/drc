package com.ctrip.framework.drc.console.monitor.task;

import com.ctrip.framework.drc.console.config.DefaultConsoleConfig;
import com.ctrip.framework.drc.console.config.DomainConfig;
import com.ctrip.framework.drc.console.dao.DbTblDao;
import com.ctrip.framework.drc.console.dao.log.ConflictRowsLogTblDao;
import com.ctrip.framework.drc.console.dao.log.ConflictTrxLogTblDao;
import com.ctrip.framework.drc.console.dao.log.entity.ConflictRowsLogCount;
import com.ctrip.framework.drc.console.dao.log.entity.ConflictRowsLogTbl;
import com.ctrip.framework.drc.console.dao.log.entity.ConflictTrxLogTbl;
import com.ctrip.framework.drc.console.service.log.ConflictLogService;
import com.ctrip.framework.drc.console.service.v2.PojoBuilder;
import com.ctrip.framework.drc.console.vo.log.ConflictRowsLogCountView;
import com.ctrip.framework.drc.core.monitor.reporter.Reporter;
import com.ctrip.framework.drc.core.service.email.Email;
import com.ctrip.framework.drc.core.service.email.EmailResponse;
import com.ctrip.framework.drc.core.service.email.EmailService;
import org.assertj.core.util.Lists;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.util.List;

/**
 * Created by dengquanliang
 * 2023/12/26 18:03
 */
public class ConflictRowsLogCountTaskTest {

    @InjectMocks
    private ConflictRowsLogCountTask task;
    @Mock
    private ConflictLogService conflictLogService;
    @Mock
    private Reporter reporter;
    @Mock
    private DefaultConsoleConfig consoleConfig;
    @Mock
    private DbTblDao dbTblDao;
    @Mock
    private DomainConfig domainConfig;
    @Mock
    private EmailService emailService;
    @Mock
    private ConflictRowsLogTblDao conflictRowsLogTblDao;
    @Mock
    private ConflictTrxLogTblDao conflictTrxLogTblDao;

    private static final String ROW_LOG_COUNT_MEASUREMENT = "row.log.count";
    private static final String ROW_LOG_DB_COUNT_MEASUREMENT = "row.log.db.count";
    private static final String ROW_LOG_DB_COUNT_ROLLBACK_MEASUREMENT = "row.log.db.rollback.count";
    private static final String ROW_LOG_COUNT_QUERY_TIME_MEASUREMENT = "row.log.count.query.time";

    @Before
    public void setUp() {
        MockitoAnnotations.openMocks(this);
    }

    @Test
    public void testScheduledTask() throws Throwable {
        ConflictRowsLogCountView view = new ConflictRowsLogCountView();
        view.setTotalCount(1);
        view.setRollBackTotalCount(1);
        view.setDbCounts(Lists.newArrayList(new ConflictRowsLogCount("db", "table", 1L, 1L, 1)));
        view.setRollBackDbCounts(Lists.newArrayList(new ConflictRowsLogCount("db", "table", 1L, 1L, 1)));

        Mockito.when(conflictLogService.getRowsLogCountView(Mockito.anyLong(), Mockito.anyLong())).thenReturn(view);
        Mockito.when(consoleConfig.isCenterRegion()).thenReturn(false);
        Mockito.when(domainConfig.getConflictAlarmTopNum()).thenReturn(10);
        Mockito.when(domainConfig.getConflictAlarmRollbackTopNum()).thenReturn(10);

        task.initialize();
        task.setNextDay();
        task.checkCount();
        Mockito.verify(reporter, Mockito.times(2)).resetReportCounter(Mockito.anyMap(), Mockito.anyLong(), Mockito.eq(ROW_LOG_COUNT_MEASUREMENT));
        Mockito.verify(reporter, Mockito.times(1)).resetReportCounter(Mockito.anyMap(), Mockito.anyLong(), Mockito.eq(ROW_LOG_DB_COUNT_MEASUREMENT));
        Mockito.verify(reporter, Mockito.times(1)).resetReportCounter(Mockito.anyMap(), Mockito.anyLong(), Mockito.eq(ROW_LOG_DB_COUNT_ROLLBACK_MEASUREMENT));
        Mockito.verify(reporter, Mockito.times(1)).resetReportCounter(Mockito.anyMap(), Mockito.anyLong(), Mockito.eq(ROW_LOG_COUNT_QUERY_TIME_MEASUREMENT));


        Mockito.when(domainConfig.getConflictAlarmSendEmailSwitch()).thenReturn(true);
        Mockito.when(domainConfig.getConflictAlarmSendTimeHour()).thenReturn(0);
        Mockito.when(emailService.sendEmail(Mockito.any(Email.class))).thenReturn(new EmailResponse());
        Mockito.when(dbTblDao.queryByDbNames(Mockito.anyList())).thenReturn(PojoBuilder.getDbTbls());
        Mockito.when(domainConfig.getConflictAlarmSendDBOwnerSwitch()).thenReturn(true);
        Mockito.when(conflictRowsLogTblDao.queryByIds(Mockito.anyList())).thenReturn(buildConflictRowsLogTbls());
        Mockito.when(conflictTrxLogTblDao.queryByIds(Mockito.anyList())).thenReturn(buildConflictTrxLogTbls());

        task.alarm();
        Mockito.verify(emailService, Mockito.times(2)).sendEmail(Mockito.any(Email.class));

    }

    private List<ConflictTrxLogTbl> buildConflictTrxLogTbls() {
        ConflictTrxLogTbl tbl = new ConflictTrxLogTbl();
        tbl.setId(1L);
        tbl.setTrxResult(0);
        tbl.setSrcMhaName("srcMha");
        tbl.setDstMhaName("dstMha");
        tbl.setGtid("gtid");
        tbl.setTrxRowsNum(1L);
        tbl.setCflRowsNum(1L);
        tbl.setHandleTime(System.currentTimeMillis());
        return com.google.common.collect.Lists.newArrayList(tbl);
    }

    private List<ConflictRowsLogTbl> buildConflictRowsLogTbls() {
        ConflictRowsLogTbl tbl = new ConflictRowsLogTbl();
        tbl.setId(1L);
        tbl.setConflictTrxLogId(1L);
        tbl.setDbName("dbName");
        tbl.setTableName("tableName");
        tbl.setRawSql("rawSql");
        tbl.setRawSqlResult("rawSqlResult");
        tbl.setDstRowRecord("dstRowRecord");
        tbl.setHandleSql("handleSql");
        tbl.setHandleSqlResult("handleSqlResult");
        tbl.setRowResult(0);
        tbl.setHandleTime(System.currentTimeMillis());
        tbl.setRowId(1L);

        return com.google.common.collect.Lists.newArrayList(tbl);
    }
}