package com.ctrip.framework.drc.console.monitor.task;

import com.ctrip.framework.drc.console.config.DefaultConsoleConfig;
import com.ctrip.framework.drc.console.dao.log.entity.ConflictRowsLogCount;
import com.ctrip.framework.drc.console.service.log.ConflictLogService;
import com.ctrip.framework.drc.console.utils.ConsoleExceptionUtils;
import com.ctrip.framework.drc.console.vo.log.ConflictRowsLogCountView;
import com.ctrip.framework.drc.core.monitor.reporter.Reporter;
import org.assertj.core.util.Lists;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

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
    private ConflictRowsLogCountTask task1;

    private static final String ROW_LOG_COUNT_MEASUREMENT = "row.log.count";
    private static final String ROW_LOG_DB_COUNT_MEASUREMENT = "row.log.db.count";
    private static final String ROW_LOG_DB_COUNT_ROLLBACK_MEASUREMENT = "row.log.db.rollback.count";
    private static final String ROW_LOG_COUNT_QUERY_TIME_MEASUREMENT = "row.log.count.query.time";

    @Before
    public void setUp(){
        MockitoAnnotations.openMocks(this);
    }

    @Test
    public void testScheduledTask() throws Throwable {
        ConflictRowsLogCountView view = new ConflictRowsLogCountView();
        view.setTotalCount(1);
        view.setRollBackTotalCount(1);
        view.setDbCounts(Lists.newArrayList(new ConflictRowsLogCount("db", "table", 1)));
        view.setRollBackDbCounts(Lists.newArrayList(new ConflictRowsLogCount("db", "table", 1)));

        Mockito.when(conflictLogService.getRowsLogCountView(Mockito.anyLong(), Mockito.anyLong())).thenReturn(view);
        Mockito.when(consoleConfig.isCenterRegion()).thenReturn(false);
//        task.isleader();

        task.initialize();
        task.checkCount();
        Mockito.verify(reporter, Mockito.times(2)).resetReportCounter(Mockito.anyMap(), Mockito.anyLong(), Mockito.eq(ROW_LOG_COUNT_MEASUREMENT));
        Mockito.verify(reporter, Mockito.times(1)).resetReportCounter(Mockito.anyMap(), Mockito.anyLong(), Mockito.eq(ROW_LOG_DB_COUNT_MEASUREMENT));
        Mockito.verify(reporter, Mockito.times(1)).resetReportCounter(Mockito.anyMap(), Mockito.anyLong(), Mockito.eq(ROW_LOG_DB_COUNT_ROLLBACK_MEASUREMENT));
        Mockito.verify(reporter, Mockito.times(1)).resetReportCounter(Mockito.anyMap(), Mockito.anyLong(), Mockito.eq(ROW_LOG_COUNT_QUERY_TIME_MEASUREMENT));

        task.isleader();
        Mockito.when(consoleConfig.isCenterRegion()).thenReturn(true);
        Mockito.doThrow(ConsoleExceptionUtils.message("error")).when(task1).checkCount();
        task.scheduledTask();
        Mockito.verify(reporter, Mockito.times(1)).removeRegister(Mockito.eq(ROW_LOG_DB_COUNT_MEASUREMENT));
        Mockito.verify(reporter, Mockito.times(1)).removeRegister(Mockito.eq(ROW_LOG_DB_COUNT_ROLLBACK_MEASUREMENT));
    }
}