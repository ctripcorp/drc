package com.ctrip.framework.drc.console.monitor.task;

import com.ctrip.framework.drc.console.config.DefaultConsoleConfig;
import com.ctrip.framework.drc.console.dao.DbTblDao;
import com.ctrip.framework.drc.console.dao.v2.DbReplicationTblDao;
import com.ctrip.framework.drc.console.dao.v2.MhaDbMappingTblDao;
import com.ctrip.framework.drc.console.dao.v2.MhaReplicationTblDao;
import com.ctrip.framework.drc.console.dao.v2.MhaTblV2Dao;
import com.ctrip.framework.drc.console.param.mysql.DbFilterReq;
import com.ctrip.framework.drc.console.service.v2.MysqlServiceV2;
import com.ctrip.framework.drc.console.service.v2.PojoBuilder;
import com.ctrip.framework.drc.core.monitor.reporter.Reporter;
import com.google.common.collect.Sets;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Created by dengquanliang
 * 2023/12/21 19:18
 */
public class TableStructureCheckTaskTest {

    @InjectMocks
    private TableStructureCheckTask task;

    @Mock
    private DefaultConsoleConfig consoleConfig;
    @Mock
    private MhaTblV2Dao mhaTblV2Dao;
    @Mock
    private DbReplicationTblDao dbReplicationTblDao;
    @Mock
    private MhaDbMappingTblDao mhaDbMappingTblDao;
    @Mock
    private DbTblDao dbTblDao;
    @Mock
    private MysqlServiceV2 mysqlServiceV2;
    @Mock
    private MhaReplicationTblDao mhaReplicationTblDao;
    @Mock
    private Reporter reporter;

    private static final String TABLE_STRUCTURE_MEASUREMENT = "drc.table.structure";
    private static final String TABLE_COLUMN_STRUCTURE_MEASUREMENT = "drc.table.column.structure";

    @Before
    public void setUp(){
        MockitoAnnotations.openMocks(this);
    }


    @Test
    public void testScheduledTask() throws SQLException, InterruptedException {
        Mockito.when(consoleConfig.getTableStructureCheckSwitch()).thenReturn(false);
        Mockito.when(consoleConfig.isCenterRegion()).thenReturn(true);
        task.setRegionLeader(true);
        task.scheduledTask();
        Mockito.verify(reporter,Mockito.times(0)).removeRegister(Mockito.anyString());
        
        
        Mockito.when(mhaReplicationTblDao.queryAllExist()).thenReturn(PojoBuilder.getMhaReplicationTbls1());
        Mockito.when(mhaTblV2Dao.queryAllExist()).thenReturn(PojoBuilder.getMhaTblV2s());
        Mockito.when(dbReplicationTblDao.queryAllExist()).thenReturn(PojoBuilder.getDbReplicationTbls());
        Mockito.when(dbTblDao.queryAllExist()).thenReturn(PojoBuilder.getDbTbls());
        Mockito.when(mhaDbMappingTblDao.queryAllExist()).thenReturn(PojoBuilder.getMhaDbMappingTbls2());

        Map<String, Set<String>> map = new HashMap<>();
        map.put("db.table", Sets.newHashSet("col1"));
        Mockito.when(mysqlServiceV2.getTableColumns(Mockito.any())).thenReturn(map);
        Mockito.doNothing().when(reporter).resetReportCounter(Mockito.anyMap(), Mockito.anyLong(), Mockito.anyString());

        task.checkTableStructure();
        Thread.sleep(200);
        Mockito.verify(reporter, Mockito.never()).resetReportCounter(Mockito.anyMap(), Mockito.anyLong(), Mockito.anyString());

        Map<String, Set<String>> map1 = new HashMap<>();
        map1.put("db.table", Sets.newHashSet("col1", "col2"));
        Mockito.when(mysqlServiceV2.getTableColumns(new DbFilterReq("mha200", "db200\\.table1"))).thenReturn(map1);

        task.checkTableStructure();
        Thread.sleep(200);
        Mockito.verify(reporter, Mockito.times(1)).resetReportCounter(Mockito.anyMap(), Mockito.anyLong(), Mockito.eq(TABLE_COLUMN_STRUCTURE_MEASUREMENT));

        map1.put("db.table1", Sets.newHashSet("col1", "col2"));
        task.checkTableStructure();
        Thread.sleep(200);
        Mockito.verify(reporter, Mockito.times(2)).resetReportCounter(Mockito.anyMap(), Mockito.anyLong(), Mockito.eq(TABLE_COLUMN_STRUCTURE_MEASUREMENT));
        Mockito.verify(reporter, Mockito.times(1)).resetReportCounter(Mockito.anyMap(), Mockito.anyLong(), Mockito.eq(TABLE_STRUCTURE_MEASUREMENT));
    }

}
