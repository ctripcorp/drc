package com.ctrip.framework.drc.console.service.v2.dbmigration;


import com.ctrip.framework.drc.console.config.DefaultConsoleConfig;
import com.ctrip.framework.drc.console.dao.entity.v2.MigrationTaskTbl;
import com.ctrip.framework.drc.console.dao.v2.MigrationTaskTblDao;
import com.ctrip.framework.drc.console.enums.MigrationStatusEnum;
import com.ctrip.framework.drc.console.service.v2.MhaServiceV2;
import com.ctrip.framework.drc.console.service.v2.MockEntityBuilder;
import java.util.HashMap;
import org.assertj.core.util.Lists;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

public class MigrationTaskManagerTest {
    
    @InjectMocks
    private MigrationTaskManager migrationTaskManager;

    @Mock
    private MigrationTaskTblDao migrationTaskTblDao;
    @Mock
    private MhaServiceV2 mhaServiceV2;
    @Mock
    private DefaultConsoleConfig consoleConfig;
    
    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.openMocks(this);
    }

    @Test
    public void testScheduledTask() throws Throwable {
        Mockito.when(consoleConfig.isCenterRegion()).thenReturn(true);
        MigrationTaskTbl task1 = MockEntityBuilder.buildMigrationTaskTbl(1L, "oldmha1", "newmha1",
                "[\"db1\"]", "operator");
        MigrationTaskTbl task2 = MockEntityBuilder.buildMigrationTaskTbl(2L, "oldmha2", "newmha2", "[\"db2\"]", "operator");
        MigrationTaskTbl task3 = MockEntityBuilder.buildMigrationTaskTbl(3L, "oldmha3", "newmha3", "[\"db3\"]", "operator");
        task1.setStatus(MigrationStatusEnum.PRE_STARTING.getStatus());
        task2.setStatus(MigrationStatusEnum.PRE_STARTING.getStatus());
        task3.setStatus(MigrationStatusEnum.PRE_STARTING.getStatus());
        Mockito.when(migrationTaskTblDao.queryByStatus(Mockito.eq(MigrationStatusEnum.PRE_STARTING.getStatus()))).thenReturn(
                Lists.newArrayList(task1, task2,task3));

        Mockito.when(mhaServiceV2.getMhaReplicatorSlaveDelay(Lists.newArrayList(task1.getNewMha(), task2.getNewMha(), task3.getNewMha())))
                .thenReturn(new HashMap<>() {{
                    put(task1.getNewMha(), 100L);
                    put(task2.getNewMha(), 100000L);
                }});
        
        Mockito.when(migrationTaskTblDao.batchUpdate(Mockito.eq(Lists.newArrayList(task1)))).thenReturn(new int[]{1});
        migrationTaskManager.isleader();
        migrationTaskManager.scheduledTask();
        Mockito.verify(migrationTaskTblDao, Mockito.times(2)).batchUpdate(Mockito.eq(Lists.newArrayList(task1)));
    }
}