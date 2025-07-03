package com.ctrip.framework.drc.console.controller.v2;

import com.ctrip.framework.drc.console.dto.v2.DbMigrationParam;
import com.ctrip.framework.drc.console.service.v2.dbmigration.DbMigrationService;
import com.ctrip.framework.drc.core.http.ApiResult;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.sql.SQLException;

/**
 * Created by shiruixin
 * 2025/5/15 16:01
 */
public class DbMigrationControllerTest {
    @InjectMocks
    private DbMigrationController dbMigrationController;
    @Mock
    private DbMigrationService dbMigrationServiceV2;

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.openMocks(this);
    }

    @Test
    public void testOverseaDbMigrationCheckAndInit() throws SQLException {
        Mockito.when(dbMigrationServiceV2.dbMigrationCheckAndCreateTask(Mockito.any(), Mockito.any())).thenReturn(Pair.of("tip", 1L));
        ApiResult result = dbMigrationController.overseaDbMigrationCheckAndInit(new DbMigrationParam());
        Assert.assertEquals(Integer.valueOf(0), result.getStatus());

        Mockito.when(dbMigrationServiceV2.dbMigrationCheckAndCreateTask(Mockito.any(), Mockito.any())).thenReturn(Pair.of("tip", null));
        result = dbMigrationController.overseaDbMigrationCheckAndInit(new DbMigrationParam());
        Assert.assertEquals(Integer.valueOf(2), result.getStatus());

        Mockito.when(dbMigrationServiceV2.dbMigrationCheckAndCreateTask(Mockito.any(), Mockito.any())).thenThrow(new SQLException());
        result = dbMigrationController.overseaDbMigrationCheckAndInit(new DbMigrationParam());
        Assert.assertEquals(Integer.valueOf(1), result.getStatus());
    }

    @Test
    public void testPreStartOverseaDbMigrationTask() throws SQLException {
        Mockito.when(dbMigrationServiceV2.preStartDbMigrationTask(Mockito.any(), Mockito.any())).thenReturn(true);
        ApiResult result = dbMigrationController.preStartOverseaDbMigrationTask(1L);
        Assert.assertEquals(Integer.valueOf(0), result.getStatus());

        Mockito.when(dbMigrationServiceV2.preStartDbMigrationTask(Mockito.any(), Mockito.any())).thenReturn(false);
        result = dbMigrationController.preStartOverseaDbMigrationTask(1L);
        Assert.assertEquals(Integer.valueOf(1), result.getStatus());

        Mockito.when(dbMigrationServiceV2.preStartDbMigrationTask(Mockito.any(), Mockito.any())).thenThrow(new SQLException());
        result = dbMigrationController.preStartOverseaDbMigrationTask(1L);
        Assert.assertEquals(Integer.valueOf(1), result.getStatus());
    }

    @Test
    public void testStartOverseaDbMigrationTaskStep1() throws SQLException {
        Mockito.when(dbMigrationServiceV2.startDbMigrationTask(Mockito.any(), Mockito.any())).thenReturn(true);
        ApiResult result = dbMigrationController.startShaToOversea(1L);
        Assert.assertEquals(Integer.valueOf(0), result.getStatus());

        Mockito.when(dbMigrationServiceV2.startDbMigrationTask(Mockito.any(), Mockito.any())).thenReturn(false);
        result = dbMigrationController.startShaToOversea(1L);
        Assert.assertEquals(Integer.valueOf(1), result.getStatus());

        Mockito.when(dbMigrationServiceV2.startDbMigrationTask(Mockito.any(), Mockito.any())).thenThrow(new SQLException());
        result = dbMigrationController.startShaToOversea(1L);
        Assert.assertEquals(Integer.valueOf(1), result.getStatus());

    }

    @Test
    public void testStartOverseaDbMigrationTaskStep2() throws SQLException {
        Mockito.when(dbMigrationServiceV2.startDbMigrationTask(Mockito.any(), Mockito.any())).thenReturn(true);
        ApiResult result = dbMigrationController.startOverseaToSha(1L);
        Assert.assertEquals(Integer.valueOf(0), result.getStatus());

        Mockito.when(dbMigrationServiceV2.startDbMigrationTask(Mockito.any(), Mockito.any())).thenReturn(false);
        result = dbMigrationController.startOverseaToSha(1L);
        Assert.assertEquals(Integer.valueOf(1), result.getStatus());

        Mockito.when(dbMigrationServiceV2.startDbMigrationTask(Mockito.any(), Mockito.any())).thenThrow(new SQLException());
        result = dbMigrationController.startOverseaToSha(1L);
        Assert.assertEquals(Integer.valueOf(1), result.getStatus());
    }

    @Test
    public void testRefreshAndGetShaSyncOverseaStatus() {
        Mockito.when(dbMigrationServiceV2.getAndUpdateTaskStatus(Mockito.anyLong(), Mockito.anyBoolean(), Mockito.any())).thenReturn(Pair.of("tip", "status"));
        ApiResult result = dbMigrationController.refreshAndGetShaToOverseaStatus(1L);
        Assert.assertEquals(Integer.valueOf(0), result.getStatus());

        Mockito.when(dbMigrationServiceV2.getAndUpdateTaskStatus(Mockito.anyLong(), Mockito.anyBoolean(), Mockito.any())).thenReturn(Pair.of("tip", null));
        result = dbMigrationController.refreshAndGetShaToOverseaStatus(1L);
        Assert.assertEquals(Integer.valueOf(1), result.getStatus());
    }

    @Test
    public void testRefreshAndGetOverseaSyncShaStatus() {
        Mockito.when(dbMigrationServiceV2.getAndUpdateTaskStatus(Mockito.anyLong(), Mockito.anyBoolean(), Mockito.any())).thenReturn(Pair.of("tip", "status"));
        ApiResult result = dbMigrationController.refreshAndGetOverseaToShaStatus(1L);
        Assert.assertEquals(Integer.valueOf(0), result.getStatus());

        Mockito.when(dbMigrationServiceV2.getAndUpdateTaskStatus(Mockito.anyLong(), Mockito.anyBoolean(), Mockito.any())).thenReturn(Pair.of("tip", null));
        result = dbMigrationController.refreshAndGetOverseaToShaStatus(1L);
        Assert.assertEquals(Integer.valueOf(1), result.getStatus());
    }

    @Test
    public void testCheckPreStartStatus() throws SQLException {
        Mockito.when(dbMigrationServiceV2.checkPreStartStatus(Mockito.anyLong())).thenReturn(Pair.of(false, "PreStarting"));
        ApiResult result = dbMigrationController.checkPreStartStatus(1L);
        Assert.assertEquals(Integer.valueOf(0), result.getStatus());
        Assert.assertEquals("notReady", result.getData());
    }

    @Test
    public void testOverseaDbMigrationTaskPreStartStatus() throws SQLException  {
        Mockito.when(dbMigrationServiceV2.checkPreStartStatus(Mockito.anyLong())).thenReturn(Pair.of(true, "PreStarted"));
        ApiResult result = dbMigrationController.overseaDbMigrationTaskPreStartStatus(1L);
        Assert.assertEquals(Integer.valueOf(0), result.getStatus());
        Assert.assertEquals("PreStarted", result.getData());

        Mockito.when(dbMigrationServiceV2.checkPreStartStatus(Mockito.anyLong())).thenThrow(new SQLException());
        result = dbMigrationController.overseaDbMigrationTaskPreStartStatus(1L);
        Assert.assertEquals(Integer.valueOf(1), result.getStatus());
    }

    @Test
    public void testStartDbMigrationTaskTestEnv() throws Exception {
        Mockito.when(dbMigrationServiceV2.preStartDbMigrationTask(Mockito.anyLong(), Mockito.any())).thenReturn( true);
        Mockito.when(dbMigrationServiceV2.startDbMigrationTask(Mockito.anyLong(), Mockito.any())).thenReturn(true);
        ApiResult result = dbMigrationController.startDbMigrationTaskTestEnv(1L);
        Assert.assertEquals(Integer.valueOf(0), result.getStatus());
    }

    @Test
    public void testDbMigrationCheckAndInitTestEnv() throws Exception {
        Mockito.when(dbMigrationServiceV2.dbMigrationCheckAndCreateTask(Mockito.any(), Mockito.any())).thenReturn(Pair.of("tip", 1L));
        ApiResult result = dbMigrationController.dbMigrationCheckAndInitTestEnv(new DbMigrationParam());
        Assert.assertEquals(Integer.valueOf(0), result.getStatus());
    }
}