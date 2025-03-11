package com.ctrip.framework.drc.console.service.v2.impl;

import com.ctrip.framework.drc.console.param.v2.MqReplicationQuery;
import com.ctrip.framework.drc.console.service.v2.PojoBuilder;
import com.ctrip.framework.drc.console.service.v2.external.dba.DbaApiService;
import com.ctrip.framework.drc.console.vo.display.v2.DbReplicationVo;
import com.ctrip.framework.drc.console.vo.request.MqReplicationQueryDto;
import com.ctrip.framework.drc.core.http.PageResult;
import com.ctrip.framework.drc.core.service.dal.DbClusterApiService;
import com.ctrip.framework.drc.core.service.ops.OPSApiService;
import com.ctrip.framework.drc.core.service.user.IAMService;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.when;

/**
 * Created by shiruixin
 * 2024/8/27 11:48
 */
public class DbReplicationServiceImplTest extends CommonDataInit{
    public static final String VPC_MHA_NAME = "vpcMha1";
    @Mock
    DbClusterApiService dbClusterService;
    @Mock
    OPSApiService opsApiServiceImpl;
    @Mock
    MhaServiceV2Impl mhaServiceV2;
    @Mock
    MhaDbReplicationServiceImpl mhaDbReplicationService;
    @Mock
    DbaApiService dbaApiService;
    @Mock
    IAMService iamService;

    @Before
    public void setUp() throws IOException, SQLException {
        MockitoAnnotations.openMocks(this);
        when(qConfigService.updateDalClusterMqConfig(anyString(),any(),any(),anyList())).thenReturn(true);
        when(qConfigService.removeDalClusterMqConfigIfNecessary(anyString(),any(),any(),any(),anyList(),anyList())).thenReturn(true);
        when(qConfigService.addOrUpdateDalClusterMqConfig(anyString(),any(),any(),any(),anyList())).thenReturn(true);
        when(defaultConsoleConfig.getConsoleMqPanelUrl()).thenReturn("");
        doNothing().when(mhaDbReplicationService).maintainMhaDbReplication(Mockito.anyList());
        super.setUp();
    }

    @Test
    public void testQueryMqReplicationsByPage() throws Exception {
        String dbNames = "db";
        String srcTblName = "test";
        String topic = "test";
        boolean queryOtter = true;
        MqReplicationQueryDto dto = MqReplicationQueryDto.from(dbNames,srcTblName,topic,queryOtter);

        Mockito.when(dbaApiService.getDBsWithQueryPermission()).thenReturn(List.of("db"));
        Mockito.when(dbTblDao.queryByDbNames(anyList())).thenReturn(PojoBuilder.getDbTbls());
        Mockito.when(mhaDbMappingTblDao.queryByDbIds(anyList())).thenReturn(PojoBuilder.getMhaDbMappingTbls1());
        Mockito.when(mhaDbMappingTblDao.queryByIds(anyList())).thenReturn(PojoBuilder.getMhaDbMappingTbls1());
        Mockito.when(dcTblDao.queryAll()).thenReturn(PojoBuilder.getDcTbls());
        Mockito.when(dbTblDao.queryByIds(anyList())).thenReturn(PojoBuilder.getDbTbls());
        Mockito.when(mhaTblV2Dao.queryByIds(anyList())).thenReturn(PojoBuilder.getMhaTblV2s());
        Mockito.when(dbReplicationTblDao.queryByPage(any())).thenReturn(PojoBuilder.getDbReplicationTbls());
        Mockito.when(dbReplicationTblDao.count((MqReplicationQuery) any())).thenReturn(2);
        Mockito.when(iamService.canQueryAllDbReplication()).thenReturn(Pair.of(false, null));

        PageResult<DbReplicationVo> result = dbReplicationService.queryMqReplicationsByPage(dto);
        Assert.assertNotNull(result);
        Assert.assertEquals(result.getTotalCount(), 2);
    }

    @Test
    public void testQueryByPage() throws Exception {
        Mockito.when(dbReplicationTblDao.queryByPage(any())).thenReturn(PojoBuilder.getDbReplicationTbls());
        Mockito.when(dbReplicationTblDao.count((MqReplicationQuery) any())).thenReturn(2);

        PageResult result = dbReplicationService.queryByPage(new MqReplicationQuery());
        Assert.assertNotNull(result);
        Assert.assertEquals(result.getTotalCount(),2);
    }

}
