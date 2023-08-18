package com.ctrip.framework.drc.console.service.v2;

import com.ctrip.framework.drc.console.config.DefaultConsoleConfig;
import com.ctrip.framework.drc.console.dao.DbTblDao;
import com.ctrip.framework.drc.console.dao.entity.DbTbl;
import com.ctrip.framework.drc.console.dao.entity.v2.MhaTblV2;
import com.ctrip.framework.drc.console.dao.v2.MhaDbMappingTblDao;
import com.ctrip.framework.drc.console.service.v2.impl.MhaDbMappingServiceImpl;
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by dengquanliang
 * 2023/8/14 11:40
 */
public class MhaDbMappingServiceTest {

    @InjectMocks
    private MhaDbMappingServiceImpl mhaDbMappingService;
    @Mock
    private MysqlServiceV2 mysqlServiceV2;
    @Mock
    private DefaultConsoleConfig defaultConsoleConfig;
    @Mock
    private DbTblDao dbTblDao;
    @Mock
    private MhaDbMappingTblDao mhaDbMappingTblDao;

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.openMocks(this);
        Mockito.when(mysqlServiceV2.queryTablesWithNameFilter(Mockito.anyString(), Mockito.anyString())).thenReturn(Lists.newArrayList("db1.table"));
        Mockito.when(dbTblDao.queryByDbNames(Mockito.anyList())).thenReturn(getDbTbls());
        Mockito.when(dbTblDao.batchInsert(Mockito.anyList())).thenReturn(new int[1]);
        Mockito.when( mhaDbMappingTblDao.queryByMhaId(Mockito.anyLong())).thenReturn(new ArrayList<>());
        Mockito.when(mhaDbMappingTblDao.batchInsert(Mockito.anyList())).thenReturn(new int[1]);

        Mockito.when(dbTblDao.queryByIds(Mockito.anyList())).thenReturn(getDbTbls());
    }

    @Test
    public void testBuildMhaDbMappings() throws Exception{
        List<MhaTblV2> mhaTblV2s = getMhaTblV2s();

        Mockito.when(defaultConsoleConfig.getVpcMhaNames()).thenReturn(new ArrayList<>());
        List<String> result = mhaDbMappingService.buildMhaDbMappings(mhaTblV2s.get(0), mhaTblV2s.get(1), "db1");
        Assert.assertEquals(result.size(), 1);

        Mockito.when(defaultConsoleConfig.getVpcMhaNames()).thenReturn(Lists.newArrayList("mha1", "mha2"));
        result = mhaDbMappingService.buildMhaDbMappings(mhaTblV2s.get(0), mhaTblV2s.get(1), "db1\\..*");
        Assert.assertEquals(result.size(), 1);

        Mockito.when(defaultConsoleConfig.getVpcMhaNames()).thenReturn(Lists.newArrayList("mha1"));
        result = mhaDbMappingService.buildMhaDbMappings(mhaTblV2s.get(0), mhaTblV2s.get(1), "db1\\..*");
        Assert.assertEquals(result.size(), 1);
    }

    private static List<DbTbl> getDbTbls() {
        List<DbTbl> tbls = new ArrayList<>();
        for (int i = 1; i <= 2; i++) {
            DbTbl tbl = new DbTbl();
            tbl.setDeleted(0);
            tbl.setDbName("db" + i);
            tbl.setId(Long.valueOf(i));
            tbls.add(tbl);
        }

        return tbls;
    }

    private static List<MhaTblV2> getMhaTblV2s() {
        List<MhaTblV2> tbls = new ArrayList<>();
        for (int i = 1; i <= 2; i++) {
            MhaTblV2 tbl = new MhaTblV2();
            tbl.setId(Long.valueOf(i));
            tbl.setMhaName("mha" + i);
            tbl.setDeleted(0);
            tbls.add(tbl);
        }

        return tbls;
    }
}
