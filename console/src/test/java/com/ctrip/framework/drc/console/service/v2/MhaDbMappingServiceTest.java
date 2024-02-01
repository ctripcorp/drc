package com.ctrip.framework.drc.console.service.v2;

import com.ctrip.framework.drc.console.config.DefaultConsoleConfig;
import com.ctrip.framework.drc.console.dao.DbTblDao;
import com.ctrip.framework.drc.console.dao.entity.DbTbl;
import com.ctrip.framework.drc.console.dao.entity.v2.MhaDbMappingTbl;
import com.ctrip.framework.drc.console.dao.entity.v2.MhaTblV2;
import com.ctrip.framework.drc.console.dao.v2.MhaDbMappingTblDao;
import com.ctrip.framework.drc.console.dao.v2.MhaTblV2Dao;
import com.ctrip.framework.drc.console.pojo.domain.DcDo;
import com.ctrip.framework.drc.console.service.v2.external.dba.DbaApiService;
import com.ctrip.framework.drc.console.service.v2.external.dba.response.ClusterInfoDto;
import com.ctrip.framework.drc.console.service.v2.external.dba.response.DbClusterInfoDto;
import com.ctrip.framework.drc.console.service.v2.impl.MhaDbMappingServiceImpl;
import com.ctrip.framework.drc.console.vo.request.MhaDbQueryDto;
import com.ctrip.framework.drc.console.vo.v2.ConfigDbView;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.when;

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
    @Mock
    private MetaInfoServiceV2 metaInfoServiceV2;
    @Mock
    private MhaTblV2Dao mhaTblV2Dao;
    @Mock
    private DbaApiService dbaApiService;

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.openMocks(this);
        when(mysqlServiceV2.queryTablesWithNameFilter(Mockito.anyString(), Mockito.anyString())).thenReturn(Lists.newArrayList("db1.table"));
        when(dbTblDao.queryByDbNames(Mockito.anyList())).thenReturn(getDbTbls());
        when(dbTblDao.batchInsert(Mockito.anyList())).thenReturn(new int[1]);
        when( mhaDbMappingTblDao.queryByMhaId(Mockito.anyLong())).thenReturn(new ArrayList<>());
        when(mhaDbMappingTblDao.batchInsert(Mockito.anyList())).thenReturn(new int[1]);

        when(dbTblDao.queryByIds(Mockito.anyList())).thenReturn(getDbTbls());
    }

    @Test
    public void testInitMhaDbMappings() throws Exception{
        List<MhaTblV2> mhaTblV2s = getMhaTblV2s();

        when(defaultConsoleConfig.getVpcMhaNames()).thenReturn(new ArrayList<>());
        Pair<List<String>, List<String>> result = mhaDbMappingService.initMhaDbMappings(mhaTblV2s.get(0), mhaTblV2s.get(1), "db1");
        Assert.assertEquals(result.getLeft().size(), 1);
        Assert.assertEquals(result.getRight().size(), 1);

        when(defaultConsoleConfig.getVpcMhaNames()).thenReturn(Lists.newArrayList("mha1"));
        result = mhaDbMappingService.initMhaDbMappings(mhaTblV2s.get(0), mhaTblV2s.get(1), "db1\\..*");
        Assert.assertEquals(result.getLeft().size(), 1);
        Assert.assertEquals(result.getRight().size(), 1);
    }

    @Test
    public void testRemoveDuplicateDbTblWithoutMhaDbMapping() throws Exception {
        when(dbTblDao.queryAll()).thenReturn(getDbTblsWithDuplicate());
        
        // mock mhaDbMappingTblDao.queryByDbIds ,when dbId = 6, return 1 record, other retuen empty List
        when(mhaDbMappingTblDao.queryByDbIds(Mockito.anyList())).thenReturn(new ArrayList<>());
        MhaDbMappingTbl mhaDbMappingTbl = new MhaDbMappingTbl();
        mhaDbMappingTbl.setDbId(6L);
        when(mhaDbMappingTblDao.queryByDbIdsIgnoreDeleted(Mockito.eq(Lists.newArrayList(6L,7L)))).thenReturn(Lists.newArrayList(mhaDbMappingTbl));

        when(dbTblDao.batchDelete(Mockito.anyList())).thenReturn(new int[] {1,1});
        Pair<Integer, Integer> pair = mhaDbMappingService.removeDuplicateDbTblWithoutMhaDbMapping(true);
        Assert.assertEquals(pair.getLeft().intValue(), 2);
        Assert.assertEquals(pair.getRight().intValue(), 2);

    }

    @Test
    public void testQuery() throws SQLException {
        // empty condition
        MhaDbQueryDto query = new MhaDbQueryDto();
        List<MhaDbMappingTbl> lists = getMhaDbMappingTbls();
        when(mhaDbMappingTblDao.queryByDbIdsOrMhaIds(null,null)).thenReturn(Collections.emptyList());
        List<MhaDbMappingTbl> r1 = mhaDbMappingService.query(query);
        Assert.assertEquals(0,r1.size());

        // mha condition
        when(mhaDbMappingTblDao.queryByDbIdsOrMhaIds(anyList(), anyList())).thenReturn(lists);

        query = new MhaDbQueryDto();
        query.setRegionId(1L);
        DcDo dcDo = new DcDo();
        dcDo.setRegionId(1L);
        when(metaInfoServiceV2.queryAllDcWithCache()).thenReturn(Lists.newArrayList(dcDo));
        MhaTblV2 mhaTblV2 = new MhaTblV2();
        mhaTblV2.setId(1L);
        when(mhaTblV2Dao.query(any())).thenReturn(Lists.newArrayList(mhaTblV2));

        // db condition
        DbTbl dbTbl = new DbTbl();
        dbTbl.setId(111L);
        when(dbTblDao.queryByLikeDbNamesOrBuCode(anyString(),anyString())).thenReturn(Lists.newArrayList(dbTbl));
        query.setBuCode("buCode1");
        query.setDbName("db1");
        List<MhaDbMappingTbl> r3 = mhaDbMappingService.query(query);
        Assert.assertNotEquals(0,r3.size());

    }

    @Test
    public void testConfigEmailGroupForDb() throws Exception {
        Mockito.when(dbaApiService.getDatabaseClusterInfoList(Mockito.anyString())).thenReturn(Lists.newArrayList(new DbClusterInfoDto("db", new ArrayList<>())));
        Mockito.when(dbTblDao.queryByDbNames(Mockito.anyList())).thenReturn(getDbTbls());
        Mockito.when(dbTblDao.update(Mockito.anyList())).thenReturn(new int[1]);
        ConfigDbView result = mhaDbMappingService.configEmailGroupForDb("db_dalcluster", "email");
        Assert.assertEquals(result.getSize(), 2);
        result = mhaDbMappingService.configEmailGroupForDb("db", "email");
        Assert.assertEquals(result.getSize(), 2);
    }

    private static List<MhaDbMappingTbl> getMhaDbMappingTbls() {
        List<MhaDbMappingTbl> lists = new ArrayList<>();
        MhaDbMappingTbl mhaDbMappingTbl = new MhaDbMappingTbl();
        mhaDbMappingTbl.setDbId(111L);
        lists.add(mhaDbMappingTbl);
        return lists;
    }

    private static List<DbTbl> getDbTblsWithDuplicate() {
        List<DbTbl> tbls = new ArrayList<>();
        for (int i = 1; i <= 5; i++) {
            DbTbl tbl = new DbTbl();
            tbl.setDeleted(0);
            tbl.setDbName("db" + i);
            tbl.setId((long) i);
            tbl.setCreateTime(new Timestamp(System.currentTimeMillis()));
            tbls.add(tbl);
        }
        
        DbTbl DB2 = new DbTbl();
        DB2.setDeleted(0);
        DB2.setDbName("DB" + 2);
        DB2.setId((long) 6);
        DB2.setCreateTime(new Timestamp(System.currentTimeMillis()));
        tbls.add(DB2);

        DbTbl Db2 = new DbTbl();
        Db2.setDeleted(0);
        Db2.setDbName("Db" + 2);
        Db2.setId((long) 7);
        Db2.setCreateTime(new Timestamp(System.currentTimeMillis()));
        tbls.add(Db2);

        DbTbl db3 = new DbTbl();
        db3.setDeleted(0);
        db3.setDbName("db" + 3);
        db3.setId((long) 8);
        db3.setCreateTime(new Timestamp(System.currentTimeMillis()));
        tbls.add(db3);
        return tbls;
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
