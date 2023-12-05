package com.ctrip.framework.drc.console.utils.convert;

import com.ctrip.framework.drc.console.dao.entity.v2.DbReplicationTbl;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TableNameBuilderTest {

    @Test
    public void buildNameFilter() {
    }

    @Test
    public void buildNameMapping() {
    }

    @Test
    public void testBuildNameFilter() {
    }

    @Test
    public void testBuildNameMapping() {
        Map<Long, String> mappingIdToDbTblMap = new HashMap<>();
        List<DbReplicationTbl> dbReplicationTbls = new ArrayList<>();
        // 1. empty replications
        Assert.assertEquals("", TableNameBuilder.buildNameFilter(mappingIdToDbTblMap, dbReplicationTbls));

        mappingIdToDbTblMap.put(11L, "db1");
        mappingIdToDbTblMap.put(12L, "db1");
        mappingIdToDbTblMap.put(21L, "db2");
        // 2. empty replications
        Assert.assertEquals("", TableNameBuilder.buildNameFilter(mappingIdToDbTblMap, dbReplicationTbls));

        DbReplicationTbl replication1 = new DbReplicationTbl();
        replication1.setSrcMhaDbMappingId(11L);
        replication1.setDstMhaDbMappingId(12L);
        replication1.setSrcLogicTableName("testTable");
        dbReplicationTbls.add(replication1);
        // 3. one replication
        Assert.assertEquals("db1\\.testTable", TableNameBuilder.buildNameFilter(mappingIdToDbTblMap, dbReplicationTbls));



        replication1.setSrcMhaDbMappingId(999L);
        // 3. mapping not found
        Assert.assertEquals("", TableNameBuilder.buildNameFilter(mappingIdToDbTblMap, dbReplicationTbls));
    }
}