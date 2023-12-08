package com.ctrip.framework.drc.console.utils;

import com.ctrip.framework.drc.console.dao.entity.v2.DbReplicationTbl;
import com.ctrip.framework.drc.console.dao.entity.v3.MhaDbReplicationTbl;
import com.ctrip.framework.drc.console.dto.v3.MhaDbDto;
import com.ctrip.framework.drc.console.dto.v3.MhaDbReplicationDto;
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.stream.Collectors;

public class StreamUtilsTest {

    public static class SomeObject {
        String p1;
        Integer p2;


        public SomeObject(String p1, Integer p2) {
            this.p1 = p1;
            this.p2 = p2;
        }
    }

    @Test
    public void testDistinctByKey() throws Exception {
        List<SomeObject> list = Lists.newArrayList();
        SomeObject o1 = new SomeObject("a", 1);
        list.add(o1);
        SomeObject o2 = new SomeObject("a", 2);
        list.add(o2);
        SomeObject o3 = new SomeObject("b", 1);
        list.add(o3);
        SomeObject o4 = new SomeObject("c", 2);
        list.add(o4);

        List<SomeObject> p1List = list.stream().filter(StreamUtils.distinctByKey(e -> e.p1)).collect(Collectors.toList());
        Assert.assertTrue(p1List.contains(o3));
        Assert.assertTrue(p1List.contains(o4));
        Assert.assertTrue(p1List.stream().anyMatch(e -> e.p1.equals(o1.p1)));
        Assert.assertEquals(3, p1List.size());
        List<SomeObject> p2List = list.stream().filter(StreamUtils.distinctByKey(e -> e.p2)).collect(Collectors.toList());
        Assert.assertEquals(2, p2List.size());
    }

    @Test
    public void testGetKey() throws Exception {
        MhaDbReplicationTbl mhaDbReplicationTbl = new MhaDbReplicationTbl();
        long srcMhaDbMappingId = 1L;
        long dstMhaDbMappingId = 2L;
        int replicationType = 0;

        mhaDbReplicationTbl.setSrcMhaDbMappingId(srcMhaDbMappingId);
        mhaDbReplicationTbl.setDstMhaDbMappingId(dstMhaDbMappingId);
        mhaDbReplicationTbl.setReplicationType(replicationType);

        DbReplicationTbl dbReplicationTbl = new DbReplicationTbl();
        dbReplicationTbl.setSrcMhaDbMappingId(srcMhaDbMappingId);
        dbReplicationTbl.setDstMhaDbMappingId(dstMhaDbMappingId);
        dbReplicationTbl.setReplicationType(replicationType);


        MhaDbReplicationDto dto = new MhaDbReplicationDto();
        dto.setSrc(new MhaDbDto(srcMhaDbMappingId,"mha1","db1"));
        dto.setDst(new MhaDbDto(dstMhaDbMappingId,"mha2","db1"));
        dto.setReplicationType(replicationType);


        MultiKey key1 = StreamUtils.getKey(mhaDbReplicationTbl);
        MultiKey key2 = StreamUtils.getKey(dbReplicationTbl);
        MultiKey key3 = StreamUtils.getKey(dto);

        Assert.assertEquals(key2, key1);
        Assert.assertEquals(key3, key1);

        Assert.assertEquals(new MultiKey(srcMhaDbMappingId, dstMhaDbMappingId, replicationType), key1);
        Assert.assertNotEquals(new MultiKey(dstMhaDbMappingId, srcMhaDbMappingId, replicationType), key1);
        Assert.assertNotEquals(new MultiKey(srcMhaDbMappingId, dstMhaDbMappingId, 0L), key1);

    }
}

