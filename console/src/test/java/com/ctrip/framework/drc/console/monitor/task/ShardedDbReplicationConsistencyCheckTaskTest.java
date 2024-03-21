package com.ctrip.framework.drc.console.monitor.task;

import com.ctrip.framework.drc.console.monitor.task.ShardedDbReplicationConsistencyCheckTask.DbReplicationDo;
import com.ctrip.framework.drc.console.monitor.task.ShardedDbReplicationConsistencyCheckTask.RouteDo;
import com.ctrip.framework.drc.console.service.v2.impl.CommonDataInit;
import com.google.common.collect.Sets;
import org.assertj.core.util.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.MockitoAnnotations;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ShardedDbReplicationConsistencyCheckTaskTest extends CommonDataInit {

    @InjectMocks
    ShardedDbReplicationConsistencyCheckTask shardedDbReplicationConsistencyCheckTask;

    @Before
    public void setUp() throws SQLException, IOException {
        MockitoAnnotations.openMocks(this);
        super.setUp();
    }

    @Test
    public void testCheckDbReplicationConsistency() throws SQLException {
        shardedDbReplicationConsistencyCheckTask.checkDbReplicationConsistency();
    }

    @Test
    public void testCheckDbReplicationConsistency2() throws SQLException {
        // true
        Map<RouteDo, Map<String, List<DbReplicationDo>>> replicationsByRoute = new HashMap<>();
        Map<String, List<DbReplicationDo>> shaToSinMap = new HashMap<>();
        shaToSinMap.put("xxxshardbasedb_dalcluster", Lists.newArrayList(
                new DbReplicationDo("xxxshard01db", Sets.newHashSet(new ShardedDbReplicationConsistencyCheckTask.LogicTable(".*", null))),
                new DbReplicationDo("xxxshard02db", Sets.newHashSet(new ShardedDbReplicationConsistencyCheckTask.LogicTable(".*", null)))
        ));
        replicationsByRoute.put(new RouteDo("shaoy", "sinaws"), shaToSinMap);
        HashMap<String, Long> dalClusterToDbCount = new HashMap<>();
        dalClusterToDbCount.put("xxxshardbasedb_dalcluster", 2L);
        Assert.assertTrue(shardedDbReplicationConsistencyCheckTask.checkDbReplicationConsistency(replicationsByRoute, dalClusterToDbCount));
    }

    @Test
    public void testCheckDbReplicationConsistency5() throws SQLException {
        // true
        Map<RouteDo, Map<String, List<DbReplicationDo>>> replicationsByRoute = new HashMap<>();
        Map<String, List<DbReplicationDo>> shaToSinMap = new HashMap<>();
        shaToSinMap.put("xxxshardbasedb_dalcluster", Lists.newArrayList(
                new DbReplicationDo("xxxshard01db", Sets.newHashSet(new ShardedDbReplicationConsistencyCheckTask.LogicTable("(table1)", "xxx.shard.binlog"))),
                new DbReplicationDo("xxxshard02db", Sets.newHashSet(new ShardedDbReplicationConsistencyCheckTask.LogicTable("(table1)", "xxx.shard.binlog")))
        ));
        replicationsByRoute.put(new RouteDo("shaoy", "sinaws"), shaToSinMap);
        HashMap<String, Long> dalClusterToDbCount = new HashMap<>();
        dalClusterToDbCount.put("xxxshardbasedb_dalcluster", 2L);
        Assert.assertTrue(shardedDbReplicationConsistencyCheckTask.checkDbReplicationConsistency(replicationsByRoute, dalClusterToDbCount));
    }

    @Test
    public void testCheckDbReplicationConsistency3() throws SQLException {
        // true
        Map<RouteDo, Map<String, List<DbReplicationDo>>> replicationsByRoute = new HashMap<>();
        Map<String, List<DbReplicationDo>> shaToSinMap = new HashMap<>();
        shaToSinMap.put("xxxshardbasedb_dalcluster", Lists.newArrayList(
                new DbReplicationDo("xxxshard01db", Sets.newHashSet(new ShardedDbReplicationConsistencyCheckTask.LogicTable(".*", null))),
                new DbReplicationDo("xxxshard02db", Sets.newHashSet(new ShardedDbReplicationConsistencyCheckTask.LogicTable(".*", null)))
        ));
        replicationsByRoute.put(new RouteDo("shaoy", "sinaws"), shaToSinMap);
        HashMap<String, Long> dalClusterToDbCount = new HashMap<>();
        dalClusterToDbCount.put("xxxshardbasedb_dalcluster", 3L);
        Assert.assertFalse(shardedDbReplicationConsistencyCheckTask.checkDbReplicationConsistency(replicationsByRoute, dalClusterToDbCount));
    }

    @Test
    public void testCheckDbReplicationConsistency4() throws SQLException {
        // true
        Map<RouteDo, Map<String, List<DbReplicationDo>>> replicationsByRoute = new HashMap<>();
        Map<String, List<DbReplicationDo>> shaToSinMap = new HashMap<>();
        shaToSinMap.put("xxxshardbasedb_dalcluster", Lists.newArrayList(
                new DbReplicationDo("xxxshard01db", Sets.newHashSet(new ShardedDbReplicationConsistencyCheckTask.LogicTable("(table1)", null))),
                new DbReplicationDo("xxxshard02db", Sets.newHashSet(new ShardedDbReplicationConsistencyCheckTask.LogicTable("(table2)", null)))
        ));
        replicationsByRoute.put(new RouteDo("shaoy", "sinaws"), shaToSinMap);
        HashMap<String, Long> dalClusterToDbCount = new HashMap<>();
        dalClusterToDbCount.put("xxxshardbasedb_dalcluster", 2L);
        Assert.assertFalse(shardedDbReplicationConsistencyCheckTask.checkDbReplicationConsistency(replicationsByRoute, dalClusterToDbCount));
    }

    @Test
    public void testCheckDbReplicationConsistency6() throws SQLException {
        // true
        Map<RouteDo, Map<String, List<DbReplicationDo>>> replicationsByRoute = new HashMap<>();
        Map<String, List<DbReplicationDo>> shaToSinMap = new HashMap<>();
        shaToSinMap.put("xxxshardbasedb_dalcluster", Lists.newArrayList(
                new DbReplicationDo("xxxshard01db", Sets.newHashSet(new ShardedDbReplicationConsistencyCheckTask.LogicTable("(table1)", "xxx.shard.binlog1"))),
                new DbReplicationDo("xxxshard02db", Sets.newHashSet(new ShardedDbReplicationConsistencyCheckTask.LogicTable("(table1)", "xxx.shard.binlog2")))
        ));
        replicationsByRoute.put(new RouteDo("shaoy", "sinaws"), shaToSinMap);
        HashMap<String, Long> dalClusterToDbCount = new HashMap<>();
        dalClusterToDbCount.put("xxxshardbasedb_dalcluster", 2L);
        Assert.assertFalse(shardedDbReplicationConsistencyCheckTask.checkDbReplicationConsistency(replicationsByRoute, dalClusterToDbCount));
    }

    @Test
    public void testGetDalClusterName() throws Exception {
        Assert.assertEquals("db1_dalcluster", new ShardedDbReplicationConsistencyCheckTask.DbReplicationDo("db1", Sets.newHashSet()).getDalClusterName());
        Assert.assertEquals("xxxshardbasedb_dalcluster", new ShardedDbReplicationConsistencyCheckTask.DbReplicationDo("xxxshard1db", Sets.newHashSet()).getDalClusterName());
        Assert.assertEquals("xxxshardbasedb_dalcluster", new ShardedDbReplicationConsistencyCheckTask.DbReplicationDo("xxxshard01db", Sets.newHashSet()).getDalClusterName());
        Assert.assertEquals("xxxshardbasedb_dalcluster", new ShardedDbReplicationConsistencyCheckTask.DbReplicationDo("xxxshard02db", Sets.newHashSet()).getDalClusterName());
    }

    @Test
    public void testIsConsistent() {
        List<ShardedDbReplicationConsistencyCheckTask.DbReplicationDo> dbReplicationDos1 = new ArrayList<>();
        dbReplicationDos1.add(new ShardedDbReplicationConsistencyCheckTask.DbReplicationDo("db1", Sets.newHashSet(new ShardedDbReplicationConsistencyCheckTask.LogicTable(".*", null))));
        dbReplicationDos1.add(new ShardedDbReplicationConsistencyCheckTask.DbReplicationDo("db2", Sets.newHashSet(new ShardedDbReplicationConsistencyCheckTask.LogicTable(".*", null))));
        Assert.assertTrue(ShardedDbReplicationConsistencyCheckTask.isConsistent(dbReplicationDos1));

        List<ShardedDbReplicationConsistencyCheckTask.DbReplicationDo> dbReplicationDos2 = new ArrayList<>();
        dbReplicationDos2.add(new ShardedDbReplicationConsistencyCheckTask.DbReplicationDo("db1", Sets.newHashSet(new ShardedDbReplicationConsistencyCheckTask.LogicTable("(table1|table2)", null))));
        dbReplicationDos2.add(new ShardedDbReplicationConsistencyCheckTask.DbReplicationDo("db2", Sets.newHashSet(new ShardedDbReplicationConsistencyCheckTask.LogicTable("(table1)", null))));
        Assert.assertFalse(ShardedDbReplicationConsistencyCheckTask.isConsistent(dbReplicationDos2));

    }

}