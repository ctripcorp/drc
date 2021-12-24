package com.ctrip.framework.drc.performance;

import com.ctrip.framework.drc.core.meta.DBInfo;
import com.ctrip.framework.drc.core.meta.InstanceInfo;
import com.ctrip.framework.drc.core.server.config.applier.dto.ApplierConfigDto;
import com.ctrip.framework.drc.performance.container.ApplierTestServerContainer;
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by jixinwang on 2021/9/22
 */
public class ApplierTestServerContainerTest {

    private ApplierConfigDto config;

    ApplierTestServerContainer serverContainer;

    @Before
    public void before() {
        System.setProperty("parse.file.path", "src/test/resources/rbinlog");
        initApplierConfigDto();
       serverContainer = new ApplierTestServerContainer();
    }

    @Test
    public void testApplyWithSetGtid() throws Exception {
        boolean added = serverContainer.addServer(config);
        Assert.assertTrue(added);
//        Thread.currentThread().join();
    }

    @Test
    public void testApplyWithTransactionTable() throws Exception {
        System.setProperty("parse.file.path", "/data/drc/replicator/test.consume");
        System.setProperty("transaction.table.size", "500");
        System.setProperty("transaction.table.merge.size", "100");
        config.setApplyMode(1);
        boolean added = serverContainer.addServer(config);
//        Thread.currentThread().join();
        Assert.assertTrue(added);
    }

    private void initApplierConfigDto() {
        config = new ApplierConfigDto();
        config.setCluster("appliertest_dalcluster");
        InstanceInfo replicator = new InstanceInfo();
        replicator.setMhaName("srcMha");
        replicator.setCluster("appliertest_dalcluster");
        config.setReplicator(replicator);
        DBInfo target = new DBInfo();
        target.setUsername("root");
        target.setIp("127.0.0.1");
//        target.setPassword("123");
//        target.setPort(13306);
        target.setPassword("123456");
        target.setPort(3306);
        target.setMhaName("destMha");
        config.setTarget(target);
        config.setGtidExecuted("11d1ec30-d557-11ea-8aab-77480644458f:1-5");
    }

//    @Test
    public void getAvg() {
        String recordStr = "618365,599138,594870,587543,567409,572133,552491,594103,554655,504068";
        String[] records = recordStr.split(",");
        List<Integer> values = Lists.newArrayList();
        for(String record : records) {
            values.add(Integer.parseInt(record));
        }
        int sum = 0;
        for(int value : values) {
            sum += value;
        }
        int avg = sum / values.size() / 30;
        System.out.println("平均值为：" + avg);
    }

//    @Test
    public void testCopy() {
        ConcurrentHashMap<Integer, String> idAndGtid = new ConcurrentHashMap<Integer, String>();
        idAndGtid.put(1, "one");
        ConcurrentHashMap<Integer, String> copy = new ConcurrentHashMap<Integer, String>(idAndGtid);
        System.out.println("1:" + copy);
        idAndGtid.put(2, "two");
        System.out.println("2copy:" + copy);
        copy.remove(1);
        System.out.println("2origin:" + idAndGtid);
        Set<Integer> sets = copy.keySet();
        System.out.println("keys:" + copy.keySet());
    }
}
