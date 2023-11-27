package com.ctrip.framework.drc.core.service.statistics.traffic;

import com.ctrip.framework.drc.core.service.utils.JsonUtils;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;

public class HickWallConflictCountTest {

    

    @Test
    public void test() {
        List<HickWallConflictCount> conflictCounts = getConflictCounts();
        
        Assert.assertEquals("blackDb", conflictCounts.get(0).getDb());
        Assert.assertEquals("fatbbzxh", conflictCounts.get(0).getSrcMha());
        Assert.assertEquals("fatbbzxy", conflictCounts.get(0).getDestMha());
        Assert.assertEquals("tabble", conflictCounts.get(0).getTable());
        Assert.assertEquals(1000L, conflictCounts.get(0).getCount().longValue());
        
    }

    private List<HickWallConflictCount> getConflictCounts() {
        String jsonResult = "[{\"metric\":{\"db\":\"blackDb\",\"destMha\":\"fatbbzxy\",\"srcMha\":\"fatbbzxh\",\"table\":\"tabble\"},\"values\":[[1701071020,\"2\"],[1701071080,\"0\"],[1701071140,\"1000\"]]},{\"metric\":{\"db\":\"notBlackDb\",\"destMha\":\"zyn_test_1\",\"srcMha\":\"zyn_test_2\",\"table\":\"test\"},\"values\":[[1701070840,\"0\"],[1701070900,\"0\"],[1701070960,\"0\"],[1701071020,\"0\"],[1701071080,\"0\"],[1701071140,\"1000\"]]}]";
        return JsonUtils.fromJsonToList(jsonResult, HickWallConflictCount.class);
    }
}