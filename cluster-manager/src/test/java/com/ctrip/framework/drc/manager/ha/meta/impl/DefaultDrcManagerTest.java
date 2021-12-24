package com.ctrip.framework.drc.manager.ha.meta.impl;

import com.ctrip.framework.drc.core.entity.DbCluster;
import com.ctrip.framework.drc.core.entity.Route;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;

/**
 * @Author: hbshen
 * @Date: 2021/4/15
 */
public class DefaultDrcManagerTest {

    private DefaultDrcManager drcManager;

    private static String OY = "oy", FQ = "fq", JQ = "jq", RB = "rb", FRA = "fra", CLUSTER_ID_1 = "drcTest-1.abc", CLUSTER_ID_2 = "drcTest-2.abc";
    @Before
    public void beforeDefaultFileDaoTest() throws Exception {

        drcManager = (DefaultDrcManager) DefaultDrcManager.buildFromFile("file-dao-test.xml");
    }

    @Test
    public void testRandomRoute(){

        //for JQ cluster1
        DbCluster clusterMeta1 = drcManager.getCluster(JQ, CLUSTER_ID_1);
        Set<Route> routes = new HashSet<>();
        for(int i=0;i<100;i++){
            Route route = drcManager.metaRandomRoutes(JQ, clusterMeta1.getOrgId(), FRA);
            routes.add(route);
            Assert.assertTrue(route.getId() >= 1 && route.getId() <=3 );
        }
        Assert.assertEquals(3, routes.size());


        //for JQ cluster2
        DbCluster clusterMeta2 = drcManager.getCluster(JQ, CLUSTER_ID_2);
        for(int i=0;i<100;i++){
            Route route = drcManager.metaRandomRoutes(JQ, clusterMeta2.getOrgId(), FRA);
            Assert.assertEquals(4, route.getId().intValue());
        }


        //for RB cluster2
        clusterMeta1 = drcManager.getCluster(RB, CLUSTER_ID_1);
        Route route = drcManager.metaRandomRoutes(RB, clusterMeta1.getOrgId(), OY);
        Assert.assertNull(route);
        route = drcManager.metaRandomRoutes(RB, clusterMeta1.getOrgId(), FQ);
        Assert.assertNull(route);
    }

}
