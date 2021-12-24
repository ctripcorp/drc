package com.ctrip.framework.drc.console.service.impl;

import com.ctrip.framework.drc.console.dao.ClusterTblDao;
import com.ctrip.framework.drc.console.dao.entity.ClusterTbl;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import static org.mockito.Mockito.when;

/**
 * @author shenhaibo
 * @version 1.0
 * date: 2020-04-09
 */
public class ClusterTblServiceImplTest {

    @InjectMocks
    private ClusterTblServiceImpl clusterTblService = new ClusterTblServiceImpl();

    @Mock
    private ClusterTblDao clusterTblDao;

    private List<ClusterTbl> clusterTbls;

    @Before
    public void setUp() throws SQLException {

        ClusterTbl clusterTbl1 = new ClusterTbl();
        clusterTbl1.setId(1L);
        clusterTbl1.setClusterName("cluster1");
        clusterTbl1.setClusterAppId(123456L);
        ClusterTbl clusterTbl2 = new ClusterTbl();
        clusterTbl2.setId(2L);
        clusterTbl2.setClusterName("cluster2");
        clusterTbl2.setClusterAppId(123456L);
        clusterTbls = new ArrayList<>() {{
            add(clusterTbl1);
            add(clusterTbl2);
        }};

        MockitoAnnotations.initMocks(this);
        when(clusterTblDao.count()).thenReturn(77);
        when(clusterTblDao.queryAllByPage(1, 10)).thenReturn(clusterTbls);
    }

    @Test
    public void testGetRecordsCount() throws SQLException {
        int recordsCount = clusterTblService.getRecordsCount();
        Assert.assertEquals(77, recordsCount);
    }

    @Test
    public void testGetClusters() throws SQLException {
        List<ClusterTbl> clusterTbls = clusterTblService.getClusters(1, 10);
        Assert.assertEquals(2, clusterTbls.size());
    }

    @Test
    public void testGetAllClusterNames() {
        ClusterTbl pojo = clusterTblService.createPojo("testCluster",null);
        Assert.assertNotNull(pojo);
    }
}
