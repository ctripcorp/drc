package com.ctrip.framework.drc.console.dao;

import com.ctrip.framework.drc.console.dao.entity.ResourceTbl;
import com.ctrip.framework.drc.console.param.v2.resource.ResourceQueryParam;
import com.ctrip.framework.drc.console.service.v2.AbstractIntegrationTest;
import com.ctrip.framework.drc.core.http.PageReq;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by dengquanliang
 * 2023/8/4 11:30
 */
public class ResourceDaoTest extends AbstractIntegrationTest {

    @Autowired
    private ResourceTblDao resourceTblDao;

    @Test
    public void testQueryByParam() throws SQLException {
        insertResource();
        ResourceQueryParam param = new ResourceQueryParam();
        PageReq pageReq = new PageReq();
        param.setPageReq(pageReq);
        List<ResourceTbl> resourceTbls = resourceTblDao.queryByParam(param);

        System.out.println(resourceTbls.size());
        System.out.println(pageReq.getTotalCount());
    }

    private void insertResource() throws SQLException {
        List<ResourceTbl> resourceTblList = new ArrayList<>();
        for (int i = 1; i <= 50; i++) {
            ResourceTbl resourceTbl = new ResourceTbl();
            resourceTbl.setIp("ip" + i);
            resourceTbl.setType(i % 5);
            resourceTbl.setDcId(Long.valueOf(i % 5));
            resourceTbl.setTag("tag" + i % 5);
            resourceTbl.setAz("az" + i % 5);
            resourceTblList.add(resourceTbl);
        }

        resourceTblDao.insert(resourceTblList);
    }
}
