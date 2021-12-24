package com.ctrip.framework.drc.console.service.impl;

import com.ctrip.framework.drc.console.dao.ClusterTblDao;
import com.ctrip.framework.drc.console.dao.entity.ClusterTbl;
import com.ctrip.framework.drc.console.service.ClusterTblService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.sql.SQLException;
import java.util.List;

/**
 * @author shenhaibo
 * @version 1.0
 * date: 2020-04-09
 */
@Service
public class ClusterTblServiceImpl implements ClusterTblService {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired
    private ClusterTblDao clusterTblDao;

    @Override
    public Integer getRecordsCount() {
        try {
            return clusterTblDao.count();
        } catch (SQLException e) {
            logger.error("SQLException ", e);
            return -1;
        }
    }

    public List<ClusterTbl> getClusters(int pageNo, int pageSize) {
        try {
            return clusterTblDao.queryAllByPage(pageNo, pageSize);
        } catch (SQLException e) {
            logger.error("SQLException ", e);
            return null;
        }
    }

    public ClusterTbl createPojo(String clusterName, Long clusterAppId) {
        ClusterTbl daoPojo = new ClusterTbl();
        daoPojo.setClusterName(clusterName);
        daoPojo.setClusterAppId(clusterAppId);
        return daoPojo;
    }
}
