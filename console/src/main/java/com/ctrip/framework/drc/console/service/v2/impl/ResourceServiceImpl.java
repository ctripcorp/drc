package com.ctrip.framework.drc.console.service.v2.impl;

import com.ctrip.framework.drc.console.dao.ReplicatorTblDao;
import com.ctrip.framework.drc.console.dao.ResourceTblDao;
import com.ctrip.framework.drc.console.dao.entity.ReplicatorTbl;
import com.ctrip.framework.drc.console.dao.entity.ResourceTbl;
import com.ctrip.framework.drc.console.dao.entity.v2.ApplierTblV2;
import com.ctrip.framework.drc.console.dao.v2.ApplierTblV2Dao;
import com.ctrip.framework.drc.console.enums.BooleanEnum;
import com.ctrip.framework.drc.console.param.v2.ResourceBuildParam;
import com.ctrip.framework.drc.console.param.v2.ResourceQueryParam;
import com.ctrip.framework.drc.console.service.v2.ResourceService;
import com.ctrip.framework.drc.console.utils.ConsoleExceptionUtils;
import com.ctrip.framework.drc.console.utils.PreconditionUtils;
import com.ctrip.framework.drc.console.vo.v2.ResourceView;
import com.ctrip.framework.drc.core.http.PageResult;
import com.ctrip.framework.drc.core.monitor.enums.ModuleEnum;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.util.CollectionUtils;

import java.util.List;


/**
 * Created by dengquanliang
 * 2023/8/3 16:18
 */
public class ResourceServiceImpl implements ResourceService {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired
    private ResourceTblDao resourceTblDao;

    @Autowired
    private ReplicatorTblDao replicatorTblDao;

    @Autowired
    private ApplierTblV2Dao applierTblDao;

    @Override
    public void configureResource(ResourceBuildParam param) throws Exception {
        checkResourceBuildParam(param);

        ResourceTbl resourceTbl = new ResourceTbl();
        resourceTbl.setIp(param.getIp());
        resourceTbl.setDcId(param.getDcId());
        resourceTbl.setAzId(param.getAzId());
        resourceTbl.setTag(param.getTag());
        resourceTbl.setDeleted(BooleanEnum.FALSE.getCode());
        resourceTbl.setActive(BooleanEnum.TRUE.getCode());

        ModuleEnum module = ModuleEnum.getModuleEnum(param.getType());
        resourceTbl.setAppId(module.getAppId());
        resourceTbl.setType(module.getCode());

        ResourceTbl existResourceTbl = resourceTblDao.queryByIp(param.getIp());
        if (existResourceTbl != null) {
            if (existResourceTbl.getDeleted() == BooleanEnum.TRUE.getCode()) {
                resourceTbl.setId(existResourceTbl.getId());
                resourceTblDao.update(resourceTbl);
                return;
            } else {
                throw ConsoleExceptionUtils.message(String.format("ip :{} already exist!", param.getIp()));
            }
        }

        logger.info("insert resource: {}", resourceTbl);
        resourceTblDao.insert(resourceTbl);
    }

    @Override
    public void offlineResource(long resourceId) throws Exception {
        ResourceTbl resourceTbl = resourceTblDao.queryByPk(resourceId);
        if (resourceTbl == null) {
            throw ConsoleExceptionUtils.message("resource not exist!");
        }
        if (resourceTbl.getType() == ModuleEnum.REPLICATOR.getCode()) {
            List<ReplicatorTbl> replicatorTbls = replicatorTblDao.queryByResourceIds(Lists.newArrayList(resourceId));
            if (!CollectionUtils.isEmpty(replicatorTbls)) {
                throw ConsoleExceptionUtils.message("resource is in use, cannot offline!");
            }
        } else if (resourceTbl.getType() == ModuleEnum.APPLIER.getCode()) {
            List<ApplierTblV2> applierTbls = applierTblDao.queryByResourceIds(Lists.newArrayList(resourceId));
            if (!CollectionUtils.isEmpty(applierTbls)) {
                throw ConsoleExceptionUtils.message("resource is in use, cannot offline!");
            }
        }

        resourceTbl.setDeleted(BooleanEnum.TRUE.getCode());
        logger.info("offline resourceIp: {}", resourceTbl.getIp());
        resourceTblDao.update(resourceTbl);
    }

    @Override
    public void onlineResource(long resourceId) throws Exception {
        ResourceTbl resourceTbl = resourceTblDao.queryByPk(resourceId);
        if (resourceTbl == null) {
            throw ConsoleExceptionUtils.message("resource not exist!");
        }

        resourceTbl.setDeleted(BooleanEnum.FALSE.getCode());
        logger.info("online resourceIp: {}", resourceTbl.getIp());
        resourceTblDao.update(resourceTbl);
    }

    @Override
    public void deactivateResource(long resourceId) throws Exception {
        ResourceTbl resourceTbl = resourceTblDao.queryByPk(resourceId);
        if (resourceTbl == null) {
            throw ConsoleExceptionUtils.message("resource not exist!");
        }

        resourceTbl.setActive(BooleanEnum.FALSE.getCode());
        logger.info("deactivate resourceIp: {}", resourceTbl.getIp());
        resourceTblDao.update(resourceTbl);
    }

    @Override
    public void recoverResource(long resourceId) throws Exception {
        ResourceTbl resourceTbl = resourceTblDao.queryByPk(resourceId);
        if (resourceTbl == null) {
            throw ConsoleExceptionUtils.message("resource not exist!");
        }

        resourceTbl.setActive(BooleanEnum.TRUE.getCode());
        logger.info("deactivate resourceIp: {}", resourceTbl.getIp());
        resourceTblDao.update(resourceTbl);
    }

    @Override
    public PageResult<ResourceView> getResourceView(ResourceQueryParam param) {
        return null;
    }

    private void checkResourceBuildParam(ResourceBuildParam param) {
        PreconditionUtils.checkString(param.getIp(), "ip requires not empty!");
        PreconditionUtils.checkString(param.getType(), "type requires not empty!");
        PreconditionUtils.checkId(param.getDcId(), "dc requires not empty!");
        PreconditionUtils.checkString(param.getAzId(), "AZ requires not empty!");
    }
}
