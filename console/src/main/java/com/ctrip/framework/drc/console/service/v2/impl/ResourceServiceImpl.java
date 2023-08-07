package com.ctrip.framework.drc.console.service.v2.impl;

import com.ctrip.framework.drc.console.dao.DcTblDao;
import com.ctrip.framework.drc.console.dao.ReplicatorTblDao;
import com.ctrip.framework.drc.console.dao.ResourceTblDao;
import com.ctrip.framework.drc.console.dao.entity.DcTbl;
import com.ctrip.framework.drc.console.dao.entity.ReplicatorTbl;
import com.ctrip.framework.drc.console.dao.entity.ResourceTbl;
import com.ctrip.framework.drc.console.dao.entity.v2.ApplierTblV2;
import com.ctrip.framework.drc.console.dao.entity.v2.MhaTblV2;
import com.ctrip.framework.drc.console.dao.v2.ApplierTblV2Dao;
import com.ctrip.framework.drc.console.dao.v2.MhaTblV2Dao;
import com.ctrip.framework.drc.console.enums.BooleanEnum;
import com.ctrip.framework.drc.console.enums.ResourceTagEnum;
import com.ctrip.framework.drc.console.param.v2.resource.ResourceBuildParam;
import com.ctrip.framework.drc.console.param.v2.resource.ResourceQueryParam;
import com.ctrip.framework.drc.console.service.v2.ResourceService;
import com.ctrip.framework.drc.console.utils.ConsoleExceptionUtils;
import com.ctrip.framework.drc.console.utils.PreconditionUtils;
import com.ctrip.framework.drc.console.vo.v2.ResourceView;
import com.ctrip.framework.drc.core.http.PageReq;
import com.ctrip.framework.drc.core.monitor.enums.ModuleEnum;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.util.CollectionUtils;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;


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
    @Autowired
    private DcTblDao dcTblDao;
    @Autowired
    private MhaTblV2Dao mhaTblDao;

    private static final int THOUSAND = 1000;

    @Override
    public void configureResource(ResourceBuildParam param) throws Exception {
        checkResourceBuildParam(param);

        ResourceTbl resourceTbl = new ResourceTbl();
        resourceTbl.setIp(param.getIp());
        resourceTbl.setDcId(param.getDcId());
        resourceTbl.setAz(param.getAz());
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
    public List<ResourceView> getResourceView(ResourceQueryParam param) throws Exception {
        List<ResourceTbl> resourceTbls = resourceTblDao.queryByParam(param);
        if (CollectionUtils.isEmpty(resourceTbls)) {
            return new ArrayList<>();
        }

        List<Long> resourceIds = resourceTbls.stream().map(ResourceTbl::getId).collect(Collectors.toList());
        List<ReplicatorTbl> replicatorTbls = replicatorTblDao.queryByResourceIds(resourceIds);
        List<ApplierTblV2> applierTbls = applierTblDao.queryByResourceIds(resourceIds);

        Map<Long, Long> replicatorMap = replicatorTbls.stream().collect(Collectors.groupingBy(ReplicatorTbl::getResourceId, Collectors.counting()));
        Map<Long, Long> applierMap = applierTbls.stream().collect(Collectors.groupingBy(ApplierTblV2::getResourceId, Collectors.counting()));

        List<ResourceView> views = buildResourceViews(resourceTbls, replicatorMap, applierMap);
        return views;
    }

    @Override
    public List<ResourceView> getResourceIpByMha(String mhaName, int type) throws Exception {
        MhaTblV2 mhaTbl = mhaTblDao.queryByMhaName(mhaName, BooleanEnum.FALSE.getCode());
        if (mhaTbl == null) {
            logger.info("mha: {} not exist", mhaName);
            return new ArrayList<>();
        }

        if (type != ModuleEnum.REPLICATOR.getCode() || type != ModuleEnum.APPLIER.getCode()) {
            logger.info("resource type: {} can only be replicator or applier", type);
            return new ArrayList<>();
        }
        DcTbl dcTbl = dcTblDao.queryById(mhaTbl.getDcId());
        List<Long> dcIds = dcTblDao.queryByRegionName(dcTbl.getRegionName()).stream().map(DcTbl::getId).collect(Collectors.toList());
        return getResourceViews(dcIds, type, mhaTbl.getTag());
    }

    @Override
    public List<ResourceView> autoConfigureResource(String mhaName, int type, List<String> selectedIps) throws Exception {
        List<ResourceView> resultViews = new ArrayList<>();
        List<ResourceView> resourceViews = getResourceIpByMha(mhaName, type);
        if (CollectionUtils.isEmpty(resourceViews)) {
            return resultViews;
        }

        if (!CollectionUtils.isEmpty(selectedIps) && selectedIps.size() == 1) {
            ResourceView firstResource = resourceViews.stream().filter(e -> e.getIp().equals(selectedIps.get(0))).findFirst().orElse(null);
            if (firstResource != null) {
                resultViews.add(firstResource);
            }

        } else if (!CollectionUtils.isEmpty(selectedIps)) {
            resourceViews = resourceViews.stream().filter(e -> !selectedIps.contains(e.getIp())).collect(Collectors.toList());
        }

        setResourceView(resultViews, resourceViews);
        return resultViews;
    }

    @Override
    public List<ResourceView> getResourceUnused(int type) throws Exception {
        ResourceQueryParam param = new ResourceQueryParam();
        param.setType(type);
        PageReq pageReq = new PageReq();
        pageReq.setPageSize(THOUSAND);
        param.setPageReq(pageReq);

        List<ResourceView> resourceViews = getResourceView(param);
        List<ResourceView> resourcesUnused = resourceViews.stream().filter(e -> e.getInstanceNum() == 0L).collect(Collectors.toList());
        return resourcesUnused;
    }

    @Override
    public int deleteResourceUnused(List<String> ips) throws Exception {
        List<ResourceTbl> resourceTbls = resourceTblDao.queryByIps(ips);
        List<Long> resourceIds = resourceTbls.stream().map(ResourceTbl::getId).collect(Collectors.toList());
        List<ReplicatorTbl> replicators = replicatorTblDao.queryByResourceIds(resourceIds);
        List<ApplierTblV2> appliers = applierTblDao.queryByResourceIds(resourceIds);

        Set<Long> resourceIdsInUse = replicators.stream().map(ReplicatorTbl::getResourceId).collect(Collectors.toSet());
        resourceIdsInUse.addAll(appliers.stream().map(ApplierTblV2::getResourceId).collect(Collectors.toSet()));

        resourceTbls = resourceTbls.stream().filter(e -> !resourceIdsInUse.contains(e.getId())).collect(Collectors.toList());
        resourceTbls.forEach(e -> e.setDeleted(BooleanEnum.TRUE.getCode()));
        int result = resourceTblDao.update(resourceTbls).length;
        return result;
    }

    @Override
    public int deleteResourceUnused(int type) throws Exception {
        List<ResourceView> resourceViews = getResourceUnused(type);
        if (CollectionUtils.isEmpty(resourceViews)) {
            return 0;
        }

        List<String> ips = resourceViews.stream().map(ResourceView::getIp).collect(Collectors.toList());
        return deleteResourceUnused(ips);
    }

    @Override
    public int updateResource(List<ResourceBuildParam> params) throws Exception {
        List<String> ips = params.stream().map(ResourceBuildParam::getIp).collect(Collectors.toList());
        List<ResourceTbl> resourceTbls = resourceTblDao.queryByIps(ips);
        Map<String, ResourceTbl> resourceMap = resourceTbls.stream().collect(Collectors.toMap(ResourceTbl::getIp, Function.identity()));

        List<String> notExistIps = new ArrayList<>();
        List<ResourceTbl> updateResourceTbls = new ArrayList<>();
        for (ResourceBuildParam param : params) {
            String ip = param.getIp();
            if (!resourceMap.containsKey(ip)) {
                notExistIps.add(ip);
                continue;
            }
            ResourceTbl resourceTbl = resourceMap.get(ip);
            if (StringUtils.isNotBlank(param.getTag())) {
                resourceTbl.setTag(param.getTag());
            }
            if (StringUtils.isNotBlank(param.getAz())) {
                resourceTbl.setAz(param.getAz());
            }
            updateResourceTbls.add(resourceTbl);
        }

        if (!CollectionUtils.isEmpty(notExistIps)) {
            throw ConsoleExceptionUtils.message(String.format("ip: %s not exist", notExistIps));
        }
        if (!CollectionUtils.isEmpty(updateResourceTbls)) {
            resourceTblDao.update(updateResourceTbls);
        }
        return updateResourceTbls.size();
    }

    @Override
    public int updateResource(String dc) throws Exception {
        DcTbl dcTbl = dcTblDao.queryByDcName(dc);
        if (dcTbl == null) {
            throw ConsoleExceptionUtils.message(String.format("dc: %s not exist", dc));
        }
        List<ResourceTbl> resourceTbls = resourceTblDao.queryByDcId(dcTbl.getId());
        resourceTbls.forEach(e -> {
            e.setAz(dc);
            e.setTag(ResourceTagEnum.COMMON.getName());
        });
        resourceTblDao.update(resourceTbls);
        return resourceTbls.size();
    }

    private void setResourceView(List<ResourceView> resultViews, List<ResourceView> resourceViews) {
        if (CollectionUtils.isEmpty(resultViews)) {
            resultViews.add(resourceViews.get(0));
        }
        ResourceView firstResource = resultViews.get(0);
        ResourceView secondResource = resourceViews.stream().filter(e -> e.getAz().equals(firstResource.getAz())).findFirst().orElse(null);
        if (secondResource != null) {
            resultViews.add(secondResource);
        }
    }

    private List<ResourceView> getResourceViews(List<Long> dcIds, int type, String tag) throws Exception {
        List<ResourceTbl> resourceTbls = resourceTblDao.queryByDcAndTag(dcIds, tag, type);
        List<Long> resourceIds = resourceTbls.stream().map(ResourceTbl::getId).collect(Collectors.toList());

        Map<Long, Long> replicatorMap = new HashMap<>();
        Map<Long, Long> applierMap = new HashMap<>();
        if (type == ModuleEnum.REPLICATOR.getCode()) {
            List<ReplicatorTbl> replicatorTbls = replicatorTblDao.queryByResourceIds(resourceIds);
            replicatorMap = replicatorTbls.stream().collect(Collectors.groupingBy(ReplicatorTbl::getResourceId, Collectors.counting()));
        } else if (type == ModuleEnum.APPLIER.getCode()) {
            List<ApplierTblV2> applierTbls = applierTblDao.queryByResourceIds(resourceIds);
            applierMap = applierTbls.stream().collect(Collectors.groupingBy(ApplierTblV2::getResourceId, Collectors.counting()));
        }

        List<ResourceView> resourceViews = buildResourceViews(resourceTbls, replicatorMap, applierMap);
        if (CollectionUtils.isEmpty(resourceViews) && !tag.equals(ResourceTagEnum.COMMON.getName())) {
            return getResourceViews(dcIds, type, ResourceTagEnum.COMMON.getName());
        }
        Collections.sort(resourceViews);
        return resourceViews;
    }

    public static void main(String[] args) {
        List<ResourceView> resourceViews = new ArrayList<>();
        Random random = new Random();
        for (int i = 10; i > 0; i--) {
            ResourceView view = new ResourceView();
            view.setInstanceNum(Long.valueOf(random.nextInt(100)));
            resourceViews.add(view);
        }
        Collections.sort(resourceViews);
        resourceViews.forEach(e -> System.out.println(e.getInstanceNum()));
    }

    private List<ResourceView> buildResourceViews(List<ResourceTbl> resourceTbls, Map<Long, Long> replicatorMap, Map<Long, Long> applierMap) {
        List<ResourceView> views = resourceTbls.stream().map(source -> {
            ResourceView target = new ResourceView();
            target.setResourceId(source.getId());
            target.setIp(source.getIp());
            target.setActive(source.getActive());
            target.setAz(source.getAz());
            target.setType(source.getType());
            if (source.getType() == ModuleEnum.REPLICATOR.getCode()) {
                target.setInstanceNum(replicatorMap.getOrDefault(source.getId(), 0L));
            } else if (source.getType() == ModuleEnum.APPLIER.getCode()) {
                target.setInstanceNum(applierMap.getOrDefault(source.getId(), 0L));
            }
            return target;
        }).collect(Collectors.toList());
        return views;
    }

    private void checkResourceBuildParam(ResourceBuildParam param) {
        PreconditionUtils.checkString(param.getIp(), "ip requires not empty!");
        PreconditionUtils.checkString(param.getType(), "type requires not empty!");
        PreconditionUtils.checkId(param.getDcId(), "dc requires not empty!");
        PreconditionUtils.checkString(param.getAz(), "AZ requires not empty!");
    }
}
