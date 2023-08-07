package com.ctrip.framework.drc.console.service.v2.impl;

import com.ctrip.framework.drc.console.config.ConsoleConfig;
import com.ctrip.framework.drc.console.dao.*;
import com.ctrip.framework.drc.console.dao.entity.*;
import com.ctrip.framework.drc.console.dao.entity.v2.ApplierTblV2;
import com.ctrip.framework.drc.console.dao.entity.v2.MhaTblV2;
import com.ctrip.framework.drc.console.dao.v2.ApplierTblV2Dao;
import com.ctrip.framework.drc.console.dao.v2.MhaTblV2Dao;
import com.ctrip.framework.drc.console.enums.BooleanEnum;
import com.ctrip.framework.drc.console.enums.ResourceTagEnum;
import com.ctrip.framework.drc.console.param.v2.resource.ResourceBuildParam;
import com.ctrip.framework.drc.console.param.v2.resource.ResourceQueryParam;
import com.ctrip.framework.drc.console.service.MetaInfoService;
import com.ctrip.framework.drc.console.service.impl.DrcBuildServiceImpl;
import com.ctrip.framework.drc.console.service.impl.MetaInfoServiceImpl;
import com.ctrip.framework.drc.console.service.v2.ResourceService;
import com.ctrip.framework.drc.console.utils.ConsoleExceptionUtils;
import com.ctrip.framework.drc.console.utils.PreconditionUtils;
import com.ctrip.framework.drc.console.vo.v2.ResourceView;
import com.ctrip.framework.drc.core.http.PageReq;
import com.ctrip.framework.drc.core.monitor.enums.ModuleEnum;
import com.ctrip.framework.drc.core.server.utils.ThreadUtils;
import com.ctrip.platform.dal.dao.annotation.DalTransactional;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.util.CollectionUtils;

import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.TimeUnit;
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
    private MhaTblV2Dao mhaTblV2Dao;
    @Autowired
    private MhaTblDao mhaTblDao;
    @Autowired
    private ApplierGroupTblDao applierGroupTblDao;
    @Autowired
    private ReplicatorGroupTblDao replicatorGroupTblDao;
    @Autowired
    private DrcBuildServiceImpl drcBuildService;
    @Autowired
    private MetaInfoServiceImpl metaInfoService;

    private final ListeningExecutorService resourceExecutorService = MoreExecutors.listeningDecorator(ThreadUtils.newFixedThreadPool(10, "resourceExecutor"));
    private static final int TIME_OUT = 5;
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
        MhaTblV2 mhaTbl = mhaTblV2Dao.queryByMhaName(mhaName, BooleanEnum.FALSE.getCode());
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

    @Override
    @DalTransactional(logicDbName = "fxdrcmetadb_w")
    public int updateMhaTag() throws Exception {
        List<MhaTblV2> mhaTblV2s = mhaTblV2Dao.queryAllList();
        List<MhaTbl> mhaTbls = mhaTblDao.queryAllList();
        List<ReplicatorGroupTbl> replicatorGroupTbls = replicatorGroupTblDao.queryAllList();
        List<ApplierGroupTbl> applierGroupTbls = applierGroupTblDao.queryAllList();
        List<ReplicatorTbl> replicatorTbls = replicatorTblDao.queryAllList();
        List<ApplierTblV2> applierTbls = applierTblDao.queryAllList();
        List<ResourceTbl> resourceTbls = resourceTblDao.queryAllList();

        Map<Long, MhaTbl> mhaTblMap = mhaTbls.stream().collect(Collectors.toMap(MhaTbl::getId, Function.identity()));
        Map<Long, Long> replicatorGroupMap = replicatorGroupTbls.stream().collect(Collectors.toMap(ReplicatorGroupTbl::getMhaId, ReplicatorGroupTbl::getId));
        Map<Long, Long> applierGroupMap = applierGroupTbls.stream().collect(Collectors.toMap(ApplierGroupTbl::getMhaId, ApplierGroupTbl::getId));
        Map<Long, ReplicatorTbl> replicatorMap = replicatorTbls.stream().collect(Collectors.toMap(ReplicatorTbl::getRelicatorGroupId, Function.identity(), (k1, k2) -> k1));
        Map<Long, ApplierTblV2> applierMap = applierTbls.stream().collect(Collectors.toMap(ApplierTblV2::getApplierGroupId, Function.identity(), (k1, k2) -> k1));
        Map<Long, String> resourceMap = resourceTbls.stream().collect(Collectors.toMap(ResourceTbl::getId, ResourceTbl::getTag));

        for (MhaTblV2 mhaTblV2 : mhaTblV2s) {
            String tag = ResourceTagEnum.COMMON.getName();
            MhaTbl mhaTbl = mhaTblMap.get(mhaTblV2.getId());
            Long replicatorGroupId = replicatorGroupMap.get(mhaTblV2.getId());
            Long applierGroupId = applierGroupMap.get(mhaTblV2.getId());

            if (replicatorGroupId != null && replicatorMap.containsKey(replicatorGroupId)) {
                ReplicatorTbl replicatorTbl = replicatorMap.get(replicatorGroupId);
                String replicatorTag = resourceMap.get(replicatorTbl.getResourceId());
                if (!replicatorTag.equals(tag)) {
                    tag = replicatorTag;
                }
            }

            if (applierGroupId != null && applierMap.containsKey(applierGroupId)) {
                ApplierTblV2 applierTblV2 = applierMap.get(applierGroupId);
                String applierTag = resourceMap.get(applierTblV2.getResourceId());
                if (!applierTag.equals(ResourceTagEnum.COMMON.getName())) {
                    tag = applierTag;
                }
            }

            mhaTblV2.setTag(tag);
            mhaTbl.setTag(tag);
        }

        mhaTblV2Dao.update(mhaTblV2s);
        mhaTblDao.update(mhaTbls);

        return mhaTbls.size();
    }

    @Override
    public int offlineReplicatorWithSameAz(List<Long> replicatorGroupIds) throws Exception {
        logger.info("offlineReplicatorWithSameAz replicatorGroupIds: {}", replicatorGroupIds);
        List<ReplicatorTbl> replicatorTbls = replicatorTblDao.queryAllList();
        Map<Long, List<ReplicatorTbl>> replicatorMap = replicatorTbls.stream().collect(Collectors.groupingBy(ReplicatorTbl::getRelicatorGroupId));

        List<ReplicatorTbl> updateTbls = new ArrayList<>();
        for (long replicatorGroupId : replicatorGroupIds) {
            List<ReplicatorTbl> replicators = replicatorMap.get(replicatorGroupId);
            if (replicators.size() != 2) {
                continue;
            }
            ReplicatorTbl replicatorTbl = replicators.stream().filter(e -> e.getMaster() == BooleanEnum.FALSE.getCode()).findFirst().orElse(null);
            if (replicatorTbl != null) {
                replicatorTbl.setDeleted(BooleanEnum.TRUE.getCode());
                updateTbls.add(replicatorTbl);
            }
        }

        if (!CollectionUtils.isEmpty(updateTbls)) {
            replicatorTblDao.update(updateTbls);
        }
        return updateTbls.size();
    }

    @Override
    public int offlineApplierWithSameAz(List<Long> applierGroupIds) throws Exception {
        logger.info("offlineApplierWithSameAz applierGroupIds: {}", applierGroupIds);
        List<ApplierTblV2> applierTbls = applierTblDao.queryAllList();
        Map<Long, List<ApplierTblV2>> applierMap = applierTbls.stream().collect(Collectors.groupingBy(ApplierTblV2::getApplierGroupId));

        List<ApplierTblV2> updateTbls = new ArrayList<>();
        for (long applierGroupId : applierGroupIds) {
            List<ApplierTblV2> appliers = applierMap.get(applierGroupId);
            if (appliers.size() != 2) {
                continue;
            }
            ApplierTblV2 applierTblV2 = appliers.get(1);
            applierTblV2.setDeleted(BooleanEnum.TRUE.getCode());
            updateTbls.add(applierTblV2);
        }

        if (!CollectionUtils.isEmpty(updateTbls)) {
            applierTblDao.update(updateTbls);
        }
        return updateTbls.size();
    }

    @Override
    public int onlineReplicatorWithSameAz(List<Long> replicatorGroupIds) throws Exception {
        logger.info("onlineReplicatorWithSameAz replicatorGroupIds: {}", replicatorGroupIds);
        List<ReplicatorGroupTbl> replicatorGroupTbls = replicatorGroupTblDao.queryAllList();
        List<MhaTblV2> mhaTblV2s = mhaTblV2Dao.queryAllList();
        List<ReplicatorTbl> replicatorTbls = replicatorTblDao.queryAllList();
        List<ResourceTbl> resourceTbls = resourceTblDao.queryAllList();
        Map<Long, List<ReplicatorTbl>> replicatorMap = replicatorTbls.stream().collect(Collectors.groupingBy(ReplicatorTbl::getRelicatorGroupId));
        Map<Long, String> mhaMap = mhaTblV2s.stream().collect(Collectors.toMap(MhaTblV2::getId, MhaTblV2::getMhaName));
        Map<Long, Long> replicatorGroupMap = replicatorGroupTbls.stream().collect(Collectors.toMap(ReplicatorGroupTbl::getId, ReplicatorGroupTbl::getMhaId));
        Map<Long, String> resourceMap = resourceTbls.stream().collect(Collectors.toMap(ResourceTbl::getId, ResourceTbl::getIp));

        List<ListenableFuture<Pair<Long, Long>>> futures = new ArrayList<>();
        for (long replicatorGroupId : replicatorGroupIds) {
            List<ReplicatorTbl> replicators = replicatorMap.get(replicatorGroupId);
            if (CollectionUtils.isEmpty(replicators) || replicators.size() == 2) {
                continue;
            }
            long mhaId = replicatorGroupMap.get(replicatorGroupId);
            String mhaName = mhaMap.get(mhaId);
            String selectedIp = resourceMap.get(replicators.get(0).getResourceId());
            ListenableFuture<Pair<Long, Long>> future = resourceExecutorService.submit(() ->
                    getSecondResource(replicatorGroupId, mhaName, ModuleEnum.REPLICATOR.getCode(), Lists.newArrayList(selectedIp)));
            futures.add(future);
        }

        List<Long> failReplicatorList = new ArrayList<>();
        List<ReplicatorTbl> insertTbls = new ArrayList<>();
        for (ListenableFuture<Pair<Long, Long>> future : futures) {
            try {
                Pair<Long, Long> resultPair = future.get(TIME_OUT, TimeUnit.SECONDS);
                long replicatorGroupId = resultPair.getLeft();
                if (resultPair.getRight() == null) {
                    failReplicatorList.add(replicatorGroupId);
                    continue;
                }

                long resourceId = resultPair.getRight();
                String ip = resourceMap.get(resourceId);
                long mhaId = replicatorGroupMap.get(replicatorGroupId);
                String mhaName = mhaMap.get(mhaId);
                String replicatorGtid = drcBuildService.getNativeGtid(mhaName);

                ReplicatorTbl secondReplicator = buildReplicatorTbl(replicatorGroupId, resourceId, ip, replicatorGtid);
                insertTbls.add(secondReplicator);
            } catch (Exception e) {
                logger.error("onlineReplicatorWithSameAz fail", e);
                throw ConsoleExceptionUtils.message(e.getMessage());
            }
        }

        if (!CollectionUtils.isEmpty(insertTbls)) {
            replicatorTblDao.insert(insertTbls);
        }
        return insertTbls.size();
    }

    private ReplicatorTbl buildReplicatorTbl(long replicatorGroupId, long resourceId, String ip, String replicatorInitGtid) throws Exception {
        ReplicatorTbl replicatorTbl = new ReplicatorTbl();
        replicatorTbl.setRelicatorGroupId(replicatorGroupId);
        replicatorTbl.setGtidInit(replicatorInitGtid);
        replicatorTbl.setResourceId(resourceId);
        replicatorTbl.setPort(ConsoleConfig.DEFAULT_REPLICATOR_PORT);
        replicatorTbl.setApplierPort(metaInfoService.findAvailableApplierPort(ip));
        replicatorTbl.setMaster(BooleanEnum.FALSE.getCode());
        replicatorTbl.setDeleted(BooleanEnum.FALSE.getCode());

        return replicatorTbl;
    }

    private Pair<Long, Long> getSecondResource(long groupId, String mhaName, int type, List<String> selectedIps) throws Exception {
        List<ResourceView> resourceViews = autoConfigureResource(mhaName, type, selectedIps);
        Long resourceId = resourceViews.stream().filter(e -> !selectedIps.contains(e.getIp())).map(ResourceView::getResourceId).findFirst().orElse(null);
        return Pair.of(groupId, resourceId);
    }

    @Override
    public int onlineApplierWithSameAz(List<Long> applierGroupIds) throws Exception {
        return 0;
    }

    @Override
    public List<Long> getReplicatorGroupIdsWithSameAz() throws Exception {
        List<ReplicatorTbl> replicatorTbls = replicatorTblDao.queryAllList();
        List<ResourceTbl> resourceTbls = resourceTblDao.queryAllList();
        Map<Long, String> resourceMap = resourceTbls.stream().collect(Collectors.toMap(ResourceTbl::getId, ResourceTbl::getAz));
        Map<Long, List<ReplicatorTbl>> replicatorMap = replicatorTbls.stream().collect(Collectors.groupingBy(ReplicatorTbl::getRelicatorGroupId));

        List<Long> replicatorGroupIds = new ArrayList<>();
        replicatorMap.forEach((replicatorGroupId, replicators) -> {
            if (replicators.size() == 2) {
                String firstAz = resourceMap.get(replicators.get(0).getResourceId());
                String secondAz = resourceMap.get(replicators.get(1).getResourceId());
                if (firstAz.equalsIgnoreCase(secondAz)) {
                    replicatorGroupIds.add(replicatorGroupId);
                }
            }
        });

        logger.info("getReplicatorGroupIdsWithSameAz replicatorGroupIds: {}", replicatorGroupIds);
        return replicatorGroupIds;
    }

    @Override
    public List<Long> getApplierGroupIdsWithSameAz() throws Exception {
        List<ApplierTblV2> applierTblV2ss = applierTblDao.queryAllList();
        List<ResourceTbl> resourceTbls = resourceTblDao.queryAllList();
        Map<Long, String> resourceMap = resourceTbls.stream().collect(Collectors.toMap(ResourceTbl::getId, ResourceTbl::getAz));
        Map<Long, List<ApplierTblV2>> applierMap = applierTblV2ss.stream().collect(Collectors.groupingBy(ApplierTblV2::getApplierGroupId));

        List<Long> applierGroupIds = new ArrayList<>();
        applierMap.forEach((applierGroupId, appliers) -> {
            if (appliers.size() == 2) {
                String firstAz = resourceMap.get(appliers.get(0).getResourceId());
                String secondAz = resourceMap.get(appliers.get(1).getResourceId());
                if (firstAz.equalsIgnoreCase(secondAz)) {
                    applierGroupIds.add(applierGroupId);
                }
            }
        });

        logger.info("getApplierGroupIdsWithSameAz applierGroupIds: {}", applierGroupIds);
        return applierGroupIds;
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
