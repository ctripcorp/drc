package com.ctrip.framework.drc.console.service.v2.resource;

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
import com.ctrip.framework.drc.console.param.v2.resource.ResourceSelectParam;
import com.ctrip.framework.drc.console.service.impl.DrcBuildServiceImpl;
import com.ctrip.framework.drc.console.service.impl.MetaInfoServiceImpl;
import com.ctrip.framework.drc.console.utils.ConsoleExceptionUtils;
import com.ctrip.framework.drc.console.vo.v2.ResourceView;
import com.ctrip.framework.drc.core.http.PageReq;
import com.ctrip.framework.drc.core.monitor.enums.ModuleEnum;
import com.ctrip.framework.drc.core.server.utils.ThreadUtils;
import com.ctrip.platform.dal.dao.DalHints;
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
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Created by dengquanliang
 * 2023/8/8 10:46
 */
@Service
@Lazy
public class ResourceMigrateServiceImpl implements ResourceMigrateService {
    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired
    private ResourceTblDao resourceTblDao;
    @Autowired
    private ReplicatorTblDao replicatorTblDao;
    @Autowired
    private ApplierTblV2Dao applierTblV2Dao;
    @Autowired
    private ApplierTblDao applierTblDao;
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
    private MessengerTblDao messengerTblDao;
    @Autowired
    private MessengerGroupTblDao messengerGroupTblDao;
    @Autowired
    private DrcBuildServiceImpl drcBuildService;
    @Autowired
    private MetaInfoServiceImpl metaInfoService;
    @Autowired
    private ResourceService resourceService;

    private final ListeningExecutorService resourceExecutorService = MoreExecutors.listeningDecorator(ThreadUtils.newFixedThreadPool(10, "resourceExecutor"));
    private static final int TIME_OUT = 5;
    private static final int THOUSAND = 1000;

    @Override
    public List<ResourceView> getResourceUnused(int type) throws Exception {
        ResourceQueryParam param = new ResourceQueryParam();
        param.setType(type);
        PageReq pageReq = new PageReq();
        pageReq.setPageSize(THOUSAND);
        param.setPageReq(pageReq);

        List<ResourceView> resourceViews = resourceService.getResourceView(param);
        List<ResourceView> resourcesUnused = resourceViews.stream().filter(e -> e.getInstanceNum() == 0L).collect(Collectors.toList());
        return resourcesUnused;
    }

    @Override
    public int deleteResourceUnused(List<String> ips) throws Exception {
        List<ResourceTbl> resourceTbls = resourceTblDao.queryByIps(ips);
        List<Long> resourceIds = resourceTbls.stream().map(ResourceTbl::getId).collect(Collectors.toList());
        List<ReplicatorTbl> replicators = replicatorTblDao.queryByResourceIds(resourceIds);
        List<ApplierTblV2> appliers = applierTblV2Dao.queryByResourceIds(resourceIds);

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
        List<ApplierTblV2> applierTbls = applierTblV2Dao.queryAllList();
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
    @DalTransactional(logicDbName = "fxdrcmetadb_w")
    public int offlineApplierWithSameAz(List<Long> applierGroupIds) throws Exception {
        logger.info("offlineApplierWithSameAz applierGroupIds: {}", applierGroupIds);
        List<ApplierTblV2> applierTblV2s = applierTblV2Dao.queryAllList();
        List<ApplierTbl> applierTbls = applierTblDao.queryAllList();
        Map<Long, List<ApplierTblV2>> applierV2Map = applierTblV2s.stream().collect(Collectors.groupingBy(ApplierTblV2::getApplierGroupId));
        Map<Long, ApplierTbl> applierMap = applierTbls.stream().collect(Collectors.toMap(ApplierTbl::getId, Function.identity()));

        List<ApplierTblV2> updateTblV2s = new ArrayList<>();
        List<ApplierTbl> updateTbls = new ArrayList<>();
        for (long applierGroupId : applierGroupIds) {
            List<ApplierTblV2> appliers = applierV2Map.get(applierGroupId);
            if (appliers.size() != 2) {
                continue;
            }
            ApplierTblV2 applierTblV2 = appliers.get(1);
            applierTblV2.setDeleted(BooleanEnum.TRUE.getCode());
            updateTblV2s.add(applierTblV2);

            ApplierTbl applierTbl = applierMap.get(applierTblV2.getId());
            applierTbl.setDeleted(BooleanEnum.FALSE.getCode());
            updateTbls.add(applierTbl);
        }

        if (updateTbls.size() != updateTblV2s.size()) {
            logger.error("updateTbls and updateTblV2s size not equal");
            throw ConsoleExceptionUtils.message("offlineApplierWithSameAz fail");
        }
        if (!CollectionUtils.isEmpty(updateTblV2s)) {
            applierTblV2Dao.update(updateTblV2s);
        }
        if (!CollectionUtils.isEmpty(updateTbls)) {
            applierTblDao.update(updateTbls);
        }
        return updateTblV2s.size();
    }

    @Override
    public int offlineMessengerWithSameAz(List<Long> messengerGroupIds) throws Exception {
        logger.info("offlineMessengerWithSameAz messengerGroupIds: {}", messengerGroupIds);
        List<MessengerTbl> messengerTbls = messengerTblDao.queryAllList();
        Map<Long, List<MessengerTbl>> messengerMap = messengerTbls.stream().collect(Collectors.groupingBy(MessengerTbl::getMessengerGroupId));

        List<MessengerTbl> updateTbls = new ArrayList<>();
        for (long messengerGroupId : messengerGroupIds) {
            List<MessengerTbl> messengers = messengerMap.get(messengerGroupId);
            if (messengers.size() != 2) {
                continue;
            }
            MessengerTbl messengerTbl = messengers.get(1);
            if (messengerTbl != null) {
                messengerTbl.setDeleted(BooleanEnum.TRUE.getCode());
                updateTbls.add(messengerTbl);
            }
        }

        if (!CollectionUtils.isEmpty(updateTbls)) {
            messengerTblDao.update(updateTbls);
        }
        return updateTbls.size();
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
        List<ApplierTblV2> applierTblV2ss = applierTblV2Dao.queryAllList();
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

    @Override
    public List<Long> getMessengerGroupIdsWithSameAz() throws Exception {
        List<MessengerTbl> messengerTbls = messengerTblDao.queryAllList();
        List<ResourceTbl> resourceTbls = resourceTblDao.queryAllList();
        Map<Long, String> resourceMap = resourceTbls.stream().collect(Collectors.toMap(ResourceTbl::getId, ResourceTbl::getAz));
        Map<Long, List<MessengerTbl>> messengerMap = messengerTbls.stream().collect(Collectors.groupingBy(MessengerTbl::getMessengerGroupId));

        List<Long> messengerGroupIds = new ArrayList<>();
        messengerMap.forEach((messengerGroupId, messengers) -> {
            if (messengers.size() == 2) {
                String firstAz = resourceMap.get(messengers.get(0).getResourceId());
                String secondAz = resourceMap.get(messengers.get(1).getResourceId());
                if (firstAz.equalsIgnoreCase(secondAz)) {
                    messengerGroupIds.add(messengerGroupId);
                }
            }
        });

        logger.info("getMessengerGroupIdsWithSameAz messengerGroupIds: {}", messengerGroupIds);
        return messengerGroupIds;
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

    @Override
    @DalTransactional(logicDbName = "fxdrcmetadb_w")
    public int onlineApplierWithSameAz(List<Long> applierGroupIds) throws Exception {
        logger.info("onlineApplierWithSameAz applierGroupIds: {}", applierGroupIds);
        List<ApplierGroupTbl> applierGroupTbls = applierGroupTblDao.queryAllList();
        List<MhaTblV2> mhaTblV2s = mhaTblV2Dao.queryAllList();
        List<ApplierTblV2> applierTblV2s = applierTblV2Dao.queryAllList();
        List<ResourceTbl> resourceTbls = resourceTblDao.queryAllList();
        Map<Long, List<ApplierTblV2>> applierMap = applierTblV2s.stream().collect(Collectors.groupingBy(ApplierTblV2::getApplierGroupId));
        Map<Long, String> mhaMap = mhaTblV2s.stream().collect(Collectors.toMap(MhaTblV2::getId, MhaTblV2::getMhaName));
        Map<Long, ApplierGroupTbl> applierGroupMap = applierGroupTbls.stream().collect(Collectors.toMap(ApplierGroupTbl::getId, Function.identity()));
        Map<Long, String> resourceMap = resourceTbls.stream().collect(Collectors.toMap(ResourceTbl::getId, ResourceTbl::getIp));

        List<ListenableFuture<Pair<Long, Long>>> futures = new ArrayList<>();
        for (long applierGroupId : applierGroupIds) {
            List<ApplierTblV2> appliers = applierMap.get(applierGroupId);
            if (CollectionUtils.isEmpty(appliers) || appliers.size() == 2) {
                continue;
            }
            ApplierGroupTbl applierGroupTbl = applierGroupMap.get(applierGroupId);
            long mhaId = applierGroupTbl.getMhaId();
            String mhaName = mhaMap.get(mhaId);
            String selectedIp = resourceMap.get(appliers.get(0).getResourceId());
            ListenableFuture<Pair<Long, Long>> future = resourceExecutorService.submit(() ->
                    getSecondResource(applierGroupId, mhaName, ModuleEnum.REPLICATOR.getCode(), Lists.newArrayList(selectedIp)));
            futures.add(future);
        }

        List<Long> failApplierGroupIds = new ArrayList<>();
        List<ApplierTbl> insertApplierTbls = new ArrayList<>();

        for (ListenableFuture<Pair<Long, Long>> future : futures) {
            try {
                Pair<Long, Long> resultPair = future.get(TIME_OUT, TimeUnit.SECONDS);
                long applierGroupId = resultPair.getLeft();
                if (resultPair.getRight() == null) {
                    failApplierGroupIds.add(applierGroupId);
                    continue;
                }

                ApplierGroupTbl applierGroupTbl = applierGroupMap.get(applierGroupId);
                String applierGtid = applierGroupTbl.getGtidExecuted();
                long resourceId = resultPair.getRight();
                ApplierTbl applierTbl = buildApplier(applierGroupId, resourceId, applierGtid);
                insertApplierTbls.add(applierTbl);
            } catch (Exception e) {
                logger.error("onlineApplierWithSameAz fail", e);
                throw ConsoleExceptionUtils.message(e.getMessage());
            }
        }

        if (!CollectionUtils.isEmpty(insertApplierTbls)) {
            logger.info("onlineApplierWithSameAz insertApplierTbls:{}", insertApplierTbls);
            applierTblDao.batchInsertWithReturnId(insertApplierTbls);

            List<ApplierTblV2> insertApplierTblV2s = insertApplierTbls.stream().map(source -> {
                ApplierTblV2 target = new ApplierTblV2();
                target.setId(source.getId());
                target.setResourceId(source.getResourceId());
                target.setPort(source.getPort());
                target.setMaster(source.getMaster());
                target.setDeleted(BooleanEnum.FALSE.getCode());
                target.setApplierGroupId(source.getApplierGroupId());
                return target;
            }).collect(Collectors.toList());

            logger.info("onlineApplierWithSameAz insertApplierTblV2s:{}", insertApplierTblV2s);
            applierTblV2Dao.insert(new DalHints().enableIdentityInsert(), insertApplierTblV2s);
        }
        return insertApplierTbls.size();
    }

    @Override
    public int onlineMessengerWithSameAz(List<Long> messengerGroupIds) throws Exception {
        logger.info("onlineMessengerWithSameAz messengerGroupIds: {}", messengerGroupIds);
        List<MessengerGroupTbl> messengerGroupTbls = messengerGroupTblDao.queryAllList();
        List<MhaTblV2> mhaTblV2s = mhaTblV2Dao.queryAllList();
        List<MessengerTbl> messengerTbls = messengerTblDao.queryAllList();
        List<ResourceTbl> resourceTbls = resourceTblDao.queryAllList();
        Map<Long, List<MessengerTbl>> messengerMap = messengerTbls.stream().collect(Collectors.groupingBy(MessengerTbl::getMessengerGroupId));
        Map<Long, String> mhaMap = mhaTblV2s.stream().collect(Collectors.toMap(MhaTblV2::getId, MhaTblV2::getMhaName));
        Map<Long, MessengerGroupTbl> messengerGroupTblMap = messengerGroupTbls.stream().collect(Collectors.toMap(MessengerGroupTbl::getId, Function.identity()));
        Map<Long, String> resourceMap = resourceTbls.stream().collect(Collectors.toMap(ResourceTbl::getId, ResourceTbl::getIp));

        List<ListenableFuture<Pair<Long, Long>>> futures = new ArrayList<>();
        for (long messengerGroupId : messengerGroupIds) {
            List<MessengerTbl> messengers = messengerMap.get(messengerGroupId);
            if (CollectionUtils.isEmpty(messengers) || messengers.size() == 2) {
                continue;
            }
            MessengerGroupTbl messengerGroupTbl = messengerGroupTblMap.get(messengerGroupId);
            long mhaId = messengerGroupTbl.getMhaId();
            String mhaName = mhaMap.get(mhaId);
            String selectedIp = resourceMap.get(messengers.get(0).getResourceId());
            ListenableFuture<Pair<Long, Long>> future = resourceExecutorService.submit(() ->
                    getSecondResource(messengerGroupId, mhaName, ModuleEnum.REPLICATOR.getCode(), Lists.newArrayList(selectedIp)));
            futures.add(future);
        }

        List<Long> failMessengerGroupIds = new ArrayList<>();
        List<MessengerTbl> insertMessengerTbls = new ArrayList<>();

        for (ListenableFuture<Pair<Long, Long>> future : futures) {
            try {
                Pair<Long, Long> resultPair = future.get(TIME_OUT, TimeUnit.SECONDS);
                long messengerGroupId = resultPair.getLeft();
                if (resultPair.getRight() == null) {
                    failMessengerGroupIds.add(messengerGroupId);
                    continue;
                }

                long resourceId = resultPair.getRight();
                MessengerTbl messengerTbl = buildMessengerTbl(messengerGroupId, resourceId);
                insertMessengerTbls.add(messengerTbl);
            } catch (Exception e) {
                logger.error("onlineMessengerWithSameAz fail", e);
                throw ConsoleExceptionUtils.message(e.getMessage());
            }
        }

        if (!CollectionUtils.isEmpty(insertMessengerTbls)) {
            logger.info("onlineMessengerWithSameAz insertMessengerTbls: {}", insertMessengerTbls);
            messengerTblDao.insert(insertMessengerTbls);
        }
        return insertMessengerTbls.size();
    }

    private MessengerTbl buildMessengerTbl(long messengerGroupId, long resourceId) {
        MessengerTbl messengerTbl = new MessengerTbl();
        messengerTbl.setResourceId(resourceId);
        messengerTbl.setMessengerGroupId(messengerGroupId);
        messengerTbl.setPort(ConsoleConfig.DEFAULT_APPLIER_PORT);
        messengerTbl.setDeleted(BooleanEnum.FALSE.getCode());

        return messengerTbl;
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

    private ApplierTbl buildApplier(long applierGroupId, long resourceId, String applierGtid) {
        ApplierTbl applierTbl = new ApplierTbl();
        applierTbl.setPort(ConsoleConfig.DEFAULT_APPLIER_PORT);
        applierTbl.setGtidInit(applierGtid);
        applierTbl.setApplierGroupId(applierGroupId);
        applierTbl.setResourceId(resourceId);
        applierTbl.setDeleted(BooleanEnum.FALSE.getCode());
        applierTbl.setMaster(BooleanEnum.FALSE.getCode());

        return applierTbl;
    }

    private Pair<Long, Long> getSecondResource(long groupId, String mhaName, int type, List<String> selectedIps) throws Exception {
        List<ResourceView> resourceViews = resourceService.autoConfigureResource(new ResourceSelectParam(mhaName, type, selectedIps));
        Long resourceId = resourceViews.stream().filter(e -> !selectedIps.contains(e.getIp())).map(ResourceView::getResourceId).findFirst().orElse(null);
        return Pair.of(groupId, resourceId);
    }
}
