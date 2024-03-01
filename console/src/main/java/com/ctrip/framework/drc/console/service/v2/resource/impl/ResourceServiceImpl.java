package com.ctrip.framework.drc.console.service.v2.resource.impl;

import com.ctrip.framework.drc.console.config.DefaultConsoleConfig;
import com.ctrip.framework.drc.console.dao.*;
import com.ctrip.framework.drc.console.dao.entity.*;
import com.ctrip.framework.drc.console.dao.entity.v2.*;
import com.ctrip.framework.drc.console.dao.entity.v3.ApplierGroupTblV3;
import com.ctrip.framework.drc.console.dao.entity.v3.ApplierTblV3;
import com.ctrip.framework.drc.console.dao.entity.v3.MessengerTblV3;
import com.ctrip.framework.drc.console.dao.entity.v3.MhaDbReplicationTbl;
import com.ctrip.framework.drc.console.dao.v2.*;
import com.ctrip.framework.drc.console.dao.v3.ApplierGroupTblV3Dao;
import com.ctrip.framework.drc.console.dao.v3.ApplierTblV3Dao;
import com.ctrip.framework.drc.console.dao.v3.MessengerTblV3Dao;
import com.ctrip.framework.drc.console.dao.v3.MhaDbReplicationTblDao;
import com.ctrip.framework.drc.console.dto.v3.DbApplierDto;
import com.ctrip.framework.drc.console.enums.BooleanEnum;
import com.ctrip.framework.drc.console.enums.ResourceTagEnum;
import com.ctrip.framework.drc.console.exception.ConsoleException;
import com.ctrip.framework.drc.console.param.v2.resource.*;
import com.ctrip.framework.drc.console.service.v2.DbDrcBuildService;
import com.ctrip.framework.drc.console.service.v2.resource.ResourceService;
import com.ctrip.framework.drc.console.utils.ConsoleExceptionUtils;
import com.ctrip.framework.drc.console.utils.PreconditionUtils;
import com.ctrip.framework.drc.console.vo.v2.MhaDbReplicationView;
import com.ctrip.framework.drc.console.vo.v2.MhaReplicationView;
import com.ctrip.framework.drc.console.vo.v2.ResourceView;
import com.ctrip.framework.drc.core.monitor.enums.ModuleEnum;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.sql.SQLException;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;


/**
 * Created by dengquanliang
 * 2023/8/3 16:18
 */
@Service
public class ResourceServiceImpl implements ResourceService {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired
    private ResourceTblDao resourceTblDao;
    @Autowired
    private ReplicatorTblDao replicatorTblDao;
    @Autowired
    private ApplierTblV2Dao applierTblDao;
    @Autowired
    private ApplierTblV3Dao dbApplierTblDao;
    @Autowired
    private DcTblDao dcTblDao;
    @Autowired
    private MhaTblV2Dao mhaTblV2Dao;
    @Autowired
    private MessengerTblDao messengerTblDao;
    @Autowired
    private MessengerTblV3Dao dbMessengerTblDao;
    @Autowired
    private ReplicatorGroupTblDao replicatorGroupTblDao;
    @Autowired
    private ApplierGroupTblV2Dao applierGroupTblDao;
    @Autowired
    private ApplierGroupTblV3Dao dbApplierGroupTblDao;
    @Autowired
    private MhaReplicationTblDao mhaReplicationTblDao;
    @Autowired
    private MhaDbReplicationTblDao mhaDbReplicationTblDao;
    @Autowired
    private MhaDbMappingTblDao mhaDbMappingTblDao;
    @Autowired
    private DbTblDao dbTblDao;
    @Autowired
    private MessengerGroupTblDao messengerGroupTblDao;
    @Autowired
    private DbDrcBuildService dbDrcBuildService;
    @Autowired
    private DefaultConsoleConfig consoleConfig;

    @Override
    public void configureResource(ResourceBuildParam param) throws Exception {
        checkResourceBuildParam(param);
        DcTbl dcTbl = dcTblDao.queryByDcName(param.getDcName());
        if (dcTbl == null) {
            throw ConsoleExceptionUtils.message("dc: " + param.getDcName() + " not exist");
        }
        ResourceTbl resourceTbl = new ResourceTbl();
        resourceTbl.setIp(param.getIp());
        resourceTbl.setDcId(dcTbl.getId());
        resourceTbl.setAz(param.getAz());
        resourceTbl.setTag(param.getTag());
        resourceTbl.setDeleted(BooleanEnum.FALSE.getCode());
        resourceTbl.setActive(BooleanEnum.TRUE.getCode());

        ModuleEnum module = ModuleEnum.getModuleEnum(param.getType());
        resourceTbl.setAppId(module.getAppId());
        resourceTbl.setType(module.getCode());

        ResourceTbl existResourceTbl = resourceTblDao.queryByIp(param.getIp());
        if (existResourceTbl != null) {
            if (existResourceTbl.getDeleted().equals(BooleanEnum.TRUE.getCode())) {
                resourceTbl.setId(existResourceTbl.getId());
                resourceTblDao.update(resourceTbl);
                return;
            } else {
                throw ConsoleExceptionUtils.message(String.format("ip :%s already exist!", param.getIp()));
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
        if (resourceTbl.getType().equals(ModuleEnum.REPLICATOR.getCode())) {
            List<ReplicatorTbl> replicatorTbls = replicatorTblDao.queryByResourceIds(Lists.newArrayList(resourceId));
            if (!CollectionUtils.isEmpty(replicatorTbls)) {
                throw ConsoleExceptionUtils.message("resource is in use, cannot offline!");
            }
        } else if (resourceTbl.getType().equals(ModuleEnum.APPLIER.getCode())) {
            List<ApplierTblV2> applierTbls = applierTblDao.queryByResourceIds(Lists.newArrayList(resourceId));
            List<MessengerTbl> messengerTbls = messengerTblDao.queryByResourceIds(Lists.newArrayList(resourceId));
            List<MessengerTblV3> messengerTblsV3 = dbMessengerTblDao.queryByResourceIds(Lists.newArrayList(resourceId));
            if (!CollectionUtils.isEmpty(applierTbls) || !CollectionUtils.isEmpty(messengerTbls) || !CollectionUtils.isEmpty(messengerTblsV3)) {
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
        logger.info("getResourceView queryParam: {}", param);
        if (StringUtils.isNotBlank(param.getRegion())) {
            List<Long> dcIds = dcTblDao.queryByRegionName(param.getRegion()).stream().map(DcTbl::getId).collect(Collectors.toList());
            if (CollectionUtils.isEmpty(dcIds)) {
                return new ArrayList<>();
            }
            param.setDcIds(dcIds);
        }
        List<ResourceTbl> resourceTbls = resourceTblDao.queryByParam(param);
        if (CollectionUtils.isEmpty(resourceTbls)) {
            return new ArrayList<>();
        }

        List<Long> resourceIds = resourceTbls.stream().map(ResourceTbl::getId).collect(Collectors.toList());
        List<ReplicatorTbl> replicatorTbls = replicatorTblDao.queryByResourceIds(resourceIds);
        List<ApplierTblV2> applierTbls = applierTblDao.queryByResourceIds(resourceIds);
        List<MessengerTbl> messengerTbls = messengerTblDao.queryByResourceIds(resourceIds);
        List<MessengerTblV3> messengerTblsV3 = dbMessengerTblDao.queryByResourceIds(resourceIds);
        List<ApplierTblV3> applierTblV3s = dbApplierTblDao.queryByResourceIds(resourceIds);

        Map<Long, Long> replicatorMap = replicatorTbls.stream().collect(Collectors.groupingBy(ReplicatorTbl::getResourceId, Collectors.counting()));
        Map<Long, Long> applierMap = applierTbls.stream().collect(Collectors.groupingBy(ApplierTblV2::getResourceId, Collectors.counting()));
        Map<Long, Long> messengerMap = messengerTbls.stream().collect(Collectors.groupingBy(MessengerTbl::getResourceId, Collectors.counting()));
        Map<Long, Long> messengerV3Map = messengerTblsV3.stream().collect(Collectors.groupingBy(MessengerTblV3::getResourceId, Collectors.counting()));
        Map<Long, Long> dbApplierMap = applierTblV3s.stream().collect(Collectors.groupingBy(ApplierTblV3::getResourceId, Collectors.counting()));

        List<ResourceView> views = buildResourceViews(resourceTbls, replicatorMap, applierMap, dbApplierMap, messengerMap, messengerV3Map);
        return views;
    }

    @Override
    public List<ResourceView> getMhaAvailableResource(String mhaName, int type) throws SQLException {
        MhaTblV2 mhaTbl = mhaTblV2Dao.queryByMhaName(mhaName, BooleanEnum.FALSE.getCode());
        if (mhaTbl == null) {
            logger.info("mha: {} not exist", mhaName);
            return new ArrayList<>();
        }

        if (type != ModuleEnum.REPLICATOR.getCode() && type != ModuleEnum.APPLIER.getCode()) {
            logger.info("resource type: {} can only be replicator or applier", type);
            return new ArrayList<>();
        }
        DcTbl dcTbl = dcTblDao.queryById(mhaTbl.getDcId());
        List<Long> dcIds = dcTblDao.queryByRegionName(dcTbl.getRegionName()).stream().map(DcTbl::getId).collect(Collectors.toList());
        return getResourceViews(dcIds, dcTbl.getRegionName(), type, mhaTbl.getTag());
    }

    @Override
    public List<ResourceView> getMhaDbAvailableResource(String mhaName, int type) throws SQLException {
        if (type != ModuleEnum.APPLIER.getCode()) {
            logger.info("resource type: {} can only be applier", type);
            return new ArrayList<>();
        }
        MhaTblV2 mhaTbl = mhaTblV2Dao.queryByMhaName(mhaName, BooleanEnum.FALSE.getCode());
        if (mhaTbl == null) {
            logger.info("mha: {} not exist", mhaName);
            return new ArrayList<>();
        }


        DcTbl dcTbl = dcTblDao.queryById(mhaTbl.getDcId());
        List<Long> dcIds = dcTblDao.queryByRegionName(dcTbl.getRegionName()).stream().map(DcTbl::getId).collect(Collectors.toList());
        return getResourceViews(dcIds, dcTbl.getRegionName(), type, mhaTbl.getTag());
    }

    @Override
    public List<ResourceView> getMhaDbAvailableResourceWithUse(String srcMhaName, String dstMhaName, int type) throws Exception {
        List<ResourceView> resourceViews = getMhaDbAvailableResource(dstMhaName, type);

        List<ResourceView> resourceViewsInUse = new ArrayList<>();
        if (type == ModuleEnum.APPLIER.getCode()) {
            resourceViewsInUse.addAll(getDbAppliersInUse(srcMhaName, dstMhaName));
            resourceViewsInUse.addAll(getDbMessengersInUse(srcMhaName));
        }

        if (!CollectionUtils.isEmpty(resourceViewsInUse)) {
            List<Long> resourceIds = resourceViews.stream().map(ResourceView::getResourceId).collect(Collectors.toList());
            resourceViewsInUse.forEach(e -> {
                if (!resourceIds.contains(e.getResourceId())) {
                    resourceViews.add(e);
                }
            });
        }
        return resourceViews;
    }

    @Override
    public List<ResourceView> getMhaAvailableResourceWithUse(String mhaName, int type) throws Exception {
        List<ResourceView> resourceViews = getMhaAvailableResource(mhaName, type);

        MhaTblV2 mhaTbl = mhaTblV2Dao.queryByMhaName(mhaName, BooleanEnum.FALSE.getCode());
        List<ResourceView> resourceViewsInUse = new ArrayList<>();
        if (type == ModuleEnum.REPLICATOR.getCode()) {
            resourceViewsInUse.addAll(getReplicatorsInUse(mhaTbl.getId()));
        } else if (type == ModuleEnum.APPLIER.getCode()) {
            resourceViewsInUse.addAll(getAppliersInUse(mhaTbl.getId()));
            resourceViewsInUse.addAll(getMessengersInUse(mhaTbl.getId()));
        }

        if (!CollectionUtils.isEmpty(resourceViewsInUse)) {
            List<Long> resourceIds = resourceViews.stream().map(ResourceView::getResourceId).collect(Collectors.toList());
            resourceViewsInUse.forEach(e -> {
                if (!resourceIds.contains(e.getResourceId())) {
                    resourceViews.add(e);
                }
            });
        }
        return resourceViews;
    }

    private List<ResourceView> getReplicatorsInUse(long mhaId) throws Exception {
        ReplicatorGroupTbl replicatorGroupTbl = replicatorGroupTblDao.queryByMhaId(mhaId);
        if (replicatorGroupTbl == null) {
            return new ArrayList<>();
        }
        List<ReplicatorTbl> replicatorTbls = replicatorTblDao.queryByRGroupIds(Lists.newArrayList(replicatorGroupTbl.getId()), BooleanEnum.FALSE.getCode());
        if (CollectionUtils.isEmpty(replicatorTbls)) {
            return new ArrayList<>();
        }
        List<Long> resourceIds = replicatorTbls.stream().map(ReplicatorTbl::getResourceId).collect(Collectors.toList());
        List<ResourceTbl> resourceTbls = resourceTblDao.queryByIds(resourceIds);
        return buildResourceViews(resourceTbls);
    }

    private List<ResourceView> getAppliersInUse(long mhaId) throws Exception {
        List<MhaReplicationTbl> mhaReplicationTbls = mhaReplicationTblDao.queryByDstMhaId(mhaId);
        if (CollectionUtils.isEmpty(mhaReplicationTbls)) {
            return new ArrayList<>();
        }
        List<Long> mhaReplicationIds = mhaReplicationTbls.stream().map(MhaReplicationTbl::getId).collect(Collectors.toList());
        List<ApplierGroupTblV2> applierGroupTblV2s = applierGroupTblDao.queryByMhaReplicationIds(mhaReplicationIds);
        if (CollectionUtils.isEmpty(applierGroupTblV2s)) {
            return new ArrayList<>();
        }

        List<Long> applierGroupIds = applierGroupTblV2s.stream().map(ApplierGroupTblV2::getId).collect(Collectors.toList());
        List<ApplierTblV2> applierTblV2s = applierTblDao.queryByApplierGroupIds(applierGroupIds, BooleanEnum.FALSE.getCode());
        if (CollectionUtils.isEmpty(applierTblV2s)) {
            return new ArrayList<>();
        }

        List<Long> resourceIds = applierTblV2s.stream().map(ApplierTblV2::getResourceId).collect(Collectors.toList());
        List<ResourceTbl> resourceTbls = resourceTblDao.queryByIds(resourceIds);
        return buildResourceViews(resourceTbls);
    }

    private List<ResourceView> getMessengersInUse(long mhaId) throws Exception {
        MessengerGroupTbl messengerGroupTbl = messengerGroupTblDao.queryByMhaId(mhaId, BooleanEnum.FALSE.getCode());
        if (messengerGroupTbl == null) {
            return new ArrayList<>();
        }
        List<MessengerTbl> messengerTbls = messengerTblDao.queryByGroupId(messengerGroupTbl.getId());
        if (CollectionUtils.isEmpty(messengerTbls)) {
            return new ArrayList<>();
        }
        List<Long> resourceIds = messengerTbls.stream().map(MessengerTbl::getResourceId).collect(Collectors.toList());
        List<ResourceTbl> resourceTbls = resourceTblDao.queryByIds(resourceIds);
        return buildResourceViews(resourceTbls);
    }

    private List<ResourceView> getDbAppliersInUse(String srcMha, String dstMha) throws Exception {
        List<DbApplierDto> dbAppliers = dbDrcBuildService.getMhaDbAppliers(srcMha, dstMha);
        return getResourceViews(dbAppliers);
    }

    private List<ResourceView> getDbMessengersInUse(String mha) throws Exception {
        List<DbApplierDto> mhaDbMessengers = dbDrcBuildService.getMhaDbMessengers(mha);
        return getResourceViews(mhaDbMessengers);
    }
    private List<ResourceView> getResourceViews(List<DbApplierDto> dbAppliers) throws SQLException {
        List<String> ips = dbAppliers.stream()
                .map(DbApplierDto::getIps)
                .filter(Objects::nonNull)
                .flatMap(Collection::stream)
                .filter(StringUtils::isNotEmpty)
                .collect(Collectors.toList());
        List<ResourceTbl> resourceTbls = resourceTblDao.queryByIps(ips);
        return buildResourceViews(resourceTbls);
    }



    private List<ResourceView> buildResourceViews(List<ResourceTbl> resourceTbls) {
        List<ResourceView> resourceViews = resourceTbls.stream().map(source -> {
            ResourceView target = new ResourceView();
            target.setResourceId(source.getId());
            target.setIp(source.getIp());
            target.setAz(source.getAz());
            target.setTag(source.getTag());
            target.setType(source.getType());
            target.setActive(source.getActive());

            return target;
        }).collect(Collectors.toList());
        return resourceViews;
    }

    @Override
    public List<ResourceView> autoConfigureResource(ResourceSelectParam param) throws SQLException {
        List<ResourceView> resultViews = new ArrayList<>();
        List<ResourceView> resourceViews = getMhaAvailableResource(param.getMhaName(), param.getType());
        if (CollectionUtils.isEmpty(resourceViews)) {
            return resultViews;
        }

        List<String> selectedIps = param.getSelectedIps();
        if (!CollectionUtils.isEmpty(selectedIps) && selectedIps.size() == 1) {
            ResourceView firstResource = resourceViews.stream().filter(e -> e.getIp().equals(selectedIps.get(0))).findFirst().orElse(null);
            if (firstResource != null) {
                resultViews.add(firstResource);
            }
        } else if (!CollectionUtils.isEmpty(selectedIps)) {
            resourceViews = resourceViews.stream().filter(e -> selectedIps.contains(e.getIp())).collect(Collectors.toList());
        }

        setResourceView(resultViews, resourceViews);
        return resultViews;
    }

    @Override
    public List<ResourceView> autoConfigureMhaDbResource(DbResourceSelectParam param) throws SQLException {
        List<ResourceView> resultViews = new ArrayList<>();
        // only applier/messenger, select by dst mha
        List<ResourceView> resourceViews = getMhaDbAvailableResource(param.getDstMhaName(), param.getType());
        if (CollectionUtils.isEmpty(resourceViews)) {
            return resultViews;
        }

        List<String> selectedIps = param.getSelectedIps();
        if (!CollectionUtils.isEmpty(selectedIps) && selectedIps.size() == 1) {
            resourceViews.stream().filter(e -> e.getIp().equals(selectedIps.get(0))).findFirst().ifPresent(resultViews::add);
        } else if (!CollectionUtils.isEmpty(selectedIps)) {
            resourceViews = resourceViews.stream().filter(e -> selectedIps.contains(e.getIp())).collect(Collectors.toList());
        }

        setResourceView(resultViews, resourceViews);
        return resultViews;
    }

    @Override
    public List<ResourceView> handOffResource(ResourceSelectParam param) throws SQLException {
        if (CollectionUtils.isEmpty(param.getSelectedIps())) {
            return autoConfigureResource(param);
        }

        List<ResourceView> mhaAvailableResource = getMhaAvailableResource(param.getMhaName(), param.getType());
        return handOffResource(param.getSelectedIps(), mhaAvailableResource);
    }

    @Override
    public List<ResourceView> handOffResource(List<String> selectedIps, List<ResourceView> availableResource) {
        List<ResourceView> resultViews = new ArrayList<>();
        List<ResourceView> resourceViews = availableResource.stream()
                .filter(e -> !selectedIps.contains(e.getIp()))
                .collect(Collectors.toList());
        if (CollectionUtils.isEmpty(resourceViews)) {
            return resultViews;
        }

        setResourceView(resultViews, resourceViews);
        return resultViews;
    }

    @Override
    public List<String> queryMhaByReplicator(long resourceId) throws Exception {
        List<ReplicatorTbl> replicators = replicatorTblDao.queryByResourceIds(Lists.newArrayList(resourceId));
        if (CollectionUtils.isEmpty(replicators)) {
            return new ArrayList<>();
        }

        List<Long> replicatorGroupIds = replicators.stream().map(ReplicatorTbl::getRelicatorGroupId).distinct().collect(Collectors.toList());
        List<ReplicatorGroupTbl> replicatorGroupTbls = replicatorGroupTblDao.queryByIds(replicatorGroupIds);
        List<Long> mhaIds = replicatorGroupTbls.stream().map(ReplicatorGroupTbl::getMhaId).collect(Collectors.toList());
        List<MhaTblV2> mhaTblV2s = mhaTblV2Dao.queryByIds(mhaIds);
        List<String> mhaNames = mhaTblV2s.stream().map(MhaTblV2::getMhaName).collect(Collectors.toList());
        return mhaNames;
    }

    @Override
    public List<MhaReplicationView> queryMhaReplicationByApplier(long resourceId) throws Exception {
        List<ApplierTblV2> applierTblV2s = applierTblDao.queryByResourceIds(Lists.newArrayList(resourceId));
        if (CollectionUtils.isEmpty(applierTblV2s)) {
            return new ArrayList<>();
        }

        List<Long> applierGroupIds = applierTblV2s.stream().map(ApplierTblV2::getApplierGroupId).distinct().collect(Collectors.toList());
        List<ApplierGroupTblV2> applierGroupTblV2s = applierGroupTblDao.queryByIds(applierGroupIds);
        List<Long> mhaReplicationIds = applierGroupTblV2s.stream().map(ApplierGroupTblV2::getMhaReplicationId).collect(Collectors.toList());
        List<MhaReplicationTbl> mhaReplicationTbls = mhaReplicationTblDao.queryByIds(mhaReplicationIds);

        List<Long> mhaIds = new ArrayList<>();
        for (MhaReplicationTbl mhaReplicationTbl : mhaReplicationTbls) {
            mhaIds.add(mhaReplicationTbl.getSrcMhaId());
            mhaIds.add(mhaReplicationTbl.getDstMhaId());
        }

        List<MhaTblV2> mhaTblV2s = mhaTblV2Dao.queryByIds(mhaIds);
        List<DcTbl> dcTbls = dcTblDao.queryAllExist();
        Map<Long, MhaTblV2> mhaMap = mhaTblV2s.stream().collect(Collectors.toMap(MhaTblV2::getId, Function.identity()));
        Map<Long, String> dcMap = dcTbls.stream().collect(Collectors.toMap(DcTbl::getId, DcTbl::getDcName));

        List<MhaReplicationView> views = mhaReplicationTbls.stream().map(mhaReplicationTbl -> {
            MhaReplicationView view = new MhaReplicationView();
            MhaTblV2 srcMha = mhaMap.get(mhaReplicationTbl.getSrcMhaId());
            MhaTblV2 dstMha = mhaMap.get(mhaReplicationTbl.getDstMhaId());
            view.setSrcMhaName(srcMha.getMhaName());
            view.setSrcDcName(dcMap.get(srcMha.getDcId()));
            view.setDstMhaName(dstMha.getMhaName());
            view.setDstDcName(dcMap.get(dstMha.getDcId()));
            return view;
        }).collect(Collectors.toList());
        return views;
    }

    @Override
    public List<MhaDbReplicationView> queryMhaDbReplicationByApplier(long resourceId) throws Exception {
        List<ApplierTblV3> applierTblV3s = dbApplierTblDao.queryByResourceIds(Lists.newArrayList(resourceId));
        if (CollectionUtils.isEmpty(applierTblV3s)) {
            return new ArrayList<>();
        }
        List<ApplierGroupTblV3> applierGroupTblV3s = dbApplierGroupTblDao.queryByIds(applierTblV3s.stream().map(ApplierTblV3::getApplierGroupId).distinct().collect(Collectors.toList()));
        List<MhaDbReplicationTbl> mhaDbReplicationTbls = mhaDbReplicationTblDao.queryByIds(applierGroupTblV3s.stream().map(ApplierGroupTblV3::getMhaDbReplicationId).collect(Collectors.toList()));
        Set<Long> mhaDdMappingIds = new HashSet<>();
        for (MhaDbReplicationTbl mhaDbReplicationTbl : mhaDbReplicationTbls) {
            mhaDdMappingIds.add(mhaDbReplicationTbl.getSrcMhaDbMappingId());
            mhaDdMappingIds.add(mhaDbReplicationTbl.getDstMhaDbMappingId());
        }

        List<MhaDbMappingTbl> mhaDbMappingTbls = mhaDbMappingTblDao.queryByIds(Lists.newArrayList(mhaDdMappingIds));
        List<MhaTblV2> mhaTblV2s = mhaTblV2Dao.queryByIds(mhaDbMappingTbls.stream().map(MhaDbMappingTbl::getMhaId).distinct().collect(Collectors.toList()));
        List<DbTbl> dbTbls = dbTblDao.queryByIds(mhaDbMappingTbls.stream().map(MhaDbMappingTbl::getDbId).distinct().collect(Collectors.toList()));
        List<DcTbl> dcTbls = dcTblDao.queryAllExist();
        Map<Long, MhaTblV2> mhaMap = mhaTblV2s.stream().collect(Collectors.toMap(MhaTblV2::getId, Function.identity()));
        Map<Long, String> dcMap = dcTbls.stream().collect(Collectors.toMap(DcTbl::getId, DcTbl::getDcName));
        Map<Long, MhaDbMappingTbl> mhaDbMappingMap = mhaDbMappingTbls.stream().collect(Collectors.toMap(MhaDbMappingTbl::getId, Function.identity()));
        Map<Long, String> dbNameMap = dbTbls.stream().collect(Collectors.toMap(DbTbl::getId, DbTbl::getDbName));

        List<MhaDbReplicationView> views = mhaDbReplicationTbls.stream().map(source -> {
            MhaDbMappingTbl srcMhaDbMapping = mhaDbMappingMap.get(source.getSrcMhaDbMappingId());
            MhaDbMappingTbl dstMhaDbMapping = mhaDbMappingMap.get(source.getDstMhaDbMappingId());
            MhaTblV2 srcMha = mhaMap.get(srcMhaDbMapping.getMhaId());
            MhaTblV2 dstMha = mhaMap.get(dstMhaDbMapping.getMhaId());

            MhaDbReplicationView target = new MhaDbReplicationView();
            target.setDbName(dbNameMap.get(srcMhaDbMapping.getDbId()));
            target.setSrcMhaName(srcMha.getMhaName());
            target.setDstMhaName(dstMha.getMhaName());
            target.setSrcDcName(dcMap.get(srcMha.getDcId()));
            target.setDstDcName(dcMap.get(dstMha.getDcId()));

            return target;
        }).collect(Collectors.toList());
        return views;
    }

    @Override
    public List<String> queryMhaByMessenger(long resourceId) throws Exception {
        List<MessengerTbl> messengerTbls = messengerTblDao.queryByResourceIds(Lists.newArrayList(resourceId));
        if (CollectionUtils.isEmpty(messengerTbls)) {
            return new ArrayList<>();
        }

        List<Long> messengerGroupIds = messengerTbls.stream().map(MessengerTbl::getMessengerGroupId).collect(Collectors.toList());
        List<MessengerGroupTbl> messengerGroupTbls = messengerGroupTblDao.queryByIds(messengerGroupIds);
        List<Long> mhaIds = messengerGroupTbls.stream().map(MessengerGroupTbl::getMhaId).collect(Collectors.toList());
        List<MhaTblV2> mhaTblV2s = mhaTblV2Dao.queryByIds(mhaIds);
        return mhaTblV2s.stream().map(MhaTblV2::getMhaName).collect(Collectors.toList());
    }

    @Override
    public int migrateResource(String newIp, String oldIp, int type) throws Exception {
        if (type == ModuleEnum.REPLICATOR.getCode()) {
            return migrateReplicator(newIp, oldIp);
        } else if (type == ModuleEnum.APPLIER.getCode()) {
            return migrateApplier(newIp, oldIp);
        }
        throw ConsoleExceptionUtils.message("type not supported!");
    }

    @Override
    public void migrateResource(ResourceMigrateParam param) throws Exception {
        if (CollectionUtils.isEmpty(param.getResourceMigrateDtoList())) {
            return;
        }
        for (ResourceMigrateDto dto : param.getResourceMigrateDtoList()) {
            migrateResource(dto.getNewIp(), dto.getOldIp(), param.getType());
        }
    }

    private int migrateReplicator(String newIp, String oldIp) throws Exception {
        ResourceTbl newResource = resourceTblDao.queryByIp(newIp, BooleanEnum.FALSE.getCode());
        ResourceTbl oldResource = resourceTblDao.queryByIp(oldIp, BooleanEnum.FALSE.getCode());
        if (newResource == null || oldResource == null) {
            throw ConsoleExceptionUtils.message("newIp or oldIp not exist");
        }
        if (newResource.getActive().equals(BooleanEnum.FALSE.getCode())) {
            throw ConsoleExceptionUtils.message("newIp is active");
        }
        if (!newResource.getType().equals(oldResource.getType()) || !newResource.getType().equals(ModuleEnum.REPLICATOR.getCode())) {
            throw ConsoleExceptionUtils.message("newIp is not replicator");
        }
        List<ReplicatorTbl> replicatorTbls = replicatorTblDao.queryByResourceIds(Lists.newArrayList(oldResource.getId()));
        if (CollectionUtils.isEmpty(replicatorTbls)) {
            return 0;
        }

        replicatorTbls.forEach(e -> e.setResourceId(newResource.getId()));
        replicatorTblDao.update(replicatorTbls);
        return replicatorTbls.size();
    }

    private int migrateApplier(String newIp, String oldIp) throws Exception {
        ResourceTbl newResource = resourceTblDao.queryByIp(newIp, BooleanEnum.FALSE.getCode());
        ResourceTbl oldResource = resourceTblDao.queryByIp(oldIp, BooleanEnum.FALSE.getCode());
        if (newResource == null || oldResource == null) {
            throw ConsoleExceptionUtils.message("newIp or oldIp not exist");
        }
        if (newResource.getActive().equals(BooleanEnum.FALSE.getCode())) {
            throw ConsoleExceptionUtils.message("newIp is active");
        }
        if (!newResource.getType().equals(oldResource.getType()) || !newResource.getType().equals(ModuleEnum.APPLIER.getCode())) {
            throw ConsoleExceptionUtils.message("newIp is not replicator");
        }

        List<ApplierTblV2> applierTblV2s = applierTblDao.queryByResourceIds(Lists.newArrayList(oldResource.getId()));
        List<ApplierTblV3> applierTblV3s = dbApplierTblDao.queryByResourceIds(Lists.newArrayList(oldResource.getId()));
        List<MessengerTbl> messengerTbls = messengerTblDao.queryByResourceIds(Lists.newArrayList(oldResource.getId()));
        List<MessengerTblV3> messengerTblsV3 = dbMessengerTblDao.queryByResourceIds(Lists.newArrayList(oldResource.getId()));

        int result =  0;
        if (!CollectionUtils.isEmpty(applierTblV2s)) {
            applierTblV2s.forEach(e -> e.setResourceId(newResource.getId()));
            applierTblDao.update(applierTblV2s);
            result += applierTblV2s.size();
        }
        if (!CollectionUtils.isEmpty(applierTblV3s)) {
            applierTblV3s.forEach(e -> e.setResourceId(newResource.getId()));
            dbApplierTblDao.update(applierTblV3s);
            result += applierTblV3s.size();
        }
        if (!CollectionUtils.isEmpty(messengerTbls)) {
            messengerTbls.forEach(e -> e.setResourceId(newResource.getId()));
            messengerTblDao.update(messengerTbls);
            result += messengerTbls.size();
        }
        if (!CollectionUtils.isEmpty(messengerTblsV3)) {
            messengerTblsV3.forEach(e -> e.setResourceId(newResource.getId()));
            dbMessengerTblDao.update(messengerTblsV3);
            result += messengerTblsV3.size();
        }
        return result;
    }

    private void setResourceView(List<ResourceView> resultViews, List<ResourceView> resourceViews) {
        if (CollectionUtils.isEmpty(resultViews)) {
            resultViews.add(resourceViews.get(0));
        }
        ResourceView firstResource = resultViews.get(0);
        ResourceView secondResource = resourceViews.stream().filter(e -> !e.getAz().equals(firstResource.getAz())).findFirst().orElse(null);
        if (secondResource != null) {
            resultViews.add(secondResource);
        }
    }

    private List<ResourceView> getResourceViewsForDb(List<Long> dcIds, String region, int type, String tag) throws SQLException {
        List<ResourceTbl> resourceTbls = resourceTblDao.queryByDcAndTag(dcIds, tag, type, BooleanEnum.TRUE.getCode());
        List<Long> resourceIds = resourceTbls.stream().map(ResourceTbl::getId).collect(Collectors.toList());

        Map<Long, Long> replicatorMap = new HashMap<>();
        Map<Long, Long> applierMap;
        Map<Long, Long> messengerMap;
        if (type == ModuleEnum.APPLIER.getCode()) {
            List<ApplierTblV3> applierTbls = dbApplierTblDao.queryByResourceIds(resourceIds);
            List<MessengerTblV3> messengerTbls = dbMessengerTblDao.queryByResourceIds(resourceIds);
            applierMap = applierTbls.stream().collect(Collectors.groupingBy(ApplierTblV3::getResourceId, Collectors.counting()));
            messengerMap = messengerTbls.stream().collect(Collectors.groupingBy(MessengerTblV3::getResourceId, Collectors.counting()));
        } else {
            throw new ConsoleException("not supported type: " + type);
        }

        List<ResourceView> resourceViews = buildResourceViews(resourceTbls, replicatorMap, applierMap, messengerMap);
        if (CollectionUtils.isEmpty(resourceViews) && !tag.equals(ResourceTagEnum.COMMON.getName())) {
            return getResourceViews(dcIds, region, type, ResourceTagEnum.COMMON.getName());
        }
        Collections.sort(resourceViews);
        return resourceViews;
    }

    private List<ResourceView> getResourceViews(List<Long> dcIds, String region, int type, String tag) throws SQLException {
        List<ResourceTbl> resourceTbls = resourceTblDao.queryByDcAndTag(dcIds, tag, type, BooleanEnum.TRUE.getCode());
        List<Long> resourceIds = resourceTbls.stream().map(ResourceTbl::getId).collect(Collectors.toList());

        Map<Long, Long> replicatorMap = new HashMap<>();
        Map<Long, Long> applierMap = new HashMap<>();
        Map<Long, Long> messengerMap = new HashMap<>();
        Map<Long, Long> dbMessengerMap = new HashMap<>();
        Map<Long, Long> dbApplierMap = new HashMap<>();
        if (type == ModuleEnum.REPLICATOR.getCode()) {
            List<ReplicatorTbl> replicatorTbls = replicatorTblDao.queryByResourceIds(resourceIds);
            replicatorMap = replicatorTbls.stream().collect(Collectors.groupingBy(ReplicatorTbl::getResourceId, Collectors.counting()));
        } else if (type == ModuleEnum.APPLIER.getCode()) {
            List<ApplierTblV2> applierTbls = applierTblDao.queryByResourceIds(resourceIds);
            List<MessengerTbl> messengerTbls = messengerTblDao.queryByResourceIds(resourceIds);
            List<MessengerTblV3> messengerTblsV3 = dbMessengerTblDao.queryByResourceIds(resourceIds);
            List<ApplierTblV3> dbAplierTbls = dbApplierTblDao.queryByResourceIds(resourceIds);
            applierMap = applierTbls.stream().collect(Collectors.groupingBy(ApplierTblV2::getResourceId, Collectors.counting()));
            messengerMap = messengerTbls.stream().collect(Collectors.groupingBy(MessengerTbl::getResourceId, Collectors.counting()));
            dbMessengerMap = messengerTblsV3.stream().collect(Collectors.groupingBy(MessengerTblV3::getResourceId, Collectors.counting()));
            dbApplierMap = dbAplierTbls.stream().collect(Collectors.groupingBy(ApplierTblV3::getResourceId, Collectors.counting()));
        }

        List<ResourceView> resourceViews = buildResourceViews(resourceTbls, replicatorMap, applierMap, dbApplierMap, messengerMap, dbMessengerMap);
        if (CollectionUtils.isEmpty(resourceViews) && !tag.equals(ResourceTagEnum.COMMON.getName())) {
            return getResourceViews(dcIds, region, type, ResourceTagEnum.COMMON.getName());
        }

        String centerRegion = consoleConfig.getCenterRegion();
        if (!centerRegion.equals(region) && type == ModuleEnum.REPLICATOR.getCode()) {
            long replicatorMaxSize = consoleConfig.getReplicatorMaxSize();
            resourceViews = resourceViews.stream().filter(e -> e.getInstanceNum() < replicatorMaxSize).collect(Collectors.toList());
        }
        Collections.sort(resourceViews);
        return resourceViews;
    }

    private List<ResourceView> buildResourceViews(List<ResourceTbl> resourceTbls,
                                                  Map<Long, Long> replicatorMap,
                                                  Map<Long, Long> applierMap,
                                                  Map<Long, Long> dbApplierMap,
                                                  Map<Long, Long> messengerMap,
                                                  Map<Long, Long> dbMessengerMap) {
        List<ResourceView> views = resourceTbls.stream().map(source -> {
            ResourceView target = new ResourceView();
            target.setResourceId(source.getId());
            target.setIp(source.getIp());
            target.setActive(source.getActive());
            target.setAz(source.getAz());
            target.setType(source.getType());
            target.setTag(source.getTag());
            if (source.getType().equals(ModuleEnum.REPLICATOR.getCode())) {
                target.setInstanceNum(replicatorMap.getOrDefault(source.getId(), 0L));
            } else if (source.getType().equals(ModuleEnum.APPLIER.getCode())) {
                long applierNum = applierMap.getOrDefault(source.getId(), 0L);
                long messengerNum = messengerMap.getOrDefault(source.getId(), 0L);
                long dbApplierNum = dbApplierMap.getOrDefault(source.getId(), 0L);
                long dbMessengerNum = dbMessengerMap.getOrDefault(source.getId(), 0L);
                target.setInstanceNum(applierNum + messengerNum + dbApplierNum + dbMessengerNum);
            }
            return target;
        }).collect(Collectors.toList());
        return views;
    }

    private List<ResourceView> buildResourceViews(List<ResourceTbl> resourceTbls, Map<Long, Long> replicatorMap, Map<Long, Long> applierMap, Map<Long, Long> messengerMap) {
        List<ResourceView> views = resourceTbls.stream().map(source -> {
            ResourceView target = new ResourceView();
            target.setResourceId(source.getId());
            target.setIp(source.getIp());
            target.setActive(source.getActive());
            target.setAz(source.getAz());
            target.setType(source.getType());
            target.setTag(source.getTag());
            if (source.getType().equals(ModuleEnum.REPLICATOR.getCode())) {
                target.setInstanceNum(replicatorMap.getOrDefault(source.getId(), 0L));
            } else if (source.getType().equals(ModuleEnum.APPLIER.getCode())) {
                long applierNum = applierMap.getOrDefault(source.getId(), 0L);
                long messengerNum = messengerMap.getOrDefault(source.getId(), 0L);
                target.setInstanceNum(applierNum + messengerNum);
            }
            return target;
        }).collect(Collectors.toList());
        return views;
    }

    private void checkResourceBuildParam(ResourceBuildParam param) {
        PreconditionUtils.checkString(param.getIp(), "ip requires not empty!");
        PreconditionUtils.checkString(param.getType(), "type requires not empty!");
        PreconditionUtils.checkString(param.getDcName(), "dc requires not empty!");
        PreconditionUtils.checkString(param.getAz(), "AZ requires not empty!");
    }
}
