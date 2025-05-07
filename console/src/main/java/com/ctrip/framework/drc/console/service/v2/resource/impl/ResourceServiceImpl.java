package com.ctrip.framework.drc.console.service.v2.resource.impl;

import com.ctrip.framework.drc.console.aop.forward.PossibleRemote;
import com.ctrip.framework.drc.console.aop.forward.response.ApplierInfoApiRes;
import com.ctrip.framework.drc.console.aop.forward.response.MessengerInfoApiRes;
import com.ctrip.framework.drc.console.aop.forward.response.ReplicatorInfoApiRes;
import com.ctrip.framework.drc.console.config.ConsoleConfig;
import com.ctrip.framework.drc.console.config.DefaultConsoleConfig;
import com.ctrip.framework.drc.console.dao.*;
import com.ctrip.framework.drc.console.dao.entity.*;
import com.ctrip.framework.drc.console.dao.entity.v2.MhaDbMappingTbl;
import com.ctrip.framework.drc.console.dao.entity.v2.MhaTblV2;
import com.ctrip.framework.drc.console.dao.entity.v3.ApplierGroupTblV3;
import com.ctrip.framework.drc.console.dao.entity.v3.ApplierTblV3;
import com.ctrip.framework.drc.console.dao.entity.v3.MessengerTblV3;
import com.ctrip.framework.drc.console.dao.entity.v3.MhaDbReplicationTbl;
import com.ctrip.framework.drc.console.dao.v2.MhaDbMappingTblDao;
import com.ctrip.framework.drc.console.dao.v2.MhaTblV2Dao;
import com.ctrip.framework.drc.console.dao.v3.ApplierGroupTblV3Dao;
import com.ctrip.framework.drc.console.dao.v3.ApplierTblV3Dao;
import com.ctrip.framework.drc.console.dao.v3.MessengerTblV3Dao;
import com.ctrip.framework.drc.console.dao.v3.MhaDbReplicationTblDao;
import com.ctrip.framework.drc.console.dto.MhaInstanceGroupDto;
import com.ctrip.framework.drc.console.dto.v3.DbApplierDto;
import com.ctrip.framework.drc.console.enums.ApplierTypeEnum;
import com.ctrip.framework.drc.console.enums.BooleanEnum;
import com.ctrip.framework.drc.console.enums.ResourceTagEnum;
import com.ctrip.framework.drc.console.monitor.delay.config.v2.MetaProviderV2;
import com.ctrip.framework.drc.console.param.v2.resource.*;
import com.ctrip.framework.drc.console.service.impl.DalServiceImpl;
import com.ctrip.framework.drc.console.service.v2.DbDrcBuildService;
import com.ctrip.framework.drc.console.service.v2.MetaInfoServiceV2;
import com.ctrip.framework.drc.console.service.v2.MysqlServiceV2;
import com.ctrip.framework.drc.console.service.v2.resource.ResourceService;
import com.ctrip.framework.drc.console.utils.ConsoleExceptionUtils;
import com.ctrip.framework.drc.console.utils.PreconditionUtils;
import com.ctrip.framework.drc.console.vo.v2.*;
import com.ctrip.framework.drc.core.entity.*;
import com.ctrip.framework.drc.core.monitor.enums.ModuleEnum;
import com.ctrip.framework.drc.core.mq.MqType;
import com.ctrip.framework.drc.core.server.config.applier.dto.ApplierInfoDto;
import com.ctrip.framework.drc.core.server.config.applier.dto.FetcherInfoDto;
import com.ctrip.framework.drc.core.server.config.applier.dto.MessengerInfoDto;
import com.ctrip.framework.drc.core.server.config.replicator.dto.ReplicatorInfoDto;
import com.ctrip.framework.drc.core.server.utils.ThreadUtils;
import com.ctrip.framework.drc.core.service.inquirer.BatchInfoInquirer;
import com.ctrip.framework.foundation.Foundation;
import com.ctrip.platform.dal.dao.annotation.DalTransactional;
import com.ctrip.xpipe.api.monitor.EventMonitor;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
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
@Service
public class ResourceServiceImpl implements ResourceService {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired
    private ResourceTblDao resourceTblDao;
    @Autowired
    private ReplicatorTblDao replicatorTblDao;
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
    private ApplierGroupTblV3Dao dbApplierGroupTblDao;
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
    @Autowired
    private MysqlServiceV2 mysqlServiceV2;
    @Autowired
    private MetaInfoServiceV2 metaInfoService;
    @Autowired
    private DalServiceImpl dalService;
    @Autowired
    private MetaProviderV2 metaProviderV2;
    @Autowired
    private ResourceService resourceService;

    private BatchInfoInquirer batchInfoInquirer = BatchInfoInquirer.getInstance();

    private final ListeningExecutorService executorService = MoreExecutors.listeningDecorator(ThreadUtils.newFixedThreadPool(5, "migrateResource"));

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
    public void batchConfigureResource(ResourceBuildParam param) throws Exception {
        checkBatchResourceBuildParam(param);
        DcTbl dcTbl = dcTblDao.queryByDcName(param.getDcName());
        if (dcTbl == null) {
            throw ConsoleExceptionUtils.message("dc: " + param.getDcName() + " not exist");
        }

        List<ResourceTbl> existResourceTbls = resourceTblDao.queryByIps(param.getIps());
        if (!CollectionUtils.isEmpty(existResourceTbls)) {
            List<String> existIps = existResourceTbls.stream().map(ResourceTbl::getIp).collect(Collectors.toList());
            throw ConsoleExceptionUtils.message(String.format("ip :%s already exist!", Joiner.on(",").join(existIps)));
        }

        List<ResourceTbl> insertTbls = new ArrayList<>();
        for (String ip : param.getIps()) {
            ResourceTbl resourceTbl = new ResourceTbl();
            resourceTbl.setIp(ip);
            resourceTbl.setDcId(dcTbl.getId());
            resourceTbl.setAz(param.getAz());
            resourceTbl.setTag(param.getTag());
            resourceTbl.setDeleted(BooleanEnum.FALSE.getCode());
            resourceTbl.setActive(BooleanEnum.TRUE.getCode());

            ModuleEnum module = ModuleEnum.getModuleEnum(param.getType());
            resourceTbl.setAppId(module.getAppId());
            resourceTbl.setType(module.getCode());
            insertTbls.add(resourceTbl);
        }
        logger.info("batchInsert resource: {}", insertTbls);
        resourceTblDao.insert(insertTbls);
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
            List<ApplierTblV3> dbAppliers = dbApplierTblDao.queryByResourceIds(Lists.newArrayList(resourceId));
            List<MessengerTbl> messengerTbls = messengerTblDao.queryByResourceIds(Lists.newArrayList(resourceId));
            List<MessengerTblV3> messengerTblsV3 = dbMessengerTblDao.queryByResourceIds(Lists.newArrayList(resourceId));
            if (!CollectionUtils.isEmpty(dbAppliers)
                    || !CollectionUtils.isEmpty(messengerTbls) || !CollectionUtils.isEmpty(messengerTblsV3)) {
                throw ConsoleExceptionUtils.message("resource is in use, cannot offline!");
            }
        } else if (resourceTbl.getType().equals(ModuleEnum.MESSENGER.getCode())) {
            List<MessengerTbl> messengerTbls = messengerTblDao.queryByResourceIds(Lists.newArrayList(resourceId));
            List<MessengerTblV3> messengerTblsV3 = dbMessengerTblDao.queryByResourceIds(Lists.newArrayList(resourceId));
            if (!CollectionUtils.isEmpty(messengerTbls) || !CollectionUtils.isEmpty(messengerTblsV3)) {
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
        List<MessengerTbl> messengerTbls = messengerTblDao.queryByResourceIds(resourceIds);
        List<MessengerTblV3> messengerTblsV3 = dbMessengerTblDao.queryByResourceIds(resourceIds);
        List<ApplierTblV3> applierTblV3s = dbApplierTblDao.queryByResourceIds(resourceIds);

        Map<Long, Long> replicatorMap = replicatorTbls.stream().collect(Collectors.groupingBy(ReplicatorTbl::getResourceId, Collectors.counting()));
        Map<Long, Long> messengerMap = messengerTbls.stream().collect(Collectors.groupingBy(MessengerTbl::getResourceId, Collectors.counting()));
        Map<Long, Long> messengerV3Map = messengerTblsV3.stream().collect(Collectors.groupingBy(MessengerTblV3::getResourceId, Collectors.counting()));
        Map<Long, Long> dbApplierMap = applierTblV3s.stream().collect(Collectors.groupingBy(ApplierTblV3::getResourceId, Collectors.counting()));

        List<ResourceView> views = buildResourceViews(resourceTbls, replicatorMap, dbApplierMap, messengerMap, messengerV3Map);
        return views;
    }

    @Override
    public List<ResourceView> getResourceViewByIp(String ip) throws Exception {
        ResourceTbl resourceTbl = resourceTblDao.queryByIp(ip, BooleanEnum.FALSE.getCode());
        if (resourceTbl == null) {
            return new ArrayList<>();
        }

        List<ResourceTbl> resourceTbls = resourceTblDao.queryByDcAndTag(Lists.newArrayList(resourceTbl.getDcId()), resourceTbl.getTag(), resourceTbl.getType(), BooleanEnum.TRUE.getCode());
        resourceTbls = resourceTbls.stream().filter(e -> !e.getIp().equals(ip)).collect(Collectors.toList());
        if (CollectionUtils.isEmpty(resourceTbls)) {
            return new ArrayList<>();
        }

        List<Long> resourceIds = resourceTbls.stream().map(ResourceTbl::getId).collect(Collectors.toList());

        List<ReplicatorTbl> replicatorTbls = new ArrayList<>();
        List<MessengerTbl> messengerTbls = new ArrayList<>();
        List<MessengerTblV3> messengerTblsV3 = new ArrayList<>();
        List<ApplierTblV3> applierTblV3s = new ArrayList<>();
        if (resourceTbl.getType() == ModuleEnum.REPLICATOR.getCode()) {
            replicatorTbls = replicatorTblDao.queryByResourceIds(resourceIds);
        } else if (resourceTbl.getType() == ModuleEnum.APPLIER.getCode()) {
            messengerTbls = messengerTblDao.queryByResourceIds(resourceIds);
            messengerTblsV3 = dbMessengerTblDao.queryByResourceIds(resourceIds);
            applierTblV3s = dbApplierTblDao.queryByResourceIds(resourceIds);
        } else if (resourceTbl.getType() == ModuleEnum.MESSENGER.getCode()) {
            messengerTbls = messengerTblDao.queryByResourceIds(resourceIds);
            messengerTblsV3 = dbMessengerTblDao.queryByResourceIds(resourceIds);
        }


        Map<Long, Long> replicatorMap = replicatorTbls.stream().collect(Collectors.groupingBy(ReplicatorTbl::getResourceId, Collectors.counting()));
        Map<Long, Long> messengerMap = messengerTbls.stream().collect(Collectors.groupingBy(MessengerTbl::getResourceId, Collectors.counting()));
        Map<Long, Long> messengerV3Map = messengerTblsV3.stream().collect(Collectors.groupingBy(MessengerTblV3::getResourceId, Collectors.counting()));
        Map<Long, Long> dbApplierMap = applierTblV3s.stream().collect(Collectors.groupingBy(ApplierTblV3::getResourceId, Collectors.counting()));

        List<ResourceView> views = buildResourceViews(resourceTbls, replicatorMap, dbApplierMap, messengerMap, messengerV3Map);
        Collections.sort(views);
        return views;
    }

    @Override
    public List<ResourceView> getMhaAvailableResource(String mhaName, int type) throws SQLException {
        MhaTblV2 mhaTbl = mhaTblV2Dao.queryByMhaName(mhaName, BooleanEnum.FALSE.getCode());
        if (mhaTbl == null) {
            logger.info("mha: {} not exist", mhaName);
            return new ArrayList<>();
        }

        if (type != ModuleEnum.REPLICATOR.getCode() && type != ModuleEnum.APPLIER.getCode() && type != ModuleEnum.MESSENGER.getCode()) {
            logger.info("resource type: {} can only be replicator or applier or messenger", type);
            return new ArrayList<>();
        }
        DcTbl dcTbl = dcTblDao.queryById(mhaTbl.getDcId());
        List<Long> dcIds = dcTblDao.queryByRegionName(dcTbl.getRegionName()).stream().map(DcTbl::getId).collect(Collectors.toList());
        return getResourceViews(dcIds, dcTbl.getRegionName(), type, mhaTbl.getTag());
    }

    @Override
    public List<ResourceView> getMhaDbAvailableResource(String mhaName, int type) throws SQLException {
        if (type != ModuleEnum.APPLIER.getCode() && type != ModuleEnum.MESSENGER.getCode()) {
            logger.info("resource type: {} can only be applier or messenger", type);
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
    public List<ResourceView> getMhaDbAvailableResourceWithUse(String srcMhaName, String dstMhaName, int type, String subType) throws Exception {
        List<ResourceView> resourceViews = getMhaDbAvailableResource(dstMhaName, type);

        List<ResourceView> resourceViewsInUse = new ArrayList<>();
        if (type == ModuleEnum.APPLIER.getCode()) {
            resourceViewsInUse.addAll(getDbAppliersInUse(srcMhaName, dstMhaName));
        }
        if (type == ModuleEnum.MESSENGER.getCode()) {
            resourceViewsInUse.addAll(getDbMessengersInUse(srcMhaName, MqType.valueOf(subType)));
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
    public List<ResourceView> getMhaAvailableResourceWithUse(String mhaName, int type, String subType) throws Exception {
        List<ResourceView> resourceViews = getMhaAvailableResource(mhaName, type);

        MhaTblV2 mhaTbl = mhaTblV2Dao.queryByMhaName(mhaName, BooleanEnum.FALSE.getCode());
        List<ResourceView> resourceViewsInUse = new ArrayList<>();
        if (type == ModuleEnum.REPLICATOR.getCode()) {
            resourceViewsInUse.addAll(getReplicatorsInUse(mhaTbl.getId()));
        } else if (type == ModuleEnum.MESSENGER.getCode()) {
            resourceViewsInUse.addAll(getMessengersInUse(mhaTbl.getId(), MqType.parse(subType)));
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

    private List<ResourceView> getMessengersInUse(long mhaId, MqType mqType) throws Exception {
        MessengerGroupTbl messengerGroupTbl = messengerGroupTblDao.queryByMhaIdAndMqType(mhaId, mqType, BooleanEnum.FALSE.getCode());
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

    private List<ResourceView> getDbMessengersInUse(String mha, MqType mqType) throws Exception {
        List<DbApplierDto> mhaDbMessengers = dbDrcBuildService.getMhaDbMessengers(mha, mqType);
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

    /**
     * side effect: will modify instanceNum of availableResource after handoff. Could be used for multiple handoff
     */
    @Override
    public List<ResourceView> handOffResource(List<String> selectedIps, List<ResourceView> availableResource) {
        List<ResourceView> resultViews = new ArrayList<>();
        List<ResourceView> resourceViews = availableResource.stream()
                .filter(e -> {
                    if (selectedIps.contains(e.getIp())) {
                        e.declineInstanceNum();
                        return false;
                    } else {
                        return true;
                    }
                }).collect(Collectors.toList());
        if (CollectionUtils.isEmpty(resourceViews)) {
            return resultViews;
        }

        setResourceView(resultViews, resourceViews);
        Collections.sort(availableResource);
        return resultViews;
    }

    @Override
    public List<MhaView> queryMhaByReplicator(long resourceId) throws Exception {
        List<ReplicatorTbl> replicators = replicatorTblDao.queryByResourceIds(Lists.newArrayList(resourceId));
        if (CollectionUtils.isEmpty(replicators)) {
            return new ArrayList<>();
        }

        List<Long> replicatorGroupIds = replicators.stream().map(ReplicatorTbl::getRelicatorGroupId).distinct().collect(Collectors.toList());
        List<ReplicatorGroupTbl> replicatorGroupTbls = replicatorGroupTblDao.queryByIds(replicatorGroupIds);
        List<Long> mhaIds = replicatorGroupTbls.stream().map(ReplicatorGroupTbl::getMhaId).collect(Collectors.toList());
        List<MhaTblV2> mhaTblV2s = mhaTblV2Dao.queryByIds(mhaIds);
        List<MhaView> views = mhaTblV2s.stream().map(source -> {
            MhaView target = new MhaView(source.getMhaName());
            return target;
        }).collect(Collectors.toList());

        return views;
    }

    @Override
    public List<ApplierReplicationView> queryReplicationByApplier(long resourceId) throws Exception {
        List<ApplierReplicationView> views = new ArrayList<>();
        views.addAll(queryMhaDbReplicationByApplier(resourceId));
        views.addAll(queryMhaByMessenger(resourceId));

        return views;
    }

    public List<ApplierReplicationView> queryMhaDbReplicationByApplier(long resourceId) throws Exception {
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

        Map<Long, Long> applierMap = applierTblV3s.stream().collect(Collectors.toMap(ApplierTblV3::getApplierGroupId, ApplierTblV3::getId));
        Map<Long, Long> mhaReplicationIdToApplierGroupId = applierGroupTblV3s.stream().collect(Collectors.toMap(ApplierGroupTblV3::getMhaDbReplicationId, ApplierGroupTblV3::getId));

        List<ApplierReplicationView> views = mhaDbReplicationTbls.stream().map(source -> {
            MhaDbMappingTbl srcMhaDbMapping = mhaDbMappingMap.get(source.getSrcMhaDbMappingId());
            MhaDbMappingTbl dstMhaDbMapping = mhaDbMappingMap.get(source.getDstMhaDbMappingId());
            MhaTblV2 srcMha = mhaMap.get(srcMhaDbMapping.getMhaId());
            MhaTblV2 dstMha = mhaMap.get(dstMhaDbMapping.getMhaId());

            ApplierReplicationView target = new ApplierReplicationView();
            target.setDbName(dbNameMap.get(srcMhaDbMapping.getDbId()));
            target.setSrcMhaName(srcMha.getMhaName());
            target.setDstMhaName(dstMha.getMhaName());
            target.setSrcDcName(dcMap.get(srcMha.getDcId()));
            target.setDstDcName(dcMap.get(dstMha.getDcId()));

            target.setRelatedId(applierMap.get(mhaReplicationIdToApplierGroupId.get(source.getId())));
            target.setType(ApplierTypeEnum.DB_APPLIER.getCode());
            return target;
        }).collect(Collectors.toList());
        return views;
    }

    public List<ApplierReplicationView> queryMhaByMessenger(long resourceId) throws Exception {
        List<MessengerTbl> messengerTbls = messengerTblDao.queryByResourceIds(Lists.newArrayList(resourceId));
        if (CollectionUtils.isEmpty(messengerTbls)) {
            return new ArrayList<>();
        }

        List<Long> messengerGroupIds = messengerTbls.stream().map(MessengerTbl::getMessengerGroupId).collect(Collectors.toList());
        List<MessengerGroupTbl> messengerGroupTbls = messengerGroupTblDao.queryByIds(messengerGroupIds);
        List<Long> mhaIds = messengerGroupTbls.stream().map(MessengerGroupTbl::getMhaId).collect(Collectors.toList());
        List<MhaTblV2> mhaTblV2s = mhaTblV2Dao.queryByIds(mhaIds);
        List<DcTbl> dcTbls = dcTblDao.queryAllExist();
        Map<Long, String> dcMap = dcTbls.stream().collect(Collectors.toMap(DcTbl::getId, DcTbl::getDcName));

        Map<Long, Long> messengerMap = messengerTbls.stream().collect(Collectors.toMap(MessengerTbl::getMessengerGroupId, MessengerTbl::getId));
//        Map<Long, Long> mhaIdToMessengerGroupId = messengerGroupTbls.stream().collect(Collectors.toMap(MessengerGroupTbl::getMhaId, MessengerGroupTbl::getId));
        Map<Long, List<MessengerGroupTbl>> mhaIdToMessengerGroupIdList = messengerGroupTbls.stream().collect(Collectors.groupingBy(MessengerGroupTbl::getMhaId));

        return mhaTblV2s.stream()
                .flatMap(source -> {
                    List<MessengerGroupTbl> mGroups = mhaIdToMessengerGroupIdList.get(source.getId());
                    return mGroups.stream().map(m -> {
                        ApplierReplicationView target = new ApplierReplicationView();
                        target.setSrcMhaName(source.getMhaName());
                        target.setSrcDcName(dcMap.get(source.getDcId()));
                        target.setRelatedId(messengerMap.get(m.getId()));
                        target.setType(ApplierTypeEnum.MESSENGER.getCode());
                        target.setMqType(m.getMqType());
                        return target;
                    });
                }).collect(Collectors.toList());

//        return mhaTblV2s.stream().map(source -> {
//            ApplierReplicationView target = new ApplierReplicationView();
//            target.setSrcMhaName(source.getMhaName());
//            target.setSrcDcName(dcMap.get(source.getDcId()));
//            target.setRelatedId(messengerMap.get(mhaIdToMessengerGroupId.get(source.getId())));
//            target.setType(ApplierTypeEnum.MESSENGER.getCode());
//            return target;
//        }).collect(Collectors.toList());
    }

    @Override
    @DalTransactional(logicDbName = "fxdrcmetadb_w")
    public int migrateResource(String newIp, String oldIp, int type) throws Exception {
        if (newIp.equals(oldIp)) {
            throw ConsoleExceptionUtils.message("newIp and oldIp cannot be the same");
        }
        if (type == ModuleEnum.REPLICATOR.getCode()) {
            return migrateReplicator(newIp, oldIp, null);
        } else if (type == ModuleEnum.APPLIER.getCode()) {
            return migrateApplier(newIp, oldIp);
        } else if (type == ModuleEnum.MESSENGER.getCode()) {
            return migrateMessenger(newIp, oldIp);
        }
        throw ConsoleExceptionUtils.message("type not supported!");
    }


    @Override
    public int partialMigrateReplicator(ReplicatorMigrateParam param) throws Exception {
        return migrateReplicator(param.getNewIp(), param.getOldIp(), param.getMhaList());
    }

    @Override
    public int partialMigrateApplier(ApplierMigrateParam param) throws Exception {
        ResourceTbl newResource = resourceTblDao.queryByIp(param.getNewIp(), BooleanEnum.FALSE.getCode());
        ResourceTbl oldResource = resourceTblDao.queryByIp(param.getOldIp(), BooleanEnum.FALSE.getCode());
        checkMigrateResource(newResource, oldResource, ModuleEnum.APPLIER.getCode());

        List<Long> dbApplierIds = param.getApplierResourceDtos().stream().filter(e -> e.getType() == ApplierTypeEnum.DB_APPLIER.getCode()).map(ApplierResourceDto::getRelatedId).collect(Collectors.toList());
        List<Long> messengerIds = param.getApplierResourceDtos().stream().filter(e -> e.getType() == ApplierTypeEnum.MESSENGER.getCode()).map(ApplierResourceDto::getRelatedId).collect(Collectors.toList());
        List<Long> dbMessengerIds = param.getApplierResourceDtos().stream().filter(e -> e.getType() == ApplierTypeEnum.DB_MESSENGER.getCode()).map(ApplierResourceDto::getRelatedId).collect(Collectors.toList());
        List<ApplierTblV3> applierTblV3s = dbApplierTblDao.queryByIds(dbApplierIds);
        List<MessengerTbl> messengerTbls = messengerTblDao.queryByIds(messengerIds);
        List<MessengerTblV3> messengerTblsV3 = dbMessengerTblDao.queryByIds(dbMessengerIds);

        int result =  0;
        result += migrateDbApplier(newResource, applierTblV3s);
        result += migrateMessenger(newResource, messengerTbls);
        result += migrateDbMessenger(newResource, messengerTblsV3);

        return result;
    }

    @Override
    public int partialMigrateMessenger(ApplierMigrateParam param) throws Exception {
        ResourceTbl newResource = resourceTblDao.queryByIp(param.getNewIp(), BooleanEnum.FALSE.getCode());
        ResourceTbl oldResource = resourceTblDao.queryByIp(param.getOldIp(), BooleanEnum.FALSE.getCode());
        checkMigrateResource(newResource, oldResource, ModuleEnum.MESSENGER.getCode());

        List<Long> messengerIds = param.getApplierResourceDtos().stream().filter(e -> e.getType() == ApplierTypeEnum.MESSENGER.getCode()).map(ApplierResourceDto::getRelatedId).collect(Collectors.toList());
        List<Long> dbMessengerIds = param.getApplierResourceDtos().stream().filter(e -> e.getType() == ApplierTypeEnum.DB_MESSENGER.getCode()).map(ApplierResourceDto::getRelatedId).collect(Collectors.toList());
        List<MessengerTbl> messengerTbls = messengerTblDao.queryByIds(messengerIds);
        List<MessengerTblV3> messengerTblsV3 = dbMessengerTblDao.queryByIds(dbMessengerIds);

        int result =  0;
        result += migrateMessenger(newResource, messengerTbls);
        result += migrateDbMessenger(newResource, messengerTblsV3);

        return result;
    }

    @Override
    @DalTransactional(logicDbName = "fxdrcmetadb_w")
    public int migrateSlaveReplicator(String newIp, String oldIp) throws Exception {
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
        List<ReplicatorTbl> slaveReplicatorTbls = replicatorTbls.stream().filter(e -> e.getMaster().equals(BooleanEnum.FALSE.getCode())).collect(Collectors.toList());
        return migrateReplicator(newResource, slaveReplicatorTbls);
    }

    @Override
    @DalTransactional(logicDbName = "fxdrcmetadb_w")
    public void migrateResource(ResourceMigrateParam param) throws Exception {
        if (CollectionUtils.isEmpty(param.getResourceMigrateDtoList())) {
            return;
        }
        for (ResourceMigrateDto dto : param.getResourceMigrateDtoList()) {
            migrateResource(dto.getNewIp(), dto.getOldIp(), param.getType());
        }
    }

    @Override
    public ResourceSameAzView checkResourceAz() throws Exception {
        List<ResourceTbl> resourceTbls = resourceTblDao.queryAllExist();
        Map<Long, String> resourceIdToAzMap = resourceTbls.stream().collect(Collectors.toMap(ResourceTbl::getId, ResourceTbl::getAz));

        List<MhaTblV2> mhaTblV2s = mhaTblV2Dao.queryAll();
        Map<Long, MhaTblV2> mhaTblV2Map = mhaTblV2s.stream().collect(Collectors.toMap(MhaTblV2::getId, e -> e));

        ResourceSameAzView view = new ResourceSameAzView();
        view.setReplicatorMhaList(checkReplicators(resourceIdToAzMap, mhaTblV2Map));
        view.setApplierDbList(checkDbAppliers(resourceIdToAzMap));
        view.setMessengerMhaList(checkMessengers(resourceIdToAzMap, mhaTblV2Map));
        return view;
    }

    private List<String> checkDbAppliers(Map<Long, String> resourceIdToAzMap) throws Exception {
        List<String> dbNames = new ArrayList<>();
        List<ApplierTblV3> dbApplierTbls = dbApplierTblDao.queryAllExist();
        Map<Long, List<ApplierTblV3>> dbApplierMap = dbApplierTbls.stream().collect(Collectors.groupingBy(ApplierTblV3::getApplierGroupId));

        List<ApplierGroupTblV3> applierGroupTblV3s = dbApplierGroupTblDao.queryAll();
        Map<Long, ApplierGroupTblV3> applierGroupTblV3Map = applierGroupTblV3s.stream().collect(Collectors.toMap(ApplierGroupTblV3::getId, e-> e));

        List<MhaDbReplicationTbl> mhaDbReplicationTbls = mhaDbReplicationTblDao.queryAll();
        Map<Long, MhaDbReplicationTbl> mhaDbReplicationTblMap = mhaDbReplicationTbls.stream().collect(Collectors.toMap(MhaDbReplicationTbl::getId, e-> e));

        List<MhaDbMappingTbl> mhaDbMappingTbls = mhaDbMappingTblDao.queryAll();
        Map<Long, MhaDbMappingTbl> mhaDbMappingTblMap = mhaDbMappingTbls.stream().collect(Collectors.toMap(MhaDbMappingTbl::getId, e -> e));

        List<DbTbl> dbTbls = dbTblDao.queryAll();
        Map<Long, DbTbl> dbTblMap = dbTbls.stream().collect(Collectors.toMap(DbTbl::getId, e -> e));

         for (Map.Entry<Long, List<ApplierTblV3>> entry : dbApplierMap.entrySet()) {
            long applierGroupId = entry.getKey();
            List<ApplierTblV3> dbAppliers = entry.getValue();
            if (checkDbAppliers(dbAppliers, resourceIdToAzMap)) {
                continue;
            }
            ApplierGroupTblV3 applierGroupTblV3 = applierGroupTblV3Map.get(applierGroupId);
            MhaDbReplicationTbl mhaDbReplicationTbl = mhaDbReplicationTblMap.get(applierGroupTblV3.getMhaDbReplicationId());
            MhaDbMappingTbl mhaDbMappingTbl = mhaDbMappingTblMap.get(mhaDbReplicationTbl.getSrcMhaDbMappingId());
            DbTbl dbTbl = dbTblMap.get(mhaDbMappingTbl.getDbId());
            dbNames.add(dbTbl.getDbName());
        }
        return dbNames;
    }

    private List<String> checkMessengers(Map<Long, String> resourceIdToAzMap, Map<Long, MhaTblV2> mhaTblV2Map) throws Exception {
        List<String> mhaNames = new ArrayList<>();
        List<MessengerTbl> messengerTbls = messengerTblDao.queryAllExist();
        Map<Long, List<MessengerTbl>> messengerMap = messengerTbls.stream().collect(Collectors.groupingBy(MessengerTbl::getMessengerGroupId));

        List<MessengerGroupTbl> messengerGroupTbls = messengerGroupTblDao.queryAll();
        Map<Long, MessengerGroupTbl> messengerGroupTblMap = messengerGroupTbls.stream().collect(Collectors.toMap(MessengerGroupTbl::getId, e -> e));

        for (Map.Entry<Long, List<MessengerTbl>> entry : messengerMap.entrySet()) {
            long messengerGroupId = entry.getKey();
            List<MessengerTbl> messengers = entry.getValue();
            if (checkMessengers(messengers, resourceIdToAzMap)) {
                continue;
            }
            MessengerGroupTbl messengerGroupTbl = messengerGroupTblMap.get(messengerGroupId);
            MhaTblV2 mhaTblV2 = mhaTblV2Map.get(messengerGroupTbl.getMhaId());
            mhaNames.add(mhaTblV2.getMhaName());
        }
        return mhaNames;
    }

    private List<String> checkReplicators(Map<Long, String> resourceIdToAzMap, Map<Long, MhaTblV2> mhaTblV2Map) throws Exception {
        List<String> mhaNames = new ArrayList<>();
        List<ReplicatorTbl> replicatorTbls = replicatorTblDao.queryAllExist();
        Map<Long, List<ReplicatorTbl>> replicatorMap = replicatorTbls.stream().collect(Collectors.groupingBy(ReplicatorTbl::getRelicatorGroupId));

        List<ReplicatorGroupTbl> replicatorGroupTbls = replicatorGroupTblDao.queryAll();
        Map<Long, ReplicatorGroupTbl> replicatorGroupTblMap = replicatorGroupTbls.stream().collect(Collectors.toMap(ReplicatorGroupTbl::getId, e -> e));

        for (Map.Entry<Long, List<ReplicatorTbl>> entry : replicatorMap.entrySet()) {
            long replicatorGroupId = entry.getKey();
            List<ReplicatorTbl> replicators = entry.getValue();
            if (checkReplicators(replicators, resourceIdToAzMap)) {
                continue;
            }
            ReplicatorGroupTbl replicatorGroupTbl = replicatorGroupTblMap.get(replicatorGroupId);
            MhaTblV2 mhaTblV2 = mhaTblV2Map.get(replicatorGroupTbl.getMhaId());
            mhaNames.add(mhaTblV2.getMhaName());
        }
        return mhaNames;
    }

    private boolean checkReplicators(List<ReplicatorTbl> replicatorTbls, Map<Long, String> resourceIdToAzMap) {
        if (replicatorTbls.size() != 2) {
            return false;
        }
        String firstAz = resourceIdToAzMap.get(replicatorTbls.get(0).getResourceId());
        String secondAz = resourceIdToAzMap.get(replicatorTbls.get(1).getResourceId());
        return !firstAz.equals(secondAz);
    }

    private boolean checkMessengers(List<MessengerTbl> messengerTbls, Map<Long, String> resourceIdToAzMap) {
        if (messengerTbls.size() != 2) {
            return false;
        }
        String firstAz = resourceIdToAzMap.get(messengerTbls.get(0).getResourceId());
        String secondAz = resourceIdToAzMap.get(messengerTbls.get(1).getResourceId());
        return !firstAz.equals(secondAz);
    }

    private boolean checkDbAppliers(List<ApplierTblV3> dbAppliers, Map<Long, String> resourceIdToAzMap) {
        if (dbAppliers.size() != 2) {
            return false;
        }
        String firstAz = resourceIdToAzMap.get(dbAppliers.get(0).getResourceId());
        String secondAz = resourceIdToAzMap.get(dbAppliers.get(1).getResourceId());
        return !firstAz.equals(secondAz);
    }


    private int migrateReplicator(String newIp, String oldIp, List<String> mhaList) throws Exception {
        ResourceTbl newResource = resourceTblDao.queryByIp(newIp, BooleanEnum.FALSE.getCode());
        ResourceTbl oldResource = resourceTblDao.queryByIp(oldIp, BooleanEnum.FALSE.getCode());
        if (newResource == null || oldResource == null) {
            throw ConsoleExceptionUtils.message("newIp or oldIp not exist");
        }
        if (newResource.getActive().equals(BooleanEnum.FALSE.getCode())) {
            throw ConsoleExceptionUtils.message("newIp is not active");
        }
        if (!newResource.getType().equals(oldResource.getType()) || !newResource.getType().equals(ModuleEnum.REPLICATOR.getCode())) {
            throw ConsoleExceptionUtils.message("newIp is not replicator");
        }
        List<ReplicatorTbl> replicatorTbls = replicatorTblDao.queryByResourceIds(Lists.newArrayList(oldResource.getId()));
        if (!CollectionUtils.isEmpty(mhaList)) {
            List<MhaTblV2> mhaTblV2s = mhaTblV2Dao.queryByMhaNames(mhaList, BooleanEnum.FALSE.getCode());
            List<ReplicatorGroupTbl> replicatorGroupTbls = replicatorGroupTblDao.queryByMhaIds(mhaTblV2s.stream().map(MhaTblV2::getId).collect(Collectors.toList()), BooleanEnum.FALSE.getCode());
            List<Long> replicatorGroupIds = replicatorGroupTbls.stream().map(ReplicatorGroupTbl::getId).collect(Collectors.toList());
            replicatorTbls = replicatorTbls.stream().filter(e -> replicatorGroupIds.contains(e.getRelicatorGroupId())).collect(Collectors.toList());
        }
        return migrateReplicator(newResource, replicatorTbls);
    }

    private int migrateReplicator(ResourceTbl newResource, List<ReplicatorTbl> replicatorTbls) throws SQLException {
        if (CollectionUtils.isEmpty(replicatorTbls)) {
            return 0;
        }
        List<Long> replicatorGroupIds = replicatorTbls.stream().map(ReplicatorTbl::getRelicatorGroupId).distinct().collect(Collectors.toList());
        List<ReplicatorTbl> allReplicatorTbls = replicatorTblDao.queryByRGroupIds(replicatorGroupIds, BooleanEnum.FALSE.getCode());
        List<Long> allResourceIds = allReplicatorTbls.stream().map(ReplicatorTbl::getResourceId).distinct().collect(Collectors.toList());
        if (allResourceIds.contains(newResource.getId())) {
            throw ConsoleExceptionUtils.message("replicator requires different master and slave");
        }

        List<ListenableFuture<Pair<Long, String>>> futures = new ArrayList<>();
        for (ReplicatorTbl replicatorTbl : replicatorTbls) {
            ListenableFuture<Pair<Long, String>> future = executorService.submit(() -> getInitGtid(replicatorTbl));
            futures.add(future);
        }

        Map<Long, String> gtidInitMap = new HashMap<>();
        for (ListenableFuture<Pair<Long, String>> future : futures) {
            try {
                Pair<Long, String> result = future.get(5, TimeUnit.SECONDS);
                gtidInitMap.put(result.getLeft(), result.getRight());
            } catch (Exception e) {
                throw ConsoleExceptionUtils.message("migrateReplicator fail");
            }

        }

        for (ReplicatorTbl replicatorTbl : replicatorTbls) {
            replicatorTbl.setResourceId(newResource.getId());
            replicatorTbl.setApplierPort(metaInfoService.findAvailableApplierPort(newResource.getIp()));
            String gtidInit = gtidInitMap.get(replicatorTbl.getId());
            if (StringUtils.isBlank(gtidInit)) {
                throw ConsoleExceptionUtils.message("query gtidInit fail, replicatorId: " + replicatorTbl.getId());
            }
            replicatorTbl.setGtidInit(gtidInit);
            replicatorTblDao.update(replicatorTbl);
        }

        return replicatorTbls.size();
    }


    private Pair<Long, String> getInitGtid(ReplicatorTbl replicatorTbl) throws SQLException {
        ReplicatorGroupTbl replicatorGroupTbl = replicatorGroupTblDao.queryById(replicatorTbl.getRelicatorGroupId());
        MhaTblV2 mhaTblV2 = mhaTblV2Dao.queryById(replicatorGroupTbl.getMhaId());
        String gtidInit = mysqlServiceV2.getMhaExecutedGtid(mhaTblV2.getMhaName());
        return Pair.of(replicatorTbl.getId(), gtidInit);
    }

    private int migrateDbApplier(ResourceTbl newResource, List<ApplierTblV3> applierTblV3s) throws SQLException {
        if (CollectionUtils.isEmpty(applierTblV3s)) {
            return 0;
        }
        List<ApplierTblV3> allApplierTblV3s = dbApplierTblDao.queryByApplierGroupIds(
                applierTblV3s.stream().map(ApplierTblV3::getApplierGroupId).distinct().collect(Collectors.toList()), BooleanEnum.FALSE.getCode());
        List<Long> allResourceIds = allApplierTblV3s.stream().map(ApplierTblV3::getResourceId).distinct().collect(Collectors.toList());
        if (allResourceIds.contains(newResource.getId())) {
            throw ConsoleExceptionUtils.message("dbApplier requires different master and slave");
        }
        applierTblV3s.forEach(e -> e.setResourceId(newResource.getId()));
        dbApplierTblDao.update(applierTblV3s);
        return applierTblV3s.size();
    }

    private int migrateMessenger(ResourceTbl newResource, List<MessengerTbl> messengerTbls) throws SQLException {
        if (CollectionUtils.isEmpty(messengerTbls)) {
            return 0;
        }
        List<MessengerTbl> allMessengerTbls = messengerTblDao.queryByGroupIds(messengerTbls.stream().map(MessengerTbl::getMessengerGroupId).distinct().collect(Collectors.toList()));
        List<Long> allResourceIds = allMessengerTbls.stream().map(MessengerTbl::getResourceId).distinct().collect(Collectors.toList());
        if (allResourceIds.contains(newResource.getId())) {
            throw ConsoleExceptionUtils.message("messenger requires different master and slave");
        }
        messengerTbls.forEach(e -> e.setResourceId(newResource.getId()));
        messengerTblDao.update(messengerTbls);
        return messengerTbls.size();
    }

    private int migrateDbMessenger(ResourceTbl newResource, List<MessengerTblV3> messengerTblsV3) throws SQLException {
        if (CollectionUtils.isEmpty(messengerTblsV3)) {
            return 0;
        }
        List<MessengerTblV3> allMessengerTblV3s = dbMessengerTblDao.queryByGroupIds(messengerTblsV3.stream().map(MessengerTblV3::getMessengerGroupId).distinct().collect(Collectors.toList()));
        List<Long> allResourceIds = allMessengerTblV3s.stream().map(MessengerTblV3::getResourceId).distinct().collect(Collectors.toList());
        if (allResourceIds.contains(newResource.getId())) {
            throw ConsoleExceptionUtils.message("dbMessenger requires different master and slave");
        }
        messengerTblsV3.forEach(e -> e.setResourceId(newResource.getId()));
        dbMessengerTblDao.update(messengerTblsV3);
        return messengerTblsV3.size();
    }

    private int migrateApplier(String newIp, String oldIp) throws Exception {
        ResourceTbl newResource = resourceTblDao.queryByIp(newIp, BooleanEnum.FALSE.getCode());
        ResourceTbl oldResource = resourceTblDao.queryByIp(oldIp, BooleanEnum.FALSE.getCode());
        checkMigrateResource(newResource, oldResource, ModuleEnum.APPLIER.getCode());

        List<ApplierTblV3> applierTblV3s = dbApplierTblDao.queryByResourceIds(Lists.newArrayList(oldResource.getId()));
        List<MessengerTbl> messengerTbls = messengerTblDao.queryByResourceIds(Lists.newArrayList(oldResource.getId()));
        List<MessengerTblV3> messengerTblsV3 = dbMessengerTblDao.queryByResourceIds(Lists.newArrayList(oldResource.getId()));

        int result =  0;
        result += migrateDbApplier(newResource, applierTblV3s);
        result += migrateMessenger(newResource, messengerTbls);
        result += migrateDbMessenger(newResource, messengerTblsV3);

        return result;
    }

    private int migrateMessenger(String newIp, String oldIp) throws Exception {
        ResourceTbl newResource = resourceTblDao.queryByIp(newIp, BooleanEnum.FALSE.getCode());
        ResourceTbl oldResource = resourceTblDao.queryByIp(oldIp, BooleanEnum.FALSE.getCode());
        checkMigrateResource(newResource, oldResource, ModuleEnum.MESSENGER.getCode());

        List<MessengerTbl> messengerTbls = messengerTblDao.queryByResourceIds(Lists.newArrayList(oldResource.getId()));
        List<MessengerTblV3> messengerTblsV3 = dbMessengerTblDao.queryByResourceIds(Lists.newArrayList(oldResource.getId()));

        int result =  0;
        result += migrateMessenger(newResource, messengerTbls);
        result += migrateDbMessenger(newResource, messengerTblsV3);

        return result;
    }

    private void checkMigrateResource(ResourceTbl newResource, ResourceTbl oldResource, int type) {
        if (newResource == null || oldResource == null) {
            throw ConsoleExceptionUtils.message("newIp or oldIp not exist");
        }
        if (newResource.getActive().equals(BooleanEnum.FALSE.getCode())) {
            throw ConsoleExceptionUtils.message("newIp is active");
        }
        if (!newResource.getType().equals(oldResource.getType()) || newResource.getType() != type) {
            throw ConsoleExceptionUtils.message("newIp and oldIp are not same type");
        }
    }

    /**
     * side effect: will modify instanceNum of availableResource after handoff. Could be used for multiple handoff
     */
    private void setResourceView(List<ResourceView> resultViews, List<ResourceView> resourceViews) {
        Collections.sort(resourceViews);
        if (CollectionUtils.isEmpty(resultViews)) {
            ResourceView firstResource = resourceViews.get(0);
            resultViews.add(firstResource);
            firstResource.addInstanceNum();
        }
        ResourceView firstResource = resultViews.get(0);
        ResourceView secondResource = resourceViews.stream().filter(e -> !e.getAz().equals(firstResource.getAz())).findFirst().orElse(null);
        if (secondResource != null) {
            resultViews.add(secondResource);
            secondResource.addInstanceNum();
        }
        Collections.sort(resourceViews);
    }

    private List<ResourceView> getResourceViews(List<Long> dcIds, String region, int type, String tag) throws SQLException {
        List<ResourceTbl> resourceTbls = resourceTblDao.queryByDcAndTag(dcIds, tag, type, BooleanEnum.TRUE.getCode());
        List<Long> resourceIds = resourceTbls.stream().map(ResourceTbl::getId).collect(Collectors.toList());

        Map<Long, Long> replicatorMap = new HashMap<>();
        Map<Long, Long> messengerMap = new HashMap<>();
        Map<Long, Long> dbMessengerMap = new HashMap<>();
        Map<Long, Long> dbApplierMap = new HashMap<>();
        if (type == ModuleEnum.REPLICATOR.getCode()) {
            List<ReplicatorTbl> replicatorTbls = replicatorTblDao.queryByResourceIds(resourceIds);
            replicatorMap = replicatorTbls.stream().collect(Collectors.groupingBy(ReplicatorTbl::getResourceId, Collectors.counting()));
        } else if (type == ModuleEnum.APPLIER.getCode()) {
            List<MessengerTbl> messengerTbls = messengerTblDao.queryByResourceIds(resourceIds);
            List<MessengerTblV3> messengerTblsV3 = dbMessengerTblDao.queryByResourceIds(resourceIds);
            List<ApplierTblV3> dbAplierTbls = dbApplierTblDao.queryByResourceIds(resourceIds);
            messengerMap = messengerTbls.stream().collect(Collectors.groupingBy(MessengerTbl::getResourceId, Collectors.counting()));
            dbMessengerMap = messengerTblsV3.stream().collect(Collectors.groupingBy(MessengerTblV3::getResourceId, Collectors.counting()));
            dbApplierMap = dbAplierTbls.stream().collect(Collectors.groupingBy(ApplierTblV3::getResourceId, Collectors.counting()));
        } else if (type == ModuleEnum.MESSENGER.getCode()) {
            List<MessengerTbl> messengerTbls = messengerTblDao.queryByResourceIds(resourceIds);
            List<MessengerTblV3> messengerTblsV3 = dbMessengerTblDao.queryByResourceIds(resourceIds);
            messengerMap = messengerTbls.stream().collect(Collectors.groupingBy(MessengerTbl::getResourceId, Collectors.counting()));
            dbMessengerMap = messengerTblsV3.stream().collect(Collectors.groupingBy(MessengerTblV3::getResourceId, Collectors.counting()));
        }

        List<ResourceView> resourceViews = buildResourceViews(resourceTbls, replicatorMap, dbApplierMap, messengerMap, dbMessengerMap);
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
                long messengerNum = messengerMap.getOrDefault(source.getId(), 0L);
                long dbApplierNum = dbApplierMap.getOrDefault(source.getId(), 0L);
                long dbMessengerNum = dbMessengerMap.getOrDefault(source.getId(), 0L);
                target.setInstanceNum(messengerNum + dbApplierNum + dbMessengerNum);
            } else if (source.getType().equals(ModuleEnum.MESSENGER.getCode())) {
                long messengerNum = messengerMap.getOrDefault(source.getId(), 0L);
                long dbMessengerNum = dbMessengerMap.getOrDefault(source.getId(), 0L);
                target.setInstanceNum(messengerNum + dbMessengerNum);
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

    private void checkBatchResourceBuildParam(ResourceBuildParam param) {
        PreconditionUtils.checkArgument(!CollectionUtils.isEmpty(param.getIps()), "ips requires not empty!");
        PreconditionUtils.checkString(param.getType(), "type requires not empty!");
        PreconditionUtils.checkString(param.getDcName(), "dc requires not empty!");
        PreconditionUtils.checkString(param.getAz(), "AZ requires not empty!");
    }

    @Override
    public MhaAzView getAllInstanceAzInfo() throws Exception {
        Set<String> dcNameDba = consoleConfig.getDbaDc2DrcDcMap().keySet();
        Drc drc = metaProviderV2.getDrc();

        MhaAzView mhaAzView = new MhaAzView();
        Map<String, Set<String>> az2MhaName = dcNameDba.stream()
                .collect(Collectors.toMap(
                        k -> k,
                        v -> Sets.newHashSet()
                ));
        Map<String, List<String>> az2DbInstance = dcNameDba.stream()
                .collect(Collectors.toMap(
                        k -> k,
                        v -> Lists.newArrayList()
                ));

        Map<String, MhaInstanceGroupDto> mhaInstanceGroupMap = getMhaInstanceGroupsInAllRegions();
        List<MhaTblV2> drcRelatedMha = mhaTblV2Dao.queryAllExist();
        List<String> drcRelatedMhaName = drcRelatedMha.stream().map(MhaTblV2::getMhaName).distinct().collect(Collectors.toList());

        for (Map.Entry<String, MhaInstanceGroupDto> instanceEntry : mhaInstanceGroupMap.entrySet()) {
            MhaInstanceGroupDto mha = instanceEntry.getValue();
            if (!drcRelatedMhaName.contains(mha.getMhaName())) {
                continue;
            }
            String mhaMasterIdc = mha.getMaster().getIdc();
            String mhaMasterIpPort = mha.getMaster().getIp() + ":" + mha.getMaster().getPort();
            if (az2MhaName.containsKey(mhaMasterIdc)) {
                az2MhaName.get(mhaMasterIdc).add(mha.getMhaName());
            } else {
                az2MhaName.put(mhaMasterIdc, Sets.newHashSet(mha.getMhaName()));
            }
            if (az2DbInstance.containsKey(mhaMasterIdc)) {
                az2DbInstance.get(mhaMasterIdc).add(mhaMasterIpPort);
            } else {
                az2DbInstance.put(mhaMasterIdc, Lists.newArrayList(mhaMasterIpPort));
            }

            for (MhaInstanceGroupDto.MySQLInstance slave : mha.getSlaves()) {
                String mhaSlaveIdc = slave.getIdc();
                String mhaSlaveIpPort = slave.getIp() + ":" + slave.getPort();
                if (az2DbInstance.containsKey(mhaSlaveIdc)) {
                    az2DbInstance.get(mhaSlaveIdc).add(mhaSlaveIpPort);
                } else {
                    az2DbInstance.put(mhaSlaveIdc, Lists.newArrayList(mhaSlaveIpPort));
                }
            }
        }

        mhaAzView.setAz2mhaName(az2MhaName);
        mhaAzView.setAz2DbInstance(az2DbInstance);

        Map<String, Set<String>> az2DrcDb = mhaRelatedDrcDb(az2MhaName, drc);
        mhaAzView.setAz2DrcDb(az2DrcDb);

        if (drc == null) {
            return mhaAzView;
        }

        mhaAzView.setAz2ApplierInstance(getAppliersInAllDcs(drc, ModuleEnum.APPLIER.getCode()));
        mhaAzView.setAz2MessengerInstance(getAppliersInAllDcs(drc, ModuleEnum.MESSENGER.getCode()));
        mhaAzView.setAz2ReplicatorInstance(getReplicatorAz(drc));
        return mhaAzView;
    }

    public Map<String, Set<String>> mhaRelatedDrcDb(Map<String, Set<String>> az2MhaName, Drc drc) {
        Map<String, Set<String>> az2DrcDb = Maps.newHashMap();
        if (drc == null) {
            return az2DrcDb;
        }
        List<DbCluster> allDbClusters = drc.getDcs().values().stream()
                .flatMap(dc -> dc.getDbClusters().values().stream())
                .collect(Collectors.toList());
        for (Map.Entry<String, Set<String>> entry : az2MhaName.entrySet()) {
            String dc = entry.getKey();
            Set<String> mhaNames = entry.getValue();
            Set<String> relateDbs = allDbClusters.stream()
                    .filter(dbCluster -> !CollectionUtils.isEmpty(dbCluster.getAppliers()))
                    .flatMap(dbCluster -> dbCluster.getAppliers().stream())
                    .filter(applier -> mhaNames.contains(applier.getTargetMhaName()) || mhaNames.contains(applier.parent().getMhaName()))
                    .filter(applier -> applier.getIncludedDbs() != null)
                    .map(applier -> applier.getIncludedDbs().split(","))
                    .flatMap(Arrays::stream)
                    .filter(Objects::nonNull)
                    .collect(Collectors.toSet());


            Set<String> relateDbsInMhaMode = allDbClusters.stream()
                    .filter(dbCluster -> !CollectionUtils.isEmpty(dbCluster.getAppliers()))
                    .flatMap(dbCluster -> dbCluster.getAppliers().stream())
                    .filter(applier -> mhaNames.contains(applier.getTargetMhaName()) || mhaNames.contains(applier.parent().getMhaName()))
                    .filter(applier -> applier.getIncludedDbs() == null && applier.getNameFilter() != null)
                    .map(applier -> applier.getNameFilter().split(","))
                    .flatMap(Arrays::stream)
                    .map(s -> s.split("\\\\.")[0])
                    .filter(s -> !s.isEmpty())
                    .collect(Collectors.toSet());

            relateDbs.addAll(relateDbsInMhaMode);
            az2DrcDb.put(dc, relateDbs);
        }
        return az2DrcDb;
    }

    /**
     * available for both applier and messenger
     */
    private Map<String, List<? extends FetcherInfoDto>> getAppliersInAllDcs(Drc drc, int type) throws SQLException {
        Map<String, Set<String>> regions2DcMap = consoleConfig.getRegion2dcsMapping();
        List<String> regions = regions2DcMap.keySet().stream().collect(Collectors.toList());
        List<ResourceTbl> resourceTbls = resourceTblDao.queryAllExist();
        List<DcTbl> dcTbls = dcTblDao.queryAllExist();
        Map<Long, String> dcId2dcName = dcTbls.stream().collect(Collectors.toMap(DcTbl::getId, DcTbl::getDcName));

        Map<String, Dc> dcMap = drc.getDcs();
        Set<String> activeApplierIps = Sets.newHashSet();
        for (Map.Entry<String, Dc> dcEntry : dcMap.entrySet()) {
            Dc dc = dcEntry.getValue();
            Set<String> activeApplierIpsInDc = Sets.newHashSet();
            if (ModuleEnum.APPLIER.getCode() == type) {
                activeApplierIpsInDc = dc.getDbClusters().values().stream()
                        .flatMap(dbCluster -> dbCluster.getAppliers().stream())
                        .map(Applier::getIp)
                        .collect(Collectors.toSet());
            } else if (ModuleEnum.MESSENGER.getCode() == type) {
                activeApplierIpsInDc = dc.getDbClusters().values().stream()
                        .flatMap(dbCluster -> dbCluster.getMessengers().stream())
                        .map(Messenger::getIp)
                        .collect(Collectors.toSet());
            }
            activeApplierIps.addAll(activeApplierIpsInDc);
        }

        List<ResourceTbl> activeResourceTbls = resourceTbls.stream()
                .filter(e -> activeApplierIps.contains(e.getIp())).collect(Collectors.toList());
        Map<String, List<String>> dcName2Ips = activeResourceTbls.stream()
                .collect(Collectors.groupingBy(
                        e -> dcId2dcName.get(e.getDcId()),
                        Collectors.mapping(ResourceTbl::getIp, Collectors.toList())
                ));

        Map<String, List<? extends FetcherInfoDto>> infoDtosInAllDc = Maps.newHashMap();
        for (String region : regions) {
            List<String> dcInThisRegion = dcTbls.stream().filter(e -> region.equals(e.getRegionName())).map(DcTbl::getDcName).collect(Collectors.toList());
            Map<String, List<String>> dcName2IpsInThisRegion = dcName2Ips.entrySet().stream()
                    .filter(entry -> dcInThisRegion.contains(entry.getKey()))
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

            for (Map.Entry<String, List<String>> entry : dcName2IpsInThisRegion.entrySet()) {
                if (ModuleEnum.APPLIER.getCode() == type) {
                    List<ApplierInfoDto> infoDtos = resourceService.getMasterAppliersInRegion(region, entry.getValue());
                    if (infoDtos == null) {
                        EventMonitor.DEFAULT.logEvent("drc.console.instanceAzCheck.applier.fail", region + ":" + entry.getValue());
                        infoDtosInAllDc.put(entry.getKey(), Lists.newArrayList());
                    } else {
                        infoDtosInAllDc.put(entry.getKey(), infoDtos);
                    }
                }
                if (ModuleEnum.MESSENGER.getCode() == type) {
                    List<MessengerInfoDto> infoDtos = resourceService.getMasterMessengersInRegion(region, entry.getValue());
                    if (infoDtos == null) {
                        EventMonitor.DEFAULT.logEvent("drc.console.instanceAzCheck.messenger.fail", region + ":" + entry.getValue());
                        infoDtosInAllDc.put(entry.getKey(), Lists.newArrayList());
                    } else {
                        infoDtosInAllDc.put(entry.getKey(), infoDtos);
                    }
                }
            }
        }
        return infoDtosInAllDc;
    }

    @Override
    @PossibleRemote(path = "/api/drc/v2/resource/getAppliersInRegion", responseType = ApplierInfoApiRes.class)
    public List<ApplierInfoDto> getMasterAppliersInRegion(String region, List<String> ips) {
        List<Applier> appliers = ips.stream().map(ip -> {
            Applier applier = new Applier();
            applier.setIp(ip);
            applier.setPort(ConsoleConfig.DEFAULT_APPLIER_PORT);
            return applier;
        }).toList();

        com.ctrip.xpipe.tuple.Pair<List<String>, List<ApplierInfoDto>> applierInfo = batchInfoInquirer.getApplierInfo(appliers);
        return applierInfo.getValue().stream().filter(applierInfoDto -> Boolean.TRUE.equals(applierInfoDto.getMaster())).collect(Collectors.toList());
    }

    public Map<String, MhaInstanceGroupDto> getMhaInstanceGroupsInAllRegions() throws Exception {
        Map<String, MhaInstanceGroupDto> allMhaInstanceGroups = Maps.newHashMap();
        allMhaInstanceGroups.putAll(dalService.getMhaList(Foundation.server().getEnv()));
        allMhaInstanceGroups.putAll(dalService.getMhaListAli(Foundation.server().getEnv()));
        allMhaInstanceGroups.putAll(dalService.getMhaListAws(Foundation.server().getEnv()));
        return allMhaInstanceGroups;
    }

    @Override
    @PossibleRemote(path = "/api/drc/v2/resource/getMessengersInRegion", responseType = MessengerInfoApiRes.class)
    public List<MessengerInfoDto> getMasterMessengersInRegion(String region, List<String> ips) {
        List<Messenger> messengers = ips.stream().map(ip -> {
            Messenger messenger = new Messenger();
            messenger.setIp(ip);
            messenger.setPort(ConsoleConfig.DEFAULT_MESSENGER_PORT);
            return messenger;
        }).toList();

        com.ctrip.xpipe.tuple.Pair<List<String>, List<MessengerInfoDto>> messengerInfo = batchInfoInquirer.getMessengerInfo(messengers);
        return messengerInfo.getValue().stream().filter(messengerInfoDto -> Boolean.TRUE.equals(messengerInfoDto.getMaster())).collect(Collectors.toList());
    }

    @Override
    @PossibleRemote(path = "/api/drc/v2/resource/getReplicatorsInRegion", responseType = ReplicatorInfoApiRes.class)
    public List<ReplicatorInfoDto> getMasterReplicatorsInRegion(String region, List<String> ips) {
        List<Replicator> replicators = ips.stream().map(ip -> {
            Replicator replicator = new Replicator();
            replicator.setIp(ip);
            replicator.setPort(ConsoleConfig.DEFAULT_REPLICATOR_PORT);
            return replicator;
        }).toList();

        com.ctrip.xpipe.tuple.Pair<List<String>, List<ReplicatorInfoDto>> replicatorInfo = batchInfoInquirer.getReplicatorInfo(replicators);
        return replicatorInfo.getValue().stream().filter(replicatorInfoDto -> Boolean.TRUE.equals(replicatorInfoDto.getMaster())).collect(Collectors.toList());
    }

    private Map<String, List<String>> getReplicatorAz(Drc drc) throws SQLException {
        List<ResourceTbl> resourceTbls = resourceTblDao.queryAllExist();
        Map<String, Long> resourceIp2DcId = resourceTbls.stream().collect(Collectors.toMap(ResourceTbl::getIp, ResourceTbl::getDcId, (s1, s2) -> s1));
        List<DcTbl> dcTbls = dcTblDao.queryAllExist();
        Map<Long, DcTbl> dcMap = dcTbls.stream().collect(Collectors.toMap(DcTbl::getId, e -> e));
        List<String> drcDcNames = dcTbls.stream().map(DcTbl::getDcName).distinct().collect(Collectors.toList());
        Map<String, List<String>> az2ReplicatorInstance = drcDcNames.stream()
                .collect(Collectors.toMap(
                        k -> k,
                        v -> Lists.newArrayList()
                ));

        for (Map.Entry<String, Dc> dcEntry : drc.getDcs().entrySet()) {
            Dc dc = dcEntry.getValue();
            for (Map.Entry<String, DbCluster> dbClusterEntry : dc.getDbClusters().entrySet()) {
                DbCluster dbCluster = dbClusterEntry.getValue();
                List<Replicator> replicators= dbCluster.getReplicators();
                for (Replicator replicator : replicators) {
                    if (!replicator.getMaster()) {
                        continue;
                    }
                    String replicatorIp = replicator.getIp();
                    String replicatorIpPort = replicator.getIp() + ":" + replicator.getPort();
                    if (resourceIp2DcId.containsKey(replicatorIp)) {
                        String az = dcMap.get(resourceIp2DcId.get(replicatorIp)).getDcName();
                        if (az2ReplicatorInstance.containsKey(az)) {
                            az2ReplicatorInstance.get(az).add(replicatorIpPort);
                        } else {
                            az2ReplicatorInstance.put(az, Lists.newArrayList(replicatorIpPort));
                        }
                    } else {
                        logger.warn("get az from replicator {} fail", replicatorIpPort);
                    }
                }
            }
        }
        return az2ReplicatorInstance;
    }
}
