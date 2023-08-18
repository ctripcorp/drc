package com.ctrip.framework.drc.console.service.v2.resource;

import com.ctrip.framework.drc.console.dao.*;
import com.ctrip.framework.drc.console.dao.entity.*;
import com.ctrip.framework.drc.console.dao.entity.v2.ApplierTblV2;
import com.ctrip.framework.drc.console.dao.entity.v2.MhaTblV2;
import com.ctrip.framework.drc.console.dao.v2.ApplierTblV2Dao;
import com.ctrip.framework.drc.console.dao.v2.MhaTblV2Dao;
import com.ctrip.framework.drc.console.enums.BooleanEnum;
import com.ctrip.framework.drc.console.enums.ResourceTagEnum;
import com.ctrip.framework.drc.console.param.v2.resource.DeleteIpParam;
import com.ctrip.framework.drc.console.param.v2.resource.ResourceBuildParam;
import com.ctrip.framework.drc.console.param.v2.resource.ResourceQueryParam;
import com.ctrip.framework.drc.console.utils.ConsoleExceptionUtils;
import com.ctrip.framework.drc.console.vo.v2.ResourceView;
import com.ctrip.framework.drc.core.http.PageReq;
import com.ctrip.platform.dal.dao.annotation.DalTransactional;
import org.apache.commons.lang3.StringUtils;
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
    private ResourceService resourceService;

    private static final int TWO_HUNDRED = 200;

    @Override
    public List<ResourceView> getResourceUnused(int type) throws Exception {
        ResourceQueryParam param = new ResourceQueryParam();
        param.setType(type);
        PageReq pageReq = new PageReq();
        pageReq.setPageSize(TWO_HUNDRED);
        param.setPageReq(pageReq);

        List<ResourceView> resourceViews = resourceService.getResourceView(param);
        List<ResourceView> resourcesUnused = resourceViews.stream().filter(e -> e.getInstanceNum() == 0L).collect(Collectors.toList());
        return resourcesUnused;
    }

    @Override
    public List<String> getDeletedIps(DeleteIpParam param) {
        List<String> unUsedIps = param.getUnusedIps();
        List<String> existIps = param.getExistIps();
        unUsedIps.removeAll(existIps);
        return unUsedIps;
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
            logger.info("updateResourceTbls: {}", updateResourceTbls);
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
        List<MhaTblV2> mhaTblV2s = mhaTblV2Dao.queryAllExist();
        List<MhaTbl> mhaTbls = mhaTblDao.queryAllExist();
        List<ReplicatorGroupTbl> replicatorGroupTbls = replicatorGroupTblDao.queryAllExist();
        List<ApplierGroupTbl> applierGroupTbls = applierGroupTblDao.queryAllExist();
        List<ReplicatorTbl> replicatorTbls = replicatorTblDao.queryAllExist();
        List<ApplierTblV2> applierTbls = applierTblV2Dao.queryAllExist();
        List<ResourceTbl> resourceTbls = resourceTblDao.queryAllExist();

        Map<Long, MhaTbl> mhaTblMap = mhaTbls.stream().collect(Collectors.toMap(MhaTbl::getId, Function.identity()));
        Map<Long, Long> replicatorGroupMap = replicatorGroupTbls.stream().collect(Collectors.toMap(ReplicatorGroupTbl::getMhaId, ReplicatorGroupTbl::getId));
        Map<Long, List<Long>> applierGroupMap = applierGroupTbls.stream().collect(Collectors.groupingBy(ApplierGroupTbl::getMhaId, Collectors.mapping(ApplierGroupTbl::getId, Collectors.toList())));
        Map<Long, ReplicatorTbl> replicatorMap = replicatorTbls.stream().collect(Collectors.toMap(ReplicatorTbl::getRelicatorGroupId, Function.identity(), (k1, k2) -> k1));
        Map<Long, ApplierTblV2> applierMap = applierTbls.stream().collect(Collectors.toMap(ApplierTblV2::getApplierGroupId, Function.identity(), (k1, k2) -> k1));
        Map<Long, String> resourceMap = resourceTbls.stream().collect(Collectors.toMap(ResourceTbl::getId, ResourceTbl::getTag));

        for (MhaTblV2 mhaTblV2 : mhaTblV2s) {
            String tag = ResourceTagEnum.COMMON.getName();
            Long replicatorGroupId = replicatorGroupMap.get(mhaTblV2.getId());
            List<Long> applierGroupIds = applierGroupMap.get(mhaTblV2.getId());

            if (replicatorGroupId != null && replicatorMap.containsKey(replicatorGroupId)) {
                ReplicatorTbl replicatorTbl = replicatorMap.get(replicatorGroupId);
                String replicatorTag = resourceMap.get(replicatorTbl.getResourceId());
                if (!replicatorTag.equals(tag)) {
                    tag = replicatorTag;
                }
            }

            if (!CollectionUtils.isEmpty(applierGroupIds)) {
                for (long applierGroupId : applierGroupIds) {
                    if (!applierMap.containsKey(applierGroupId)) {
                        continue;
                    }
                    ApplierTblV2 applierTblV2 = applierMap.get(applierGroupId);
                    String applierTag = resourceMap.get(applierTblV2.getResourceId());
                    if (!applierTag.equals(ResourceTagEnum.COMMON.getName())) {
                        tag = applierTag;
                    }
                }

            }

            mhaTblV2.setTag(tag);
            MhaTbl mhaTbl = mhaTblMap.get(mhaTblV2.getId());
            if (mhaTbl != null) {
                mhaTbl.setTag(tag);
            }
        }

        mhaTblV2Dao.update(mhaTblV2s);
        mhaTblDao.update(mhaTbls);

        return mhaTbls.size();
    }

    @Override
    public List<Long> getReplicatorGroupIdsWithSameAz() throws Exception {
        List<ReplicatorTbl> replicatorTbls = replicatorTblDao.queryAllExist();
        List<ResourceTbl> resourceTbls = resourceTblDao.queryAllExist();
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
        List<ApplierTblV2> applierTblV2ss = applierTblV2Dao.queryAllExist();
        List<ResourceTbl> resourceTbls = resourceTblDao.queryAllExist();
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
        List<MessengerTbl> messengerTbls = messengerTblDao.queryAllExist();
        List<ResourceTbl> resourceTbls = resourceTblDao.queryAllExist();
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
}
