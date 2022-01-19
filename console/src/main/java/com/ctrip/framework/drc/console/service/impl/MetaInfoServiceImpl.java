package com.ctrip.framework.drc.console.service.impl;

import com.ctrip.framework.drc.console.config.DefaultConsoleConfig;
import com.ctrip.framework.drc.console.dao.GroupMappingTblDao;
import com.ctrip.framework.drc.console.dao.MhaGroupTblDao;
import com.ctrip.framework.drc.console.dao.MhaTblDao;
import com.ctrip.framework.drc.console.dao.entity.*;
import com.ctrip.framework.drc.console.dto.RouteDto;
import com.ctrip.framework.drc.console.enums.BooleanEnum;
import com.ctrip.framework.drc.console.enums.EstablishStatusEnum;
import com.ctrip.framework.drc.console.enums.TableEnum;
import com.ctrip.framework.drc.console.monitor.delay.config.DbClusterSourceProvider;
import com.ctrip.framework.drc.console.monitor.delay.config.MonitorTableSourceProvider;
import com.ctrip.framework.drc.console.service.MetaInfoService;
import com.ctrip.framework.drc.console.utils.DalUtils;
import com.ctrip.framework.drc.console.utils.MySqlUtils;
import com.ctrip.framework.drc.console.utils.XmlUtils;
import com.ctrip.framework.drc.console.vo.MhaGroupPair;
import com.ctrip.framework.drc.console.vo.MhaGroupPairVo;
import com.ctrip.framework.drc.core.driver.command.netty.endpoint.MySqlEndpoint;
import com.ctrip.framework.drc.core.entity.*;
import com.ctrip.framework.drc.core.meta.DBInfo;
import com.ctrip.framework.drc.core.meta.InstanceInfo;
import com.ctrip.framework.drc.core.monitor.enums.ModuleEnum;
import com.ctrip.framework.foundation.Env;
import com.ctrip.framework.foundation.Foundation;
import com.ctrip.platform.dal.dao.DalPojo;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.StringUtils;
import org.dom4j.DocumentException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;

import static com.ctrip.framework.drc.console.config.ConsoleConfig.DEFAULT_REPLICATOR_APPLIER_PORT;
import static com.ctrip.framework.drc.console.config.ConsoleConfig.MHA_GROUP_SIZE;
import static com.ctrip.framework.drc.console.monitor.delay.config.MonitorTableSourceProvider.SOURCE_QCONFIG;

@Service
public class MetaInfoServiceImpl implements MetaInfoService {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired
    private MetaGenerator metaService;

    @Autowired
    private DalServiceImpl dalService;

    @Autowired
    private MonitorTableSourceProvider monitorTableSourceProvider;

    @Autowired
    private DefaultConsoleConfig defaultConsoleConfig;

    @Autowired
    private DbClusterSourceProvider dbClusterSourceProvider;

    private DalUtils dalUtils = DalUtils.getInstance();

    private Env env = Foundation.server().getEnv();

    public List<MhaTbl> getMhaTbls(Long mhaGroupId) throws SQLException {
        Set<Long> mhaIds = dalUtils.getGroupMappingTblDao().queryAll().stream()
                .filter(p -> BooleanEnum.FALSE.getCode().equals(p.getDeleted()) && p.getMhaGroupId().equals(mhaGroupId)).map(GroupMappingTbl::getMhaId).collect(Collectors.toSet());
        return dalUtils.getMhaTblDao().queryAll().stream()
                .filter(p -> BooleanEnum.FALSE.getCode().equals(p.getDeleted()) && mhaIds.contains(p.getId())).collect(Collectors.toList());
    }

    public Map<String, MhaTbl> getMhaTblMap(Long mhaGroupId) throws SQLException {
        Map<String, MhaTbl> mhaTblMap = Maps.newHashMap();
        List<MhaTbl> mhaTbls = getMhaTbls(mhaGroupId);
        for(MhaTbl mhaTbl : mhaTbls) {
            mhaTblMap.put(dalUtils.getDcTblDao().queryByPk(mhaTbl.getDcId()).getDcName(), mhaTbl);
        }
        return mhaTblMap;
    }

    public List<Long> getMhaIds(List<String> mhaNames) {
        List<Long> mhaIds = Lists.newArrayList();
        mhaNames.forEach(mhaName -> {
            try {
                mhaIds.add(dalUtils.getId(TableEnum.MHA_TABLE, mhaName));
            } catch (SQLException e) {
                logger.error("fail get id for mha {}", mhaName, e);
            }
        });
        return mhaIds;
    }

    public Long getMhaGroupId(String srcMha, String dstMha) throws SQLException {
        Long srcMhaId = dalUtils.getId(TableEnum.MHA_TABLE, srcMha);
        Long dstMhaId = dalUtils.getId(TableEnum.MHA_TABLE, dstMha);

        Set<Long> srcMhaGroupIds = dalUtils.getGroupMappingTblDao().queryAll().stream().filter(p -> BooleanEnum.FALSE.getCode().equals(p.getDeleted()) && p.getMhaId().equals(srcMhaId)).map(GroupMappingTbl::getMhaGroupId).collect(Collectors.toSet());
        Set<Long> dstMhaGroupIds = dalUtils.getGroupMappingTblDao().queryAll().stream().filter(p -> BooleanEnum.FALSE.getCode().equals(p.getDeleted()) && p.getMhaId().equals(dstMhaId)).map(GroupMappingTbl::getMhaGroupId).collect(Collectors.toSet());
        srcMhaGroupIds.retainAll(dstMhaGroupIds);

        if (srcMhaGroupIds.size() == 1) {
            return srcMhaGroupIds.iterator().next();
        }
        return null;
    }

    public Long getMhaGroupId(String srcMha, String dstMha,BooleanEnum isDeleted) throws SQLException {
        MhaTbl srcMhaTbl = new MhaTbl();
        MhaTbl dstMhaTbl = new MhaTbl();
        srcMhaTbl.setMhaName(srcMha);
        dstMhaTbl.setMhaName(dstMha);
        List<MhaTbl> mhaTbls = dalUtils.getMhaTblDao().queryBy(srcMhaTbl);
        List<MhaTbl> mhaTbls1 = dalUtils.getMhaTblDao().queryBy(srcMhaTbl);
        if(mhaTbls == null || mhaTbls.size() != 1){
            logger.info("no such mhaTbls name is {}",srcMha);
            return null;
        }
        if(mhaTbls1 == null || mhaTbls1.size() != 1){
            logger.info("no such mhaTbls name is {}",dstMha);
            return null;
        }
        Long srcMhaId = mhaTbls.get(0).getId();
        Long dstMhaId = mhaTbls1.get(0).getId();
        Set<Long> srcMhaGroupIds = dalUtils.getGroupMappingTblDao().queryAll().stream().filter(p -> isDeleted.getCode().equals(p.getDeleted()) && p.getMhaId().equals(srcMhaId)).map(GroupMappingTbl::getMhaGroupId).collect(Collectors.toSet());
        Set<Long> dstMhaGroupIds = dalUtils.getGroupMappingTblDao().queryAll().stream().filter(p -> isDeleted.getCode().equals(p.getDeleted()) && p.getMhaId().equals(dstMhaId)).map(GroupMappingTbl::getMhaGroupId).collect(Collectors.toSet());
        srcMhaGroupIds.retainAll(dstMhaGroupIds);
        if (srcMhaGroupIds.size() == 1) {
            return srcMhaGroupIds.iterator().next();
        }
        return null;
    }

    public MhaGroupTbl getMhaGroup(String srcMha, String dstMha) throws SQLException {
        Long mhaGroupId = getMhaGroupId(srcMha, dstMha);
        return dalUtils.getMhaGroupTblDao().queryByPk(mhaGroupId);
    }

    public Long getMhaGroupId(Long mhaId) throws SQLException {
        return dalUtils.getGroupMappingTblDao().queryAll().stream().filter(p -> BooleanEnum.FALSE.getCode().equals(p.getDeleted()) && p.getMhaId().equals(mhaId)).map(GroupMappingTbl::getMhaGroupId).findFirst().orElse(null);
    }

    public MhaTbl getMha(Long mhaId) throws SQLException {
        return dalUtils.getMhaTblDao().queryByPk(mhaId);
    }

    public Long getMhaGroupId(String mhaName) throws SQLException {
        Long mhaId = dalUtils.getId(TableEnum.MHA_TABLE, mhaName);
        return getMhaGroupId(mhaId);
    }

    public List<Long> getMhaGroupIds(Long mhaId) throws SQLException {
        return  dalUtils.getGroupMappingTblDao().queryAll().stream().filter(p -> BooleanEnum.FALSE.getCode().equals(p.getDeleted()) && p.getMhaId().equals(mhaId)).map(GroupMappingTbl::getMhaGroupId).collect(Collectors.toList());
    }


    public List<Long> getMhaGroupIds(String mhaName) throws SQLException {
        Long mhaId = dalUtils.getId(TableEnum.MHA_TABLE, mhaName);
        return getMhaGroupIds(mhaId);
    }

    public List<MhaGroupTbl> getMhaGroupsForMha(String mha) throws SQLException {
        List<MhaGroupTbl> mhaGroups = Lists.newArrayList();
        List<Long> mhaGroupIds = getMhaGroupIds(mha);
        mhaGroupIds.forEach(mhaGroupId -> {
            try {
                mhaGroups.add(dalUtils.getMhaGroupTblDao().queryByPk(mhaGroupId));
            } catch (SQLException e) {
                logger.warn("Fail get mhaGroupTbl for id:{}", mhaGroupId, e);
            }
        });
        return mhaGroups;
    }

    // get any mha group for mha, usually used for getting user and password
    public MhaGroupTbl getMhaGroupForMha(String mha) throws SQLException {
        List<MhaGroupTbl> mhaGroupsForMha = getMhaGroupsForMha(mha);
        return mhaGroupsForMha.size() != 0 ? mhaGroupsForMha.iterator().next() : null;
    }

    public Long getReplicatorGroupId(String mhaName) {
        try {
            Long mhaId = dalUtils.getId(TableEnum.MHA_TABLE, mhaName);
            return dalUtils.getId(TableEnum.REPLICATOR_GROUP_TABLE, Long.toString(mhaId));
        } catch (SQLException t) {
            logger.error("Fail get ReplicatorGroupId for {}, ", mhaName, t);
            return null;
        }
    }

    public Map<String, ReplicatorTbl> getReplicators(String mha) {
        Map<String, ReplicatorTbl> replicators = Maps.newHashMap();
        Long replicatorGroupId = getReplicatorGroupId(mha);
        if(null != replicatorGroupId) {
            try {
                List<ReplicatorTbl> replicatorTbls = dalUtils.getReplicatorTblDao().queryAll().stream().filter(p -> p.getDeleted().equals(BooleanEnum.FALSE.getCode()) && p.getRelicatorGroupId().equals(replicatorGroupId)).collect(Collectors.toList());
                for(ReplicatorTbl replicatorTbl : replicatorTbls) {
                    Long resourceId = replicatorTbl.getResourceId();
                    String ip = dalUtils.getResourceTblDao().queryByPk(resourceId).getIp();
                    replicators.put(ip, replicatorTbl);
                }
            } catch (SQLException e) {
                logger.error("Fail getReplicator map for {}, ", mha, e);
            }
        }
        return  replicators;
    }

    public List<String> getAllMhaNamesInCluster(Long clusterId) throws SQLException {
        List<Long> mhaIds = Lists.newArrayList();
        List<String> mhaNames = Lists.newArrayList();
        dalUtils.getClusterMhaMapTblDao().queryAll().stream().filter(p -> (p.getDeleted().equals(BooleanEnum.FALSE.getCode()) && clusterId.equals(p.getClusterId()))).forEach(clusterMhaMapTbl -> mhaIds.add(clusterMhaMapTbl.getMhaId()));
        dalUtils.getMhaTblDao().queryAll().forEach(mhaTbl -> {
            if(mhaTbl.getDeleted().equals(BooleanEnum.FALSE.getCode()) && mhaIds.contains(mhaTbl.getId())) {
                mhaNames.add(mhaTbl.getMhaName());
            }
        });
        return mhaNames;
    }

    public List<DBInfo> getMachines(Long mhaGroupId) throws SQLException {
        List<DBInfo> dbInfos = Lists.newArrayList();
        List<MhaTbl> mhaTbls = getMhaTbls(mhaGroupId);
        for(MhaTbl mhaTbl : mhaTbls) {
            List<MachineTbl> machineTbls = getMachineTbls(mhaTbl.getMhaName());
            for(MachineTbl machineTbl : machineTbls) {
                DBInfo dbInfo = new DBInfo();
                dbInfo.setIdc(dalUtils.getDcTblDao().queryByPk(mhaTbl.getDcId()).getDcName());
                dbInfo.setUuid(machineTbl.getUuid());
                dbInfo.setMhaName(mhaTbl.getMhaName());
                dbInfo.setIp(machineTbl.getIp());
                dbInfo.setPort(machineTbl.getPort());
                dbInfo.setCluster(getCluster(mhaTbl.getMhaName()));
                dbInfos.add(dbInfo);
            }
        }
        return dbInfos;
    }

    public List<MachineTbl> getMachineTbls(String mha) {
        List<MachineTbl> machineTbls = Lists.newArrayList();
        try {
            List<DalPojo> allPojos = TableEnum.MHA_TABLE.getAllPojos();
            List<DalPojo> allMachinePojos = TableEnum.MACHINE_TABLE.getAllPojos();
            for (DalPojo pojo : allPojos) {
                MhaTbl mhaTbl = (MhaTbl) pojo;
                if(mha.equalsIgnoreCase(mhaTbl.getMhaName())) {
                    for(DalPojo machinePojo : allMachinePojos) {
                        MachineTbl machineTbl = (MachineTbl) machinePojo;
                        if(machineTbl.getMhaId().equals(mhaTbl.getId())) {
                            machineTbls.add(machineTbl);
                        }
                    }
                }
            }
        } catch(SQLException e) {
            logger.error("Fail get machineTbls for {}", mha);
        }
        logger.debug("[getMachineTbls] for {} : {}", mha, machineTbls);
        return machineTbls;
    }

    public List<String> getMachines(String mha) {
        List<String> machines = Lists.newArrayList();
        List<MachineTbl> machineTbls = getMachineTbls(mha);
        for(MachineTbl machineTbl : machineTbls) {
            String endpoint = machineTbl.getIp() + ":" + machineTbl.getPort();
            if(machineTbl.getMaster().equals(BooleanEnum.TRUE.getCode())) {
                machines.add(0, endpoint);
            } else {
                machines.add(endpoint);
            }
        }
        return machines;
    }

    public List<MhaGroupPair> getAllMhaGroups() throws Exception {
        List<MhaGroupPair> mhaGroupPairs = Lists.newArrayList();
        List<DalPojo> allPojos = TableEnum.MHA_GROUP_TABLE.getAllPojos();
        allPojos.forEach(dalPojo -> {
            MhaGroupTbl mhaGroupTbl = (MhaGroupTbl) dalPojo;
            Long mhaGroupTblId = mhaGroupTbl.getId();
            try {
                List<MhaTbl> mhaTbls = getMhaTbls(mhaGroupTblId);
                if(MHA_GROUP_SIZE != mhaTbls.size()) {
                    logger.info("Fail to get mhas for group id: {}", mhaGroupTblId);
                } else {
                    Integer unitVerificationSwitch = mhaGroupTbl.getUnitVerificationSwitch();
                    Integer monitorSwitch = mhaGroupTbl.getMonitorSwitch();
                    MhaGroupPair mhaGroupPair = new MhaGroupPair(mhaTbls.get(0).getMhaName(),
                            mhaTbls.get(1).getMhaName(),
                            EstablishStatusEnum.getEnumByCode(mhaGroupTbl.getDrcEstablishStatus()),
                            unitVerificationSwitch, monitorSwitch, mhaGroupTblId);
                    if(BooleanEnum.TRUE.getCode().equals(unitVerificationSwitch)) {
                        mhaGroupPairs.add(0, mhaGroupPair);
                    } else {
                        mhaGroupPairs.add(mhaGroupPair);
                    }
                }
            } catch (SQLException e) {
                logger.error("Fail to get mhas for group id: {}", mhaGroupTblId, e);
            }
        });

        return mhaGroupPairs;
    }

    public List<MhaGroupPairVo> getAllOrderedGroupPairs() {
        List<MhaGroupPairVo> mhaGroupPairVos = Lists.newArrayList();
        List<DalPojo> allPojos;
        try {
            allPojos = TableEnum.MHA_GROUP_TABLE.getAllPojos();
            if(null == allPojos){
                logger.info("Fail to get allMhaGroup" );
                return null;
            }
        } catch (SQLException throwables) {
            logger.error("Fail to get allMhaGroup" + throwables.getMessage());
            return null;
        }
        HashMap<String, Integer> srcNameNumMap = srcNameNumCount();
        HashMap<String, List<MhaGroupPairVo>> srcNameListMap = new HashMap<>();
        allPojos.forEach(dalPojo -> {
            MhaGroupTbl mhaGroupTbl = (MhaGroupTbl) dalPojo;
            Long mhaGroupTblId = mhaGroupTbl.getId();
            try {
                List<MhaTbl> mhaTbls = getMhaTbls(mhaGroupTblId);
                String mhaName0 = mhaTbls.get(0).getMhaName();
                String mhaName1 = mhaTbls.get(1).getMhaName();
                Integer unitVerificationSwitch = mhaGroupTbl.getUnitVerificationSwitch();
                Integer monitorSwitch = mhaGroupTbl.getMonitorSwitch();
                if(srcNameNumMap.get(mhaName1) > 1){
                    MhaGroupPairVo mhaGroupPairVo = new MhaGroupPairVo(mhaName1, mhaName0,
                            mhaGroupTbl.getDrcEstablishStatus(), unitVerificationSwitch ,monitorSwitch, mhaGroupTblId);
                    List<MhaGroupPairVo> list = srcNameListMap.getOrDefault(mhaName1, new ArrayList<MhaGroupPairVo>());
                    list.add(mhaGroupPairVo);
                    srcNameListMap.put(mhaName1,list);
                }else {
                    MhaGroupPairVo mhaGroupPair = new MhaGroupPairVo(mhaName0, mhaName1,
                            mhaGroupTbl.getDrcEstablishStatus(), unitVerificationSwitch ,monitorSwitch, mhaGroupTblId);
                    List<MhaGroupPairVo> list = srcNameListMap.getOrDefault(mhaName0, new ArrayList<MhaGroupPairVo>());
                    list.add(mhaGroupPair);
                    srcNameListMap.put(mhaName0,list);
                }
            } catch (SQLException e) {
                logger.error("Fail occur in SQL" + e.getMessage());
            }
        });
        Collection<List<MhaGroupPairVo>> allPairs = srcNameListMap.values();
        Iterator<List<MhaGroupPairVo>> iterator = allPairs.iterator();
        while(iterator.hasNext()){
            List<MhaGroupPairVo> list = iterator.next();
            list.forEach(mhaGroupPairVo -> {
                mhaGroupPairVos.add(mhaGroupPairVo);
            });
        }
        return mhaGroupPairVos;
    }

    private HashMap<String, Integer> srcNameNumCount(){
        List<DalPojo> allPojos;
        try {
            allPojos = TableEnum.MHA_GROUP_TABLE.getAllPojos();
            if(null == allPojos){
                logger.error("Fail to get allMhaGroup" );
                return null;
            }
        } catch (SQLException throwables) {
            logger.error("Fail to get allMhaGroup" + throwables.getMessage());
            return null;
        }
        HashMap<String, Integer> srcNameNumMap = new HashMap<>();
        allPojos.forEach(dalPojo -> {
            MhaGroupTbl mhaGroupTbl = (MhaGroupTbl) dalPojo;
            Long mhaGroupTblId = mhaGroupTbl.getId();
            try {
                List<MhaTbl> mhaTbls = getMhaTbls(mhaGroupTblId);
                String mhaName0 = mhaTbls.get(0).getMhaName();
                String mhaName1 = mhaTbls.get(1).getMhaName();
                if (MHA_GROUP_SIZE != mhaTbls.size()) {
                    logger.info("Fail to get mhas for group id: {}", mhaGroupTblId);
                } else {
                    srcNameNumMap.put(mhaName0, srcNameNumMap.getOrDefault(mhaName0, 0) + 1);
                    srcNameNumMap.put(mhaName1, srcNameNumMap.getOrDefault(mhaName1, 0) + 1);
                }
            } catch (SQLException e) {
                logger.error("Fail occur in SQL" + e.getMessage());
            }
        });
        return srcNameNumMap;
    }

    public List<MhaGroupPairVo> getAllOrderedDeletedGroupPairs() throws SQLException {
        List<MhaGroupPairVo> mhaGroupPairVos = Lists.newArrayList();
        MhaGroupTblDao mhaGroupTblDao = dalUtils.getMhaGroupTblDao();
        List<MhaGroupTbl> deletedMhaGroups = mhaGroupTblDao.queryAll().stream()
                .filter(predicate -> BooleanEnum.TRUE.getCode().equals(predicate.getDeleted()))
                .collect(Collectors.toList());
        HashMap<String, Integer> srcNameNumMap = srcNameNumMap(deletedMhaGroups, BooleanEnum.TRUE);
        HashMap<String, List<MhaGroupPairVo>> srcNameListMap = new HashMap<>();
        for (MhaGroupTbl mhaGroupTbl : deletedMhaGroups) {
            Long mhaGroupTblId = mhaGroupTbl.getId();
            List<MhaTbl> mhaTbls = getMhasByGroupId(mhaGroupTblId, BooleanEnum.TRUE);
            if (mhaTbls.size() != MHA_GROUP_SIZE) continue;
            String mhaName0 = mhaTbls.get(0).getMhaName();
            String mhaName1 = mhaTbls.get(1).getMhaName();
            MhaGroupPairVo mhaGroupPairVo = new MhaGroupPairVo();
            if (srcNameNumMap.get(mhaName1) > 1) {
                mhaGroupPairVo.setSrcMha(mhaName1);
                mhaGroupPairVo.setDestMha(mhaName0);
                List<MhaGroupPairVo> list = srcNameListMap.getOrDefault(mhaName1, new ArrayList<MhaGroupPairVo>());
                list.add(mhaGroupPairVo);
                srcNameListMap.put(mhaName1, list);
            } else {
                mhaGroupPairVo.setSrcMha(mhaName0);
                mhaGroupPairVo.setDestMha(mhaName1);
                List<MhaGroupPairVo> list = srcNameListMap.getOrDefault(mhaName0, new ArrayList<MhaGroupPairVo>());
                list.add(mhaGroupPairVo);
                srcNameListMap.put(mhaName0, list);
            }
        }
        Iterator<List<MhaGroupPairVo>> iterator = srcNameListMap.values().iterator();
        while (iterator.hasNext()) {
            List<MhaGroupPairVo> list = iterator.next();
            list.forEach(mhaGroupPairVo -> {
                mhaGroupPairVos.add(mhaGroupPairVo);
            });
        }
        return mhaGroupPairVos;
    }

    public List<MhaTbl> getMhasByGroupId(Long mhaGroupTblId,BooleanEnum isDeleted) {
        ArrayList<MhaTbl> mhaTbls = Lists.newArrayList();
        try {
            GroupMappingTblDao groupMappingTblDao = dalUtils.getGroupMappingTblDao();
            List<Long> mhaIds = groupMappingTblDao.queryAll().stream()
                    .filter(p -> mhaGroupTblId.equals(p.getMhaGroupId()) && isDeleted.getCode().equals(p.getDeleted()))
                    .map(GroupMappingTbl::getMhaId).collect(Collectors.toList());
            if (mhaIds.size() != MHA_GROUP_SIZE) {
                logger.info("Fail to get mhas for group id: {}", mhaGroupTblId);
                return mhaTbls;
            }
            MhaTblDao mhaTblDao = dalUtils.getMhaTblDao();
            for (Long mhaId : mhaIds) {
                MhaTbl mhaTbl = mhaTblDao.queryByPk(mhaId);
                mhaTbls.add(mhaTbl);
            }
        } catch (SQLException e) {
            logger.error("Fail occur in SQL" + e.getMessage());
        }
        return mhaTbls;
    }

    private HashMap<String, Integer> srcNameNumMap(List<MhaGroupTbl> mhaGroups,BooleanEnum isDeleted) {
        HashMap<String, Integer> srcNameNumMap = new HashMap<>();
        for (MhaGroupTbl mhaGroupTbl : mhaGroups) {
            List<MhaTbl> mhaTbls = getMhasByGroupId(mhaGroupTbl.getId(),isDeleted);
            if (MHA_GROUP_SIZE != mhaTbls.size()) {
                logger.info("Fail to get mhas for group id: {}", mhaGroupTbl.getId());
            } else {
                String mhaName0 = mhaTbls.get(0).getMhaName();
                String mhaName1 = mhaTbls.get(1).getMhaName();
                srcNameNumMap.put(mhaName0, srcNameNumMap.getOrDefault(mhaName0, 0) + 1);
                srcNameNumMap.put(mhaName1, srcNameNumMap.getOrDefault(mhaName1, 0) + 1);
            }
        }
        return srcNameNumMap;
    }

    public String getXmlConfiguration(String srcMha, String dstMha) throws Exception {
        Long mhaGroupId = getMhaGroupId(srcMha, dstMha);
        return getXmlConfiguration(mhaGroupId);
    }

    public String getXmlConfiguration(String srcMha, String dstMha,BooleanEnum isDeleted) throws Exception {
        Long mhaGroupId = getMhaGroupId(srcMha, dstMha,isDeleted); // BooleanEnum
        return getXmlConfiguration(mhaGroupId,isDeleted);
    }

    public String getXmlConfiguration(Long mhaGroupId,BooleanEnum isDeleted) throws DocumentException {
        Drc drc = new Drc();
        try{
            List<MhaTbl> mhaTbls = getMhasByGroupId(mhaGroupId,isDeleted); // BooleanEnum
            if(MHA_GROUP_SIZE != mhaTbls.size()) {
                return XmlUtils.formatXML(drc.toString());
            }
            // one2many consider use mhaTbl.getDeleted();
            generateViewDbCluster(drc, mhaTbls.get(0), mhaTbls.get(1),isDeleted); // BooleanEnum
            generateViewDbCluster(drc, mhaTbls.get(1), mhaTbls.get(0),isDeleted); // BooleanEnum
        } catch (SQLException e) {
            logger.error("Fail to get xml config for mhaGroup : {}", mhaGroupId, e);
        }

        return XmlUtils.formatXML(drc.toString());
    }

    public String getXmlConfiguration(Long mhaGroupId) throws DocumentException {
        Drc drc = new Drc();

        try {
            List<MhaTbl> mhaTbls = getMhaTbls(mhaGroupId);
            if(MHA_GROUP_SIZE != mhaTbls.size()) {
                return XmlUtils.formatXML(drc.toString());
            }
            generateViewDbCluster(drc, mhaTbls.get(0), mhaTbls.get(1));
            generateViewDbCluster(drc, mhaTbls.get(1), mhaTbls.get(0));
        } catch (SQLException e) {
            logger.error("Fail to get xml config for mhaGroup : {}", mhaGroupId, e);
        }

        return XmlUtils.formatXML(drc.toString());
    }

    public List<String> getResources(String type) {
        List<String> resources = Lists.newArrayList();
        try {
            int typeCode = ModuleEnum.getModuleEnum(type).getCode();
            List<ResourceTbl> resourceTbls = dalUtils.getResourceTblDao().queryAll().stream().filter(predicate -> predicate.getDeleted().equals(BooleanEnum.FALSE.getCode())).collect(Collectors.toList());
            resourceTbls.stream().filter(r -> typeCode == r.getType()).forEach(resourceTbl -> resources.add(resourceTbl.getIp()));
        } catch (Exception e) {
            logger.error("Fail get resource, type is: {}", type, e);
        }
        return resources;
    }

    public int updateMhaDc(String mha, String dcName) {
        try {
            DcTbl sample = new DcTbl();
            sample.setDcName(dcName);
            DcTbl dcTbl = dalUtils.getDcTblDao().queryBy(sample).get(0);
            if (dcTbl == null) {
                return 0;
            }
            long dcId = dcTbl.getId();
            MhaTbl mhaTbl = dalUtils.getMhaTblDao().queryAll().stream().filter(p -> (mha.equalsIgnoreCase(p.getMhaName()) && p.getDeleted().equals(BooleanEnum.FALSE.getCode()))).findFirst().orElse(null);
            if (mhaTbl == null) {
                return 0;
            }
            MhaTbl toUpdateMha = new MhaTbl();
            toUpdateMha.setId(mhaTbl.getId());
            toUpdateMha.setDcId(dcId);
            return dalUtils.getMhaTblDao().update(toUpdateMha);
        } catch (SQLException e) {
            logger.error("update mha dc name error, mha is: {}, dc name is: {}", mha, dcName, e);
        }
        return 0;
    }

    public List<String> getResourcesInDcOfMha(String mha, String type) {
        try {
            MhaTbl mhaTbl = dalUtils.getMhaTblDao().queryAll().stream().filter(p -> (mha.equalsIgnoreCase(p.getMhaName()) && p.getDeleted().equals(BooleanEnum.FALSE.getCode()))).findFirst().orElse(null);
            if(null != mhaTbl) {
                logger.info("get {} in metadb given {}", type, mha);
                Long dcId = mhaTbl.getDcId();
                DcTbl dcTbl = dalUtils.getDcTblDao().queryByPk(dcId);
                if(null != dcTbl) {
                    String dcName = dcTbl.getDcName();
                    logger.info("get {} in {}({})", type, dcName, mha);
                    if(ModuleEnum.REPLICATOR.getDescription().equals(type)) {
                        return getReplicatorResources(dcName);
                    } else if (ModuleEnum.APPLIER.getDescription().equals(type)) {
                        return getApplierResources(dcName);
                    }
                }
            }
        } catch (SQLException e) {
            logger.error("Fail get {} in metadb given {}, ", type, mha, e);
        }
        return Lists.newArrayList();
    }

    public List<String> getReplicatorResources(String dc) throws SQLException {
        List<String> replicatorResources = Lists.newArrayList();
        List<ResourceTbl> resourceTbls = dalUtils.getResourceTblDao().queryAll().stream().filter(predicate -> predicate.getDeleted().equals(BooleanEnum.FALSE.getCode())).collect(Collectors.toList());
        Long dcId = dalUtils.getId(TableEnum.DC_TABLE, dc);
        if(null != dcId) {
            logger.info("{} dcId: {}", dc, dcId);
            resourceTbls.stream().filter(r -> dcId.equals(r.getDcId()) && ModuleEnum.REPLICATOR.getCode() == r.getType()).forEach(resourceTbl -> replicatorResources.add(resourceTbl.getIp()));
        }
        return replicatorResources;
    }

    public List<String> getApplierResources(String dc) throws SQLException {
        List<String> applierResources = Lists.newArrayList();
        List<ResourceTbl> resourceTbls = dalUtils.getResourceTblDao().queryAll().stream().filter(predicate -> predicate.getDeleted().equals(BooleanEnum.FALSE.getCode())).collect(Collectors.toList());
        Long dcId = dalUtils.getId(TableEnum.DC_TABLE, dc);
        if(null != dcId) {
            logger.info("{} dcId: {}", dc, dcId);
            resourceTbls.stream().filter(r -> dcId.equals(r.getDcId()) && ModuleEnum.APPLIER.getCode() == r.getType()).forEach(resourceTbl -> applierResources.add(resourceTbl.getIp()));
        }
        return applierResources;
    }

    public List<String> getResourcesInUse(String mha, String remoteMha, String type) {
        try {
            MhaTbl mhaTbl = dalUtils.getMhaTblDao().queryAll().stream().filter(p -> (mha.equalsIgnoreCase(p.getMhaName()) && p.getDeleted().equals(BooleanEnum.FALSE.getCode()))).findFirst().orElse(null);
            MhaTbl remoteMhaTbl = dalUtils.getMhaTblDao().queryAll().stream().filter(p -> (remoteMha.equalsIgnoreCase(p.getMhaName()) && p.getDeleted().equals(BooleanEnum.FALSE.getCode()))).findFirst().orElse(null);
            if(null != mhaTbl) {
                logger.info("get {} of {}<-{} in metadb", type, mha, remoteMha);
                if(ModuleEnum.REPLICATOR.getDescription().equals(type)) {
                    ReplicatorGroupTbl replicatorGroupTbl = dalUtils.getReplicatorGroupTblDao().queryAll().stream().filter(p -> (p.getDeleted().equals(BooleanEnum.FALSE.getCode()) && mhaTbl.getId().equals(p.getMhaId()))).findFirst().orElse(null);
                    if (null != replicatorGroupTbl) {
                        logger.info("get {} of {} in metadb, rg{}", type, mha, replicatorGroupTbl.getId());
                        return getReplicatorIps(replicatorGroupTbl.getId());
                    }
                } else if (ModuleEnum.APPLIER.getDescription().equals(type) && null != remoteMhaTbl) {
                    ApplierGroupTbl applierGroupTbl = getApplierGroupTbl(mhaTbl, remoteMhaTbl);
                    if (null != applierGroupTbl) {
                        logger.info("get {} of {}<-{} in metadb, ag{}", type, mha, remoteMha, applierGroupTbl.getId());
                        return getApplierIps(applierGroupTbl.getId());
                    }
                }
            }
        } catch (SQLException e) {
            logger.error("Fail get {} of {}->{} in metadb, ", type, mha, remoteMha, e);
        }
        return Lists.newArrayList();
    }

    public String getIncludedDbs(String mha, String remoteMha) throws SQLException {
        MhaTbl mhaTbl = dalUtils.getMhaTblDao().queryAll().stream().filter(p -> (mha.equalsIgnoreCase(p.getMhaName()) && p.getDeleted().equals(BooleanEnum.FALSE.getCode()))).findFirst().orElse(null);
        MhaTbl remoteMhaTbl = dalUtils.getMhaTblDao().queryAll().stream().filter(p -> (remoteMha.equalsIgnoreCase(p.getMhaName()) && p.getDeleted().equals(BooleanEnum.FALSE.getCode()))).findFirst().orElse(null);
        ApplierGroupTbl applierGroupTbl = getApplierGroupTbl(mhaTbl, remoteMhaTbl);
        return applierGroupTbl == null ? null : applierGroupTbl.getIncludedDbs();
    }

    public String getNameFilter(String mha, String remoteMha) throws SQLException {
        MhaTbl mhaTbl = dalUtils.getMhaTblDao().queryAll().stream().filter(p -> (mha.equalsIgnoreCase(p.getMhaName()) && p.getDeleted().equals(BooleanEnum.FALSE.getCode()))).findFirst().orElse(null);
        MhaTbl remoteMhaTbl = dalUtils.getMhaTblDao().queryAll().stream().filter(p -> (remoteMha.equalsIgnoreCase(p.getMhaName()) && p.getDeleted().equals(BooleanEnum.FALSE.getCode()))).findFirst().orElse(null);
        ApplierGroupTbl applierGroupTbl = getApplierGroupTbl(mhaTbl, remoteMhaTbl);
        return applierGroupTbl == null ? null : applierGroupTbl.getNameFilter();
    }

    public int getApplyMode(String mha, String remoteMha) throws SQLException {
        MhaTbl mhaTbl = dalUtils.getMhaTblDao().queryAll().stream().filter(p -> (mha.equalsIgnoreCase(p.getMhaName()) && p.getDeleted().equals(BooleanEnum.FALSE.getCode()))).findFirst().orElse(null);
        MhaTbl remoteMhaTbl = dalUtils.getMhaTblDao().queryAll().stream().filter(p -> (remoteMha.equalsIgnoreCase(p.getMhaName()) && p.getDeleted().equals(BooleanEnum.FALSE.getCode()))).findFirst().orElse(null);
        ApplierGroupTbl applierGroupTbl = getApplierGroupTbl(mhaTbl, remoteMhaTbl);
        return applierGroupTbl == null ? 0 : applierGroupTbl.getApplyMode();
    }

    private ApplierGroupTbl getApplierGroupTbl(MhaTbl mhaTbl, MhaTbl remoteMhaTbl) throws SQLException {
        ReplicatorGroupTbl remoteReplicatorGroupTbl = dalUtils.getReplicatorGroupTblDao().queryAll().stream().filter(p -> (p.getDeleted().equals(BooleanEnum.FALSE.getCode()) && remoteMhaTbl.getId().equals(p.getMhaId()))).findFirst().orElse(null);
        return dalUtils.getApplierGroupTblDao().queryAll().stream()
                .filter(p -> (p.getDeleted().equals(BooleanEnum.FALSE.getCode()) && mhaTbl.getId().equals(p.getMhaId()) && p.getReplicatorGroupId().equals(remoteReplicatorGroupTbl.getId())))
                .findFirst().orElse(null);
    }

    public List<Integer> getReplicatorInstances(String ip) throws SQLException {
        List<Integer> replicatorInstances =  Lists.newArrayList();
        List<ReplicatorTbl> replicatorTbls = dalUtils.getReplicatorTblDao().queryAll().stream().filter(predicate -> predicate.getDeleted().equals(BooleanEnum.FALSE.getCode())).collect(Collectors.toList());
        Long resourceId = dalUtils.getId(TableEnum.RESOURCE_TABLE, ip);
        if(null != resourceId) {
            logger.info("{} resourceId: {}", ip, resourceId);
            replicatorTbls.stream().filter(r -> resourceId.equals(r.getResourceId())).forEach(replicatorTbl -> replicatorInstances.add(replicatorTbl.getApplierPort()));
        }
        return replicatorInstances;
    }

    private Dc generateDcFrame(Drc drc, String dcName) {
        logger.info("generate view dc: {}", dcName);
        Dc dc = new Dc(dcName);
        drc.addDc(dc);
        return dc;
    }

    private DbCluster generateDbCluster(Dc dc, MhaTbl mhaTbl) {
        // header tag
        String mhaName = mhaTbl.getMhaName();
        Long mhaId = mhaTbl.getId();
        ClusterMhaMapTbl clusterMhaMapTbl = metaService.getClusterMhaMapTbls().stream().filter(cMMapTbl -> cMMapTbl.getMhaId().equals(mhaId)).findFirst().get();
        Long clusterId = clusterMhaMapTbl.getClusterId();
        ClusterTbl clusterTbl = metaService.getClusterTbls().stream().filter(cTbl -> cTbl.getId().equals(clusterId)).findFirst().get();
        String clusterName = clusterTbl.getClusterName();
        BuTbl buTbl = metaService.getBuTbls().stream().filter(predicate -> predicate.getId().equals(clusterTbl.getBuId())).findFirst().get();
        logger.info("generate view dbCluster for mha: {}", mhaName);
        DbCluster dbCluster = new DbCluster();
        dbCluster.setId(clusterName+'.'+mhaName)
                .setName(clusterName)
                .setMhaName(mhaName)
                .setBuName(buTbl.getBuName())
                .setAppId(clusterTbl.getClusterAppId());
        dc.addDbCluster(dbCluster);
        return dbCluster;
    }

    private Dbs generateDbs(DbCluster dbCluster, MhaTbl mhaTbl) {
        // dbs
        logger.info("generate view dbs for mha: {}", mhaTbl.getMhaName());
        // not show password for preview
        Dbs dbs = new Dbs();
        dbCluster.setDbs(dbs);
        List<MachineTbl> curMhaMachineTbls = metaService.getMachineTbls().stream().filter(predicate -> predicate.getMhaId().equals(mhaTbl.getId())).collect(Collectors.toList());
        for(MachineTbl machineTbl : curMhaMachineTbls) {
            logger.info("generate view machine: {} for mha: {}", machineTbl.getIp(), mhaTbl.getMhaName());
            Db db = new Db();
            db.setIp(machineTbl.getIp())
                    .setPort(machineTbl.getPort())
                    .setMaster(machineTbl.getMaster().equals(BooleanEnum.TRUE.getCode()))
                    .setUuid(machineTbl.getUuid());
            dbs.addDb(db);
        }
        return dbs;
    }

    private Dbs generateDbs(DbCluster dbCluster, MhaTbl mhaTbl,BooleanEnum isDeleted) {
        // dbs
        logger.info("generate view dbs for mha: {}", mhaTbl.getMhaName());
        // not show password for preview
        Dbs dbs = new Dbs();
        dbCluster.setDbs(dbs);
        List<MachineTbl> curMhaMachineTbls = Lists.newArrayList();
        if (BooleanEnum.FALSE.equals(isDeleted)){
            curMhaMachineTbls = metaService.getMachineTbls().stream().
                    filter(predicate -> predicate.getMhaId().equals(mhaTbl.getId())).collect(Collectors.toList());
        } else {
            try {
                curMhaMachineTbls = dalUtils.getMachineTblDao().queryAll().stream()
                        .filter(p -> isDeleted.getCode().equals(p.getDeleted()) && mhaTbl.getId().equals(p.getMhaId()))
                        .collect(Collectors.toList());
            } catch (SQLException e) {
                logger.error("Fail to exc SQL in generateDbs: " + e.getMessage());
            }
        }
        for(MachineTbl machineTbl : curMhaMachineTbls) {
            logger.info("generate view machine: {} for mha: {}", machineTbl.getIp(), mhaTbl.getMhaName());
            Db db = new Db();
            db.setIp(machineTbl.getIp())
                    .setPort(machineTbl.getPort())
                    .setMaster(machineTbl.getMaster().equals(BooleanEnum.TRUE.getCode()))
                    .setUuid(machineTbl.getUuid());
            dbs.addDb(db);
        }
        return dbs;
    }

    private void generateReplicators(DbCluster dbCluster, MhaTbl mhaTbl) throws SQLException {
        //  replicators
        ReplicatorGroupTbl replicatorGroupTbl = metaService.getReplicatorGroupTbls().stream().filter(rg -> rg.getMhaId().equals(mhaTbl.getId())).findFirst().get();
        List<ReplicatorTbl> replicatorTbls = dalUtils.getReplicatorTblDao().queryAll().stream().filter(r -> (r.getDeleted().equals(BooleanEnum.FALSE.getCode()) && r.getRelicatorGroupId().equals(replicatorGroupTbl.getId()))).collect(Collectors.toList());
        for(ReplicatorTbl replicatorTbl : replicatorTbls) {
            ResourceTbl resourceTbl = metaService.getResourceTbls().stream().filter(r -> r.getId().equals(replicatorTbl.getResourceId())).findFirst().get();
            logger.info("generate view replicator: {}:{} for mha: {}", resourceTbl.getIp(), replicatorTbl.getApplierPort(), mhaTbl.getMhaName());
            Replicator replicator = new Replicator();
            replicator.setIp(resourceTbl.getIp())
                    .setPort(replicatorTbl.getPort())
                    .setApplierPort(replicatorTbl.getApplierPort())
                    .setGtidSkip(replicatorTbl.getGtidInit())
                    .setMaster(BooleanEnum.TRUE.getCode().equals(replicatorTbl.getMaster()));
            dbCluster.addReplicator(replicator);
        }
    }

    private void generateReplicators(DbCluster dbCluster, MhaTbl mhaTbl,BooleanEnum isDeleted) throws SQLException {
        //  replicators
        List<ReplicatorTbl> replicatorTbls = Lists.newArrayList();
        if(BooleanEnum.FALSE.equals(isDeleted)){
            ReplicatorGroupTbl replicatorGroupTbl = metaService.getReplicatorGroupTbls().stream()
                    .filter(rg -> rg.getMhaId().equals(mhaTbl.getId())).findFirst().get();
            replicatorTbls = dalUtils.getReplicatorTblDao().queryAll().stream()
                    .filter(r -> (r.getDeleted().equals(BooleanEnum.FALSE.getCode())
                            && r.getRelicatorGroupId().equals(replicatorGroupTbl.getId()))).collect(Collectors.toList());
        }else{
            ReplicatorGroupTbl replicatorGroupTbl = dalUtils.getReplicatorGroupTblDao().queryAll().stream()
                    .filter(p -> isDeleted.getCode().equals(p.getDeleted()) && mhaTbl.getId().equals(p.getMhaId())).findFirst().get();
            replicatorTbls = dalUtils.getReplicatorTblDao().queryAll().stream()
                    .filter(p -> isDeleted.getCode().equals(p.getDeleted())
                            && p.getRelicatorGroupId().equals(replicatorGroupTbl.getId())).collect(Collectors.toList());
        }
        for(ReplicatorTbl replicatorTbl : replicatorTbls) {
            ResourceTbl resourceTbl = metaService.getResourceTbls().stream()
                    .filter(r -> r.getId().equals(replicatorTbl.getResourceId())).findFirst().get();
            logger.info("generate view replicator: {}:{} for mha: {}", resourceTbl.getIp()
                    , replicatorTbl.getApplierPort(), mhaTbl.getMhaName());
            Replicator replicator = new Replicator();
            replicator.setIp(resourceTbl.getIp())
                    .setPort(replicatorTbl.getPort())
                    .setApplierPort(replicatorTbl.getApplierPort())
                    .setGtidSkip(replicatorTbl.getGtidInit())
                    .setMaster(BooleanEnum.TRUE.getCode().equals(replicatorTbl.getMaster()));
            dbCluster.addReplicator(replicator);
        }

    }

    private void generateAppliers(DbCluster dbCluster, MhaTbl mhaTbl, MhaTbl targetMhaTbl) throws SQLException {
        // appliers
        ReplicatorGroupTbl replicatorGroupTbl = metaService.getReplicatorGroupTbls().stream().filter(rg -> rg.getMhaId().equals(targetMhaTbl.getId())).findFirst().get();
        ApplierGroupTbl applierGroupTbl = metaService.getApplierGroupTbls().stream().filter(ag -> ag.getMhaId().equals(mhaTbl.getId()) && ag.getReplicatorGroupId().equals(replicatorGroupTbl.getId())).findFirst().get();
        List<ApplierTbl> applierTbls = dalUtils.getApplierTblDao().queryAll().stream().filter(a -> (a.getDeleted().equals(BooleanEnum.FALSE.getCode()) && a.getApplierGroupId().equals(applierGroupTbl.getId()))).collect(Collectors.toList());
        for(ApplierTbl applierTbl : applierTbls) {
            ResourceTbl resourceTbl = metaService.getResourceTbls().stream().filter(r -> r.getId().equals(applierTbl.getResourceId())).findFirst().get();
            logger.info("generate view applier: {} for mha: {}", resourceTbl.getIp(), mhaTbl.getMhaName());
            String targetIdc = metaService.getDcTbls().stream().filter(d -> d.getId().equals(targetMhaTbl.getDcId())).findFirst().get().getDcName();
            Applier applier = new Applier();
            applier.setIp(resourceTbl.getIp())
                    .setPort(applierTbl.getPort())
                    .setTargetIdc(targetIdc)
                    .setTargetMhaName(targetMhaTbl.getMhaName())
                    .setGtidExecuted(applierTbl.getGtidInit())
                    .setIncludedDbs(applierGroupTbl.getIncludedDbs())
                    .setNameFilter(applierGroupTbl.getNameFilter())
                    .setTargetName(applierGroupTbl.getTargetName())
                    .setApplyMode(applierGroupTbl.getApplyMode());
            dbCluster.addApplier(applier);
        }
    }

    private void generateAppliers(DbCluster dbCluster, MhaTbl mhaTbl, MhaTbl targetMhaTbl,BooleanEnum isDeleted0,BooleanEnum isDeleted1) throws SQLException {
        // appliers
        //targetMha ->replicatorGroup_id---mha--->applierGroup
        List<ApplierTbl> applierTbls = Lists.newArrayList();
        ApplierGroupTbl applierGroupTbl = new ApplierGroupTbl();
        if(BooleanEnum.FALSE.equals(isDeleted0) && BooleanEnum.FALSE.equals(isDeleted1)){
            ReplicatorGroupTbl replicatorGroupTbl = metaService.getReplicatorGroupTbls().stream().filter(rg -> rg.getMhaId().equals(targetMhaTbl.getId())).findFirst().get();
            applierGroupTbl = metaService.getApplierGroupTbls().stream().filter(ag -> ag.getMhaId().equals(mhaTbl.getId()) && ag.getReplicatorGroupId().equals(replicatorGroupTbl.getId())).findFirst().get();
            ApplierGroupTbl finalApplierGroupTbl = applierGroupTbl;
            applierTbls = dalUtils.getApplierTblDao().queryAll().stream().filter(a -> (a.getDeleted().equals(BooleanEnum.FALSE.getCode()) && a.getApplierGroupId().equals(finalApplierGroupTbl.getId()))).collect(Collectors.toList());
        } else {
            ReplicatorGroupTbl replicatorGroupTbl = dalUtils.getReplicatorGroupTblDao().queryAll().stream()
                    .filter(p -> isDeleted1.getCode().equals(p.getDeleted()) && p.getMhaId().equals(targetMhaTbl.getId())).findFirst().get();
            applierGroupTbl = dalUtils.getApplierGroupTblDao().queryAll().stream()
                    .filter(p -> isDeleted0.getCode().equals(p.getDeleted()) && p.getMhaId().equals(mhaTbl.getId())
                            && p.getReplicatorGroupId().equals(replicatorGroupTbl.getId())).findFirst().get();
            ApplierGroupTbl finalApplierGroupTbl1 = applierGroupTbl;
            applierTbls = dalUtils.getApplierTblDao().queryAll().stream()
                    .filter(p -> (isDeleted0.getCode().equals(p.getDeleted()) &&
                            p.getApplierGroupId().equals(finalApplierGroupTbl1.getId()))).collect(Collectors.toList());
        }
        for(ApplierTbl applierTbl : applierTbls) {
            ResourceTbl resourceTbl = metaService.getResourceTbls().stream().filter(r -> r.getId().equals(applierTbl.getResourceId())).findFirst().get();
            logger.info("generate view applier: {} for mha: {}", resourceTbl.getIp(), mhaTbl.getMhaName());
            String targetIdc = metaService.getDcTbls().stream().filter(d -> d.getId().equals(targetMhaTbl.getDcId())).findFirst().get().getDcName();
            Applier applier = new Applier();
            applier.setIp(resourceTbl.getIp())
                    .setPort(applierTbl.getPort())
                    .setTargetIdc(targetIdc)
                    .setTargetMhaName(targetMhaTbl.getMhaName())
                    .setGtidExecuted(applierTbl.getGtidInit())
                    .setIncludedDbs(applierGroupTbl.getIncludedDbs())
                    .setNameFilter(applierGroupTbl.getNameFilter())
                    .setTargetName(applierGroupTbl.getTargetName())
                    .setApplyMode(applierGroupTbl.getApplyMode());
            dbCluster.addApplier(applier);
        }
    }

    protected void generateViewDbCluster(Drc drc, MhaTbl mhaTbl, MhaTbl targetMhaTbl) throws SQLException {
        DcTbl dcTbl = metaService.getDcTbls().stream().filter(d -> d.getId().equals(mhaTbl.getDcId())).findFirst().get();
        String dcName = dcTbl.getDcName();
        Dc dc = generateDcFrame(drc, dcName);
        DbCluster dbCluster = generateDbCluster(dc, mhaTbl);
        generateDbs(dbCluster, mhaTbl);
        generateReplicators(dbCluster, mhaTbl);
        generateAppliers(dbCluster, mhaTbl, targetMhaTbl);
    }

    protected void generateViewDbCluster(Drc drc, MhaTbl mhaTbl, MhaTbl targetMhaTbl,BooleanEnum isDeleted) throws SQLException {
        DcTbl dcTbl = metaService.getDcTbls().stream().filter(d -> d.getId().equals(mhaTbl.getDcId())).findFirst().get();
        String dcName = dcTbl.getDcName();
        Dc dc = generateDcFrame(drc, dcName);
        DbCluster dbCluster = generateDbCluster(dc, mhaTbl);
        // isDeleted means group's status,isDelete0 means mha0's status...because one mha maybe not deleted
        BooleanEnum isDeleted0 = BooleanEnum.FALSE;
        BooleanEnum isDeleted1 = BooleanEnum.FALSE;
        if(BooleanEnum.TRUE.equals(isDeleted) && BooleanEnum.TRUE.getCode().equals(mhaTbl.getDeleted())){
            isDeleted0 = BooleanEnum.TRUE;
        }
        if(BooleanEnum.TRUE.equals(isDeleted) && BooleanEnum.TRUE.getCode().equals(targetMhaTbl.getDeleted())){
            isDeleted1 = BooleanEnum.TRUE;
        }
        generateDbs(dbCluster, mhaTbl,isDeleted0);
        generateReplicators(dbCluster, mhaTbl,isDeleted0);
        generateAppliers(dbCluster, mhaTbl, targetMhaTbl,isDeleted0,isDeleted1);
    }

    public Endpoint getMasterEndpoint(MhaTbl mhaTbl) throws SQLException {
        MhaGroupTbl mhaGroupTbl = getMhaGroupForMha(mhaTbl.getMhaName());
        MachineTbl machineTbl = dalUtils.getMachineTblDao().queryAll().stream().filter(m -> (m.getDeleted().equals(BooleanEnum.FALSE.getCode()) && m.getMhaId().equals(mhaTbl.getId()) && m.getMaster().equals(BooleanEnum.TRUE.getCode()))).findFirst().get();
        return new MySqlEndpoint(machineTbl.getIp(), machineTbl.getPort(), mhaGroupTbl.getMonitorUser(), mhaGroupTbl.getMonitorPassword(), BooleanEnum.TRUE.isValue());
    }

    public Integer findAvailableApplierPort(String ip) throws SQLException {
        ResourceTbl resourceTbl = dalUtils.getResourceTblDao().queryAll().stream().filter(predicate -> (predicate.getDeleted().equals(BooleanEnum.FALSE.getCode()) && predicate.getIp().equalsIgnoreCase(ip))).findFirst().get();
        List<ReplicatorTbl> replicatorTbls = dalUtils.getReplicatorTblDao().queryAll().stream().filter(r -> r.getDeleted().equals(BooleanEnum.FALSE.getCode()) && r.getResourceId().equals(resourceTbl.getId())).collect(Collectors.toList());
        if(replicatorTbls.size() == 0) {
            return DEFAULT_REPLICATOR_APPLIER_PORT;
        }
        replicatorTbls.sort((r1, r2) -> r2.getApplierPort() - r1.getApplierPort());
        return replicatorTbls.get(0).getApplierPort() + 1;
    }

    public List<String> getReplicatorIps(Long replicatorGroupId) throws SQLException {
        List<Long> resourceIds = Lists.newArrayList();
        dalUtils.getReplicatorTblDao().queryAll().forEach(replicatorTbl -> {
            if(replicatorTbl.getDeleted().equals(BooleanEnum.FALSE.getCode()) && replicatorTbl.getRelicatorGroupId().equals(replicatorGroupId)) {
                resourceIds.add(replicatorTbl.getResourceId());
            }
        });
        return getResourceIps(resourceIds);
    }

    public InstanceInfo getReplicator(MhaTbl mhaTbl) throws SQLException {
        ReplicatorGroupTbl replicatorGroupTbl = dalUtils.getReplicatorGroupTblDao().queryAll().stream().filter(p -> p.getMhaId().equals(mhaTbl.getId()) && BooleanEnum.FALSE.getCode().equals(p.getDeleted())).findFirst().get();
        List<ReplicatorTbl> replicatorTblCandidates = dalUtils.getReplicatorTblDao().queryAll().stream().filter(p -> p.getRelicatorGroupId().equals(replicatorGroupTbl.getId()) && BooleanEnum.FALSE.getCode().equals(p.getDeleted())).collect(Collectors.toList());
        int size = replicatorTblCandidates.size();
        if(size == 0) {
            return null;
        }
        ReplicatorTbl replicatorTbl;
        if(size == 1) {
            replicatorTbl =  replicatorTblCandidates.get(0);
        } else {
            replicatorTbl = replicatorTblCandidates.stream().filter(p -> p.getMaster().equals(BooleanEnum.FALSE.getCode())).findFirst().get();
        }
        ResourceTbl resourceTbl = dalUtils.getResourceTblDao().queryByPk(replicatorTbl.getResourceId());
        InstanceInfo info = new InstanceInfo();
        info.setIp(resourceTbl.getIp());
        info.setPort(replicatorTbl.getApplierPort());
        info.setMhaName(mhaTbl.getMhaName());
        info.setCluster(getCluster(mhaTbl.getMhaName()));
        info.setIdc(dalUtils.getDcTblDao().queryByPk(mhaTbl.getDcId()).getDcName());
        return info;
    }

    public List<String> getApplierIps(Long applierGroupId) throws SQLException {
        List<Long> resourceIds = Lists.newArrayList();
        dalUtils.getApplierTblDao().queryAll().forEach(applierTbl -> {
            if(applierTbl.getDeleted().equals(BooleanEnum.FALSE.getCode()) && applierTbl.getApplierGroupId().equals(applierGroupId)) {
                resourceIds.add(applierTbl.getResourceId());
            }
        });
        return getResourceIps(resourceIds);
    }

    public List<String> getResourceIps(List<Long> resourceIds) throws SQLException {
        List<String> ips = Lists.newArrayList();
        dalUtils.getResourceTblDao().queryAll().forEach(resourceTbl -> {
            if(resourceTbl.getDeleted().equals(BooleanEnum.FALSE.getCode()) && resourceIds.contains(resourceTbl.getId())) {
                ips.add(resourceTbl.getIp());
            }
        });
        return ips;
    }

    public String getCluster(String mha) throws SQLException {
        Long mhaId = dalUtils.getId(TableEnum.MHA_TABLE, mha);
        ClusterMhaMapTbl clusterMhaMapTbl = dalUtils.getClusterMhaMapTblDao().queryAll().stream().filter(p -> (p.getDeleted().equals(BooleanEnum.FALSE.getCode()) && p.getMhaId().equals(mhaId))).findFirst().orElse(null);
        if(null == clusterMhaMapTbl) {
            return null;
        }
        ClusterTbl clusterTbl = dalUtils.getClusterTblDao().queryByPk(clusterMhaMapTbl.getClusterId());
        return null == clusterTbl ? null : clusterTbl.getClusterName();
    }

    /**
     *  key: dbname, value: strategy id
     * @return
     */
    public Map<String, Integer> getUcsStrategyIdMap(String clusterName, String mhaName) {
        Set<String> realDalClusters = getRealDalClusters(clusterName, Collections.singletonList(mhaName));
        Map<String, Integer> ucsStrategyIdMap = Maps.newHashMap();
        if(SOURCE_QCONFIG.equalsIgnoreCase(monitorTableSourceProvider.getUcsStrategyIdMapSource())) {
            for(String dalClusterName : realDalClusters) {
                Map<String, Integer> ucsStrategyIds = defaultConsoleConfig.getUcsStrategyIdMap(dalClusterName);
                ucsStrategyIdMap.putAll(ucsStrategyIds);
            }
        }
        return ucsStrategyIdMap;
    }

    public Map<String, String> getUidMap(String clusterName, String mhaName) {
        Set<String> realDalClusters = getRealDalClusters(clusterName, Collections.singletonList(mhaName));
        Map<String, String> uidMap = Maps.newHashMap();
        if(SOURCE_QCONFIG.equalsIgnoreCase(monitorTableSourceProvider.getUidMapSource())) {
            Map<String, String> rawMap = Maps.newHashMap();
            for(String dalClusterName : realDalClusters) {
                Map<String, String> uidNameMap = defaultConsoleConfig.getUidMap(dalClusterName);
                rawMap.putAll(uidNameMap);
            }
            Endpoint endpoint = getMasterMachine(mhaName);
            if(null != endpoint) {
                List<MySqlUtils.TableSchemaName> tables = MySqlUtils.getDefaultTables(endpoint);

                for(MySqlUtils.TableSchemaName table : tables) {
                    String uid = rawMap.get(table.toString());
                    String defaultUid = rawMap.get(table.getSchema() + ".*");
                    if(null != uid) {
                        uidMap.put(table.toString(), uid);
                    } else if (null != defaultUid) {
                        uidMap.put(table.toString(), defaultUid);
                    }
                }
            }
        }
        return uidMap;
    }

    public Endpoint getMasterMachine(String mha) {
        try {
            Long mhaId = dalUtils.getId(TableEnum.MHA_TABLE, mha);
            MhaGroupTbl mhaGroupTbl = getMhaGroupForMha(mha);
            MachineTbl master = dalUtils.getMachineTblDao().queryAll().stream().filter(p -> p.getDeleted().equals(BooleanEnum.FALSE.getCode()) && p.getMaster().equals(BooleanEnum.TRUE.getCode()) && mhaId.equals(p.getMhaId())).findFirst().get();
            return new MySqlEndpoint(master.getIp(), master.getPort(), mhaGroupTbl.getMonitorUser(), mhaGroupTbl.getMonitorPassword(), BooleanEnum.TRUE.isValue());
        } catch(Exception e) {
            logger.error("cannot get master machine for {}", mha, e);
        }
        return null;
    }

    public Set<String> getRealDalClusters(String dalClusterNameInDrc, List<String> mhas) {
        Set<String> realDalClusterSet = Sets.newHashSet(dalClusterNameInDrc);

        if(null == mhas || mhas.size() == 0) {
            logger.info("mhas size 0 for {} in meta db", dalClusterNameInDrc);
            return realDalClusterSet;
        }

        Map<String, String> mhaDalClusterInfo = dalService.getInstanceGroupsInfo(mhas, env);
        logger.info("mhaDalClusterInfo : {}", mhaDalClusterInfo);
        int i = 0;
        for(Map.Entry<String, String> entry : mhaDalClusterInfo.entrySet()) {
            if(i == 0) {
                String[] split = entry.getValue().split(",");
                realDalClusterSet = new HashSet<>(Arrays.asList(split));
                i++;
                continue;
            }
            String[] split = entry.getValue().split(",");
            Set<String> temp = new HashSet<>(Arrays.asList(split));
            realDalClusterSet = Sets.intersection(realDalClusterSet, temp);
        }
        return realDalClusterSet;
    }

    /**
     * @param dalClusterNameInDrc: the clusterName recorded in drc metadb
     * @param mhas: mhas.size() must a double integer, i.e. the mhas are under DRC replication
     * @return key: dalcluster name, value: list of mhas in that dalcluster name
     */
    public Map<String, List<String>> getRealDalClusterMap(String dalClusterNameInDrc, List<String> mhas) {

        if(null == mhas || mhas.size() == 0 || mhas.size() % 2 == 1) {
            logger.info("[realDalClusterMap] mhas({}) does not fulfill prerequisite for {}", mhas, dalClusterNameInDrc);
            return Maps.newHashMap();
        }

        Map<String, String> mhaDalClusterInfo = dalService.getInstanceGroupsInfo(mhas, env);  // key:mhaName, value:multi real dalClusterName
        logger.info("[realDalClusterMap] mhaDalClusterInfo for {}-{} : {}", dalClusterNameInDrc, mhas, mhaDalClusterInfo);

        Map<String, List<String>> roughRealDalClusterMap = buildDalClusterMap(mhaDalClusterInfo); // key:real dalClusterName, value:mhaName list
        return generateRealDalClusterMap(roughRealDalClusterMap);
    }

    protected Map<String, List<String>> buildDalClusterMap(Map<String, String> mhaDalClusterInfo) {
        Map<String, List<String>> dalClusterMap = Maps.newHashMap();
        for(Map.Entry<String, String> entry : mhaDalClusterInfo.entrySet()) {
            String mha = entry.getKey();
            String[] dalClusterNames = entry.getValue().split(",");
            for(String dalClusterName : dalClusterNames) {
                List<String> mhaList = dalClusterMap.get(dalClusterName);
                if(null == mhaList) {
                    mhaList = Lists.newArrayList();
                }
                mhaList.add(mha);
                dalClusterMap.put(dalClusterName, mhaList);
            }
        }
        return dalClusterMap;
    }

    protected Map<String, List<String>> generateRealDalClusterMap(Map<String, List<String>> roughRealDalClusterMap) {
        Map<String, List<String>> realDalClusterMap = Maps.newHashMap();
        for(Map.Entry<String, List<String>> entry : roughRealDalClusterMap.entrySet()) {
            String dalCluster = entry.getKey();
            List<String> roughMhaList = entry.getValue();
            List<String> mhaPairList = generateMhaPairList(roughMhaList);
            if(mhaPairList.size() > 0) {
                realDalClusterMap.put(dalCluster, mhaPairList);
            }
        }
        return realDalClusterMap;
    }

    private List<String> generateMhaPairList(List<String> roughMhaList) {
        Set<String> mhas = Sets.newHashSet();
        for(String mha : roughMhaList) {
            try {
                List<Long> mhaGroupIds = getMhaGroupIds(mha);
                Set<Long> mhaIdSets = dalUtils.getGroupMappingTblDao().queryAll().stream().filter(p -> BooleanEnum.FALSE.getCode().equals(p.getDeleted()) && mhaGroupIds.contains(p.getMhaGroupId())).map(GroupMappingTbl::getMhaId).collect(Collectors.toSet());

                for (Long mhaId : mhaIdSets) {
                    MhaTbl targetMhaTbl = dalUtils.getMhaTblDao()
                            .queryAll()
                            .stream()
                            .filter(p -> BooleanEnum.FALSE.getCode().equals(p.getDeleted()) && mhaId.equals(p.getId()) && !mha.equalsIgnoreCase(p.getMhaName()))
                            .findFirst().orElse(null);
                    if(null != targetMhaTbl && roughMhaList.contains(targetMhaTbl.getMhaName())) {
                        mhas.add(mha);
                        mhas.add(targetMhaTbl.getMhaName());
                    }
                }
            } catch (SQLException e) {
                logger.error("Fail generate for {}", mha, e);
            }
        }
        return Lists.newArrayList(mhas);
    }

    /**
     * key: local Mha
     * value: master db's uuid set which are not in local dc, i.e. all potential uuids which will be copied into local mha
     */
    public Map<String, Set<String>> getUuidMap() {
        Map<String, Set<String>> uuidMap = Maps.newHashMap();

        Drc drc = dbClusterSourceProvider.getDrc();
        String localDcName = dbClusterSourceProvider.getLocalDcName();
        Map<String, Dc> dcs = drc.getDcs();

        List<DbCluster> targetDbClusters = dcs.values()
                .stream()
                .filter(p -> !localDcName.equalsIgnoreCase(p.getId()))
                .map(dc -> dc.getDbClusters().values())
                .flatMap(Collection::stream)
                .collect(Collectors.toList());

        for(DbCluster dbCluster :  targetDbClusters) {
            for(Applier applier : dbCluster.getAppliers()) {
                if(localDcName.equalsIgnoreCase(applier.getTargetIdc())) {
                    String localMhaName = applier.getTargetMhaName();
                    Dbs dbs = dbCluster.getDbs();
                    List<Db> dbList = dbs.getDbs();
                    for(Db db : dbList) {
                        if(db.isMaster()) {
                            // ali dc uuid is not only
                            String uuidString = db.getUuid();
                            String[] uuids = uuidString.split(",");
                            Set<String> uuidSet = uuidMap.getOrDefault(localMhaName, Sets.newHashSet());
                            for (String uuid : uuids) {
                                uuidSet.add(uuid);
                                logger.info("mhaName is {},opposite end mha master uuid contain {}",localMhaName,uuid);
                            }
                            uuidMap.put(localMhaName, uuidSet);
                            break;
                        }
                    }
                }
            }
        }
        return uuidMap;
    }

    public List<RouteDto> getRoutes(String routeOrgName, String srcDcName, String dstDcName, String tag) {
        List<RouteDto> routes = Lists.newArrayList();
        try {
            Long buId = null, srcDcId = null, dstDcId = null;
            if(null != routeOrgName) {
                buId = dalUtils.getId(TableEnum.BU_TABLE, routeOrgName);
            }
            if(null != srcDcName) {
                srcDcId = dalUtils.getId(TableEnum.DC_TABLE, srcDcName);
            }
            if(null != dstDcName) {
                dstDcId = dalUtils.getId(TableEnum.DC_TABLE, dstDcName);
            }
            final Long finalBuId = buId, finalSrcDcId = srcDcId, finalDstDcId = dstDcId;

            List<RouteTbl> routeTbls = dalUtils.getRouteTblDao().queryAll().stream()
                    .filter(p -> p.getDeleted().equals(BooleanEnum.FALSE.getCode()) &&
                            (null == routeOrgName || p.getRouteOrgId().equals(finalBuId)) &&
                            (null == srcDcName || p.getSrcDcId().equals(finalSrcDcId)) &&
                            (null == dstDcName || p.getDstDcId().equals(finalDstDcId)) &&
                            (null == tag || p.getTag().equalsIgnoreCase(tag)))
                    .collect(Collectors.toList());

            for(RouteTbl routeTbl : routeTbls) {
                routes.add(getRouteDto(routeTbl));
            }
        } catch (SQLException e) {
            logger.error("[metaInfo] fail get Proxy routes, ", e);
        }
        return routes;
    }

    private RouteDto getRouteDto(RouteTbl routeTbl) throws SQLException {
        RouteDto routeDto = new RouteDto();
        routeDto.setId(routeTbl.getId());
        routeDto.setRouteOrgName(dalUtils.getBuTblDao().queryByPk(routeTbl.getRouteOrgId()).getBuName());
        routeDto.setSrcDcName(dalUtils.getDcTblDao().queryByPk(routeTbl.getSrcDcId()).getDcName());
        routeDto.setDstDcName(dalUtils.getDcTblDao().queryByPk(routeTbl.getDstDcId()).getDcName());

        List<ProxyTbl> proxyTbls = dalUtils.getProxyTblDao().queryAll().stream().filter(p -> p.getDeleted().equals(BooleanEnum.FALSE.getCode())).collect(Collectors.toList());

        String srcProxyIds = routeTbl.getSrcProxyIds();
        String optionalProxyIds = routeTbl.getOptionalProxyIds();
        String dstProxyIds = routeTbl.getDstProxyIds();
        List<String> srcProxyUris = getProxyUris(srcProxyIds, proxyTbls);
        List<String> relayProxyUris = getProxyUris(optionalProxyIds, proxyTbls);
        List<String> dstProxyUris = getProxyUris(dstProxyIds, proxyTbls);

        routeDto.setSrcProxyUris(srcProxyUris);
        routeDto.setRelayProxyUris(relayProxyUris);
        routeDto.setDstProxyUris(dstProxyUris);
        routeDto.setTag(routeTbl.getTag());
        return routeDto;
    }

    private List<String> getProxyUris(String proxyIds, List<ProxyTbl> proxyTbls) {
        List<String> proxyIps = Lists.newArrayList();
        if(StringUtils.isNotBlank(proxyIds)) {
            String[] proxyIdArr = proxyIds.split(",");
            for(String idStr : proxyIdArr) {
                Long proxyId = Long.parseLong(idStr);
                proxyTbls.stream().filter(p -> p.getId().equals(proxyId)).findFirst().ifPresent(proxyTbl -> proxyIps.add(proxyTbl.getUri()));
            }
        }
        return proxyIps;
    }

    public List<MhaTbl> getMhas(String dcName) throws SQLException {
        Long dcId = getDcId(dcName);
        return dalUtils.getMhaTblDao().queryMhas(dcId);
    }

    private Long getDcId(String dcName) throws SQLException {
        if (StringUtils.isBlank(dcName)) {
            return null;
        }
        DcTbl dcTbl = new DcTbl();
        dcTbl.setDcName(dcName);
        List<DcTbl> dcTbls =  dalUtils.getDcTblDao().queryBy(dcTbl);
        if (dcTbls.isEmpty()) {
            throw new IllegalStateException("dc name does not exist in meta db, dc name is: " + dcName);
        }
        return dcTbls.get(0).getId();
    }
}
