package com.ctrip.framework.drc.console.service.v2.impl;

import com.ctrip.framework.drc.console.aop.forward.PossibleRemote;
import com.ctrip.framework.drc.console.aop.forward.response.MhaAccountsApiRes;
import com.ctrip.framework.drc.console.aop.forward.response.MhaDbReplicationListResponse;
import com.ctrip.framework.drc.console.aop.forward.response.MhaV2ListResponse;
import com.ctrip.framework.drc.console.dao.*;
import com.ctrip.framework.drc.console.dao.entity.*;
import com.ctrip.framework.drc.console.dao.entity.v2.MhaTblV2;
import com.ctrip.framework.drc.console.dao.v2.MhaTblV2Dao;
import com.ctrip.framework.drc.console.dto.v3.MhaDbReplicationDto;
import com.ctrip.framework.drc.console.enums.BooleanEnum;
import com.ctrip.framework.drc.console.enums.ForwardTypeEnum;
import com.ctrip.framework.drc.console.enums.HttpRequestEnum;
import com.ctrip.framework.drc.console.param.MhaReplicatorEntity;
import com.ctrip.framework.drc.console.param.mysql.DdlHistoryEntity;
import com.ctrip.framework.drc.console.param.v2.security.MhaAccounts;
import com.ctrip.framework.drc.console.service.v2.CentralService;
import com.ctrip.framework.drc.console.service.v2.MachineService;
import com.ctrip.framework.drc.console.service.v2.MhaDbReplicationService;
import com.ctrip.framework.drc.console.service.v2.security.AccountService;
import com.ctrip.framework.drc.console.utils.ConsoleExceptionUtils;
import com.ctrip.framework.drc.core.monitor.reporter.DefaultEventMonitorHolder;
import com.ctrip.platform.dal.dao.DalHints;
import com.ctrip.platform.dal.dao.KeyHolder;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @ClassName CentralServiceImpl
 * @Author haodongPan
 * @Date 2023/7/28 17:47
 * @Version: $
 * @Description: forward Service through PossibleRemote aop
 */
@Service
public class CentralServiceImpl implements CentralService {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired
    private MhaTblV2Dao mhaTblV2Dao;
    @Autowired
    private DcTblDao dcTblDao;
    @Autowired
    private DdlHistoryTblDao ddlHistoryTblDao;
    @Autowired
    private MhaDbReplicationService mhaDbReplicationService;
    @Autowired
    private MachineService machineService;
    @Autowired
    private AccountService accountService;
    @Autowired
    private ReplicatorGroupTblDao replicatorGroupTblDao;
    @Autowired
    private ReplicatorTblDao replicatorTblDao;
    @Autowired
    private ResourceTblDao resourceTblDao;


    @Override
    @PossibleRemote(path = "/api/drc/v2/centralService/mhaTblV2s", forwardType = ForwardTypeEnum.TO_META_DB, responseType = MhaV2ListResponse.class)
    public List<MhaTblV2> getMhaTblV2s(String dcName) throws SQLException {
        Long dcId = getDcId(dcName);
        MhaTblV2 sample = new MhaTblV2();
        sample.setDcId(dcId);
        return mhaTblV2Dao.queryBy(sample);
    }

    @Override
    @PossibleRemote(path = "/api/drc/v2/centralService/ddlHistory", httpType = HttpRequestEnum.POST, requestClass = DdlHistoryEntity.class, forwardType = ForwardTypeEnum.TO_META_DB)
    public Integer insertDdlHistory(DdlHistoryEntity requestBody) throws SQLException {
        logger.info("insertDdlHistory requestBody: {}", requestBody);
        MhaTblV2 mhaTblV2 = mhaTblV2Dao.queryByMhaName(requestBody.getMha());
        if (mhaTblV2 == null) {
            throw ConsoleExceptionUtils.message(String.format("mha: %s not exist", requestBody.getMha()));
        }
        DdlHistoryTbl pojo = DdlHistoryTbl.createDdlHistoryPojo
                (mhaTblV2.getId(), requestBody.getDdl(), requestBody.getQueryType(), requestBody.getSchemaName(), requestBody.getTableName());
        KeyHolder keyHolder = new KeyHolder();
        return ddlHistoryTblDao.insert(new DalHints(), keyHolder, pojo);
    }

    @Override
    @PossibleRemote(path = "/api/drc/v2/centralService/mhaDbReplicationDtos", forwardType = ForwardTypeEnum.TO_META_DB, responseType = MhaDbReplicationListResponse.class)
    public List<MhaDbReplicationDto> getMhaDbReplications(String dcName) {
        List<MhaDbReplicationDto> replicationDtos = mhaDbReplicationService.queryByDcName(dcName, null);
        return replicationDtos.stream().filter(e -> Boolean.TRUE.equals(e.getDrcStatus())).collect(Collectors.toList());
    }

    @Override
    @PossibleRemote(path = "/api/drc/v2/centralService/uuid", forwardType = ForwardTypeEnum.TO_META_DB)
    public String getUuidInMetaDb(String mhaName, String ip, Integer port) throws SQLException {
        logger.info("getUuidInMetaDb mhaName: {}, ip: {}, port: {}", mhaName, ip, port);
        return machineService.getUuid(ip, port);
    }

    @Override
    @PossibleRemote(path = "/api/drc/v2/centralService/uuid/correct", forwardType = ForwardTypeEnum.TO_META_DB,
            httpType = HttpRequestEnum.POST, requestClass = MachineTbl.class)
    public Integer correctMachineUuid(MachineTbl requestBody) throws SQLException {
        logger.info("correctMachineUuid requestBody: {}", requestBody);
        return machineService.correctUuid(requestBody.getIp(), requestBody.getPort(), requestBody.getUuid());
    }

    @Override
    @PossibleRemote(path = "/api/drc/v2/centralService/mhaAccounts", forwardType = ForwardTypeEnum.TO_META_DB, responseType = MhaAccountsApiRes.class)
    public MhaAccounts getMhaAccounts(String mhaName) throws SQLException {
        return accountService.getMhaAccountsOrDefault(mhaName);
    }

    @Override
    @PossibleRemote(path = "/api/drc/v2/centralService/replicator", httpType = HttpRequestEnum.POST, forwardType = ForwardTypeEnum.TO_META_DB)
    public Boolean updateMasterReplicatorIfChange(MhaReplicatorEntity requestBody) throws SQLException {
        String mhaName = requestBody.getMhaName();
        String newIp = requestBody.getReplicatorIp();
        Map<String, ReplicatorTbl> replicators = getIpReplicatorMap(mhaName);
        List<ReplicatorTbl> rTblsToBeUpdated = Lists.newArrayList();
        for (Map.Entry<String, ReplicatorTbl> entry : replicators.entrySet()) {
            String ip = entry.getKey();
            ReplicatorTbl replicatorTbl = entry.getValue();
            if (newIp.equalsIgnoreCase(ip) && BooleanEnum.FALSE.getCode().equals(replicatorTbl.getMaster())) {
                replicatorTbl.setMaster(BooleanEnum.TRUE.getCode());
                rTblsToBeUpdated.add(replicatorTbl);
            }
            if (!newIp.equalsIgnoreCase(ip) && BooleanEnum.TRUE.getCode().equals(replicatorTbl.getMaster())) {
                replicatorTbl.setMaster(BooleanEnum.FALSE.getCode());
                rTblsToBeUpdated.add(replicatorTbl);
            }
        }
        if (rTblsToBeUpdated.size() > 0) {
            try {
                int[] ints = replicatorTblDao.batchUpdate(rTblsToBeUpdated);
                String updateRes = StringUtils.join(ints, ",");
                logger.info("update replicator master, mhaName: {}, newIp: {}", mhaName, newIp);
                DefaultEventMonitorHolder.getInstance()
                        .logEvent("DRC.replicator.master", String.format("%s:%s", mhaName, newIp));
                return true;
            } catch (SQLException e) {
                logger.error("Fail update master replicator({}), ", newIp, e);
            }
        } else {
            logger.debug("replicator master ip not change,mha:{},ip:{}", mhaName, newIp);
        }
        return false;
    }

    private Map<String, ReplicatorTbl> getIpReplicatorMap(String mha) throws SQLException {
        Map<String, ReplicatorTbl> ip2Replicator = Maps.newHashMap();
        MhaTblV2 mhaTblV2 = mhaTblV2Dao.queryByMhaName(mha);
        ReplicatorGroupTbl replicatorGroupTbl = replicatorGroupTblDao.queryByMhaId(mhaTblV2.getId());
        if (null != replicatorGroupTbl) {
            try {
                List<ReplicatorTbl> replicatorTbls = replicatorTblDao.
                        queryByRGroupIds(Lists.newArrayList(replicatorGroupTbl.getId()), BooleanEnum.FALSE.getCode());
                for (ReplicatorTbl replicatorTbl : replicatorTbls) {
                    Long resourceId = replicatorTbl.getResourceId();
                    String ip = resourceTblDao.queryByPk(resourceId).getIp();
                    ip2Replicator.put(ip, replicatorTbl);
                }
            } catch (SQLException e) {
                logger.error("Fail getIpReplicatorMap for {}, ", mha, e);
            }
        }
        return ip2Replicator;
    }


    private Long getDcId(String dcName) throws SQLException {
        if (StringUtils.isBlank(dcName)) {
            return null;
        }
        DcTbl dcTbl = new DcTbl();
        dcTbl.setDcName(dcName);
        List<DcTbl> dcTbls = dcTblDao.queryBy(dcTbl);
        if (dcTbls.isEmpty()) {
            throw new IllegalStateException("dc name does not exist in meta db, dc name is: " + dcName);
        }
        return dcTbls.get(0).getId();
    }
}
