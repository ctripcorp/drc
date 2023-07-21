package com.ctrip.framework.drc.console.service.impl;

import com.ctrip.framework.drc.console.config.DefaultConsoleConfig;
import com.ctrip.framework.drc.console.config.DomainConfig;
import com.ctrip.framework.drc.console.dao.*;
import com.ctrip.framework.drc.console.dao.entity.*;
import com.ctrip.framework.drc.console.dto.DalClusterDto;
import com.ctrip.framework.drc.console.dto.DalClusterShard;
import com.ctrip.framework.drc.console.dto.DalMhaDto;
import com.ctrip.framework.drc.console.dto.MhaDto;
import com.ctrip.framework.drc.console.enums.BooleanEnum;
import com.ctrip.framework.drc.console.monitor.AbstractMonitor;
import com.ctrip.framework.drc.console.monitor.delay.config.MonitorTableSourceProvider;
import com.ctrip.framework.drc.console.service.MhaService;
import com.ctrip.framework.drc.console.service.impl.api.ApiContainer;
import com.ctrip.framework.drc.console.service.v2.DrcDoubleWriteService;
import com.ctrip.framework.drc.console.utils.DalUtils;
import com.ctrip.framework.drc.core.driver.command.netty.endpoint.DefaultEndPoint;
import com.ctrip.framework.drc.core.http.ApiResult;
import com.ctrip.framework.drc.core.service.dal.DbClusterApiService;
import com.ctrip.framework.drc.core.service.ops.OPSApiService;
import com.ctrip.framework.drc.core.service.utils.JsonUtils;
import com.ctrip.platform.dal.dao.annotation.DalTransactional;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.Lists;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.ctrip.framework.drc.console.enums.BooleanEnum.FALSE;
import static com.ctrip.framework.drc.console.monitor.delay.config.MonitorTableSourceProvider.SWITCH_STATUS_ON;

/**
 * @author maojiawei
 * @version 1.0
 * date: 2020-08-04
 */
@Service
public class MhaServiceImpl extends AbstractMonitor implements MhaService {

    @Autowired private DefaultConsoleConfig defaultConsoleConfig;

    @Autowired private MonitorTableSourceProvider monitorTableSourceProvider;

    @Autowired private DomainConfig domainConfig;

    @Autowired private MhaTblDao mhaTblDao;

    @Autowired private ClusterMhaMapTblDao clusterMhaMapTblDao;

    @Autowired private ClusterTblDao clusterTblDao;

    @Autowired private BuTblDao buTblDao;

    @Autowired private DcTblDao dcTblDao;

    @Autowired
    private DrcDoubleWriteService drcDoubleWriteService;

    private DalUtils dalUtils = DalUtils.getInstance();

    private OPSApiService opsApiServiceImpl = ApiContainer.getOPSApiServiceImpl();
    private DbClusterApiService dbClusterApiServiceImpl = ApiContainer.getDbClusterApiServiceImpl();

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private List<Map<String, String>> cachedAllClusterNames;


    @Override
    public void scheduledTask() {
        String cachedAllClusterNamesSwitch = monitorTableSourceProvider.getCacheAllClusterNamesSwitch();
        if (SWITCH_STATUS_ON.equalsIgnoreCase(cachedAllClusterNamesSwitch)) {
            cachedAllClusterNames = getAllClusterNames();
        }
    }

    @Override
    public List<Map<String, String>> getCachedAllClusterNames() {
        if(null == cachedAllClusterNames) {
            cachedAllClusterNames = getAllClusterNames();
        }
        return cachedAllClusterNames;
    }

    @Override
    public List<Map<String, String>> getCachedAllClusterNames(String keyWord) {
        List<Map<String, String>> allClusterNames = getCachedAllClusterNames();
        if(StringUtils.isEmpty(keyWord)) {
            return allClusterNames;
        }
        List<Map<String, String>> res = new ArrayList<>();
        for(Map<String, String> pair : allClusterNames) {
            if(pair.get("cluster").toLowerCase().contains(keyWord.toLowerCase())) {
                Map<String, String> clusterPair = new HashMap<>();
                clusterPair.put("cluster", pair.get("cluster"));
                clusterPair.put("zoneId", pair.get("zoneId"));
                res.add(clusterPair);
            }
        }
        return res;
    }

    /**
     * get all the cluster names and their Chinese zoneIds
     *
     */
    @Override
    public List<Map<String,String>> getAllClusterNames() {
        List<Map<String, String>> res = new ArrayList<>();
        try {
            JsonNode root = getAllClusterInfo();
            for(JsonNode cluster : root.get("data")){
                Map<String,String> clusterPair = new HashMap<>();
                clusterPair.put("cluster", cluster.get("cluster").asText());
                JsonNode machines = cluster.get("machines");
                if(machines.isArray()){
                    for(JsonNode machine:machines){
                        if("master".equalsIgnoreCase(machine.get("role").asText())) {
                            clusterPair.put("zoneId", machine.get("machine_located").asText());
                            break;
                        }
                    }
                }
                res.add(clusterPair);
            }
        } catch (JsonProcessingException e) {
            logger.error("JsonProcessingException", e);
        }
        return res;
    }

    protected JsonNode getAllClusterInfo() throws JsonProcessingException {
        String getAllClusterUrl = domainConfig.getGetAllClusterUrl();
        String opsAccessToken = domainConfig.getOpsAccessToken();
        return opsApiServiceImpl.getAllClusterInfo(getAllClusterUrl,opsAccessToken);
    }

    @Override
    public Endpoint getMasterMachineInstance(String mha) {
        try {
            String getAllClusterUrl = domainConfig.getGetAllClusterUrl();
            String opsAccessToken = domainConfig.getOpsAccessToken();
            JsonNode root = opsApiServiceImpl.getAllClusterInfo(getAllClusterUrl,opsAccessToken);
            for(JsonNode cluster : root.get("data")){
                if(cluster.get("cluster").asText().equalsIgnoreCase(mha)) {
                    JsonNode machines = cluster.get("machines");
                    if(machines.isArray()){
                        for(JsonNode machine : machines){
                            if(machine.get("role").asText().equals("master")) {
                                return new DefaultEndPoint(machine.get("ip_business").asText(), machine.get("instance_port").asInt());
                            }
                        }
                    }
                }
            }
        } catch (JsonProcessingException e) {
            logger.error("JsonProcessingException", e);
        }
        return null;
    }

    /**
     * get all the dbs of one specified cluster
     *
     */
    @Override
    public List<String> getAllDbs(String clusterName, String env) {
        List<String> res = new ArrayList();
        JsonNode root = null;
        try {
            String mysqlDbClusterUrl = domainConfig.getMysqlDbClusterUrl();
            String opsAccessToken = domainConfig.getOpsAccessToken();
            root = opsApiServiceImpl.getAllDbs(mysqlDbClusterUrl,opsAccessToken,clusterName, env);
        } catch (JsonProcessingException e) {
            logger.error("JsonProcessingException", e);
        }
        if (root == null) return null;
        String dbName;
        if(root.hasNonNull("data") && root.get("data").isArray()){
            for(JsonNode db: root.get("data")){
                dbName = db.get("db_name").asText();
                res.add(dbName);
            }
        }
        return res;
    }


    /**
     * get all the db names and dal names of one specified cluster
     *
     */
    @Override
    public List<Map<String, Object>> getAllDbsAndDals(String clusterName, String env, String zoneId) {
        List<Map<String, Object>> res = new ArrayList();
        String dalServicePrefix = domainConfig.getDalServicePrefix();
        Map<String, Object> dalClusterRetMap = dbClusterApiServiceImpl.getDalClusterFromDalService(dalServicePrefix,clusterName);
        List<DalMhaDto> dalMhaDtoList = JsonUtils.fromJsonToList(JsonUtils.toJson(dalClusterRetMap.get("result")), DalMhaDto.class);
        List<DalClusterDto> dalClusterDtoList = dalMhaDtoList.get(0).getClusters();
        for(DalClusterDto dalCluster : dalClusterDtoList) {
            String baseDbName = dalCluster.getBaseDbName();
            List<DalClusterShard> shards = dalCluster.getShards();

            for(DalClusterShard shard : shards) {
                Map<String, Object> map = new HashMap();
                map.put("basedbname", baseDbName);
                map.put("dbname", shard.getDbName());
                map.put("dalname", baseDbName + "_dalcluster");
                if(shard.getShardIndex() == 0) {
                    map.put("isshard", false);
                    map.put("shardstartno", 0);
                } else {
                    map.put("isshard", true);
                    map.put("shardstartno", getShardStart(shard.getShardIndex(), shard.getDbName()));
                }
                if(!zoneId.equals("")){
                    map.put("status", isRegistered(shard.getZoneId(), zoneId));
                }
                res.add(map);
            }
        }
        return res;
    }


    @Override
    public List<Map<String, Object>> getAllDbsAndDals(String clusterName, String env){
        return getAllDbsAndDals(clusterName, env, "");
    }

    @Override
    public String getDcForMha(String mha) {
        List<Map<String, String>> allClusterNames = getAllClusterNames();
        // zoneId, cluster
        Map<String, String> cluster = allClusterNames.stream().filter(p -> mha.equalsIgnoreCase(p.get("cluster"))).findFirst().orElse(null);
        if(null == cluster) {
            return null;
        }
        String zoneId = cluster.get("zoneId");
        Map<String, String> dbaDcInfos = defaultConsoleConfig.getDbaDcInfos();
        if(dbaDcInfos.containsKey(zoneId)) {
            return dbaDcInfos.get(zoneId);
        }
        logger.info("Cannot find dc for zoneId: {}", zoneId);
        return zoneId;
    }

    @Override
    @DalTransactional(logicDbName = "fxdrcmetadb_w")
    public ApiResult recordMha(MhaDto mhaDto) {
        try {
            MhaTbl mhaTbl = mhaTblDao.queryByMhaName(mhaDto.getMhaName(), FALSE.getCode());
            if (null != mhaTbl) {
                return ApiResult.getFailInstance(null,"mha already exist!");
            }
            Long dcId = dalUtils.updateOrCreateDc(mhaDto.getDc());
            Long buId = dalUtils.updateOrCreateBu(mhaDto.getBuName());
            Long clusterId = dalUtils.updateOrCreateCluster(mhaDto.getDalClusterName(), mhaDto.getAppid(), buId);
            Long mhaId = dalUtils.recoverOrCreateMha(mhaDto.getMhaName(), dcId);
            dalUtils.updateOrCreateClusterMhaMap(clusterId, mhaId);

            if (defaultConsoleConfig.getDrcDoubleWriteSwitch().equals(DefaultConsoleConfig.SWITCH_ON)) {
                logger.info("drcDoubleWrite buildMhaForMq");
                drcDoubleWriteService.buildMhaForMq(mhaId);
            }
            return ApiResult.getSuccessInstance(null);

        } catch (Exception e) {
            logger.error("recordMha error,mhaDto:{}",mhaDto,e);
            return  ApiResult.getFailInstance(null,e.getMessage());
        }
    }

    @Override
    public MhaDto queryMhaInfo(Long mhaId) throws SQLException{
        MhaDto mhaDto = new MhaDto();
        MhaTbl mhaTbl = mhaTblDao.queryByPk(mhaId);
        mhaDto.setMhaName(mhaTbl.getMhaName());
        mhaDto.setMonitorSwitch(mhaTbl.getMonitorSwitch());

        List<ClusterMhaMapTbl> clusterMhaMapTbls = clusterMhaMapTblDao.queryByMhaIds(
                Lists.newArrayList(mhaId), BooleanEnum.FALSE.getCode());
        ClusterTbl clusterTbl = clusterTblDao.queryByPk(clusterMhaMapTbls.get(0).getClusterId());
        BuTbl buTbl = buTblDao.queryByPk(clusterTbl.getBuId());
        mhaDto.setBuName(buTbl.getBuName());
        return mhaDto;
    }

    @Override
    public String getDcNameForMha(String mha) throws SQLException {
        MhaTbl mhaTbl = mhaTblDao.queryByMhaName(mha, FALSE.getCode());
        DcTbl dcTbl = dcTblDao.queryByPk(mhaTbl.getDcId());
        return dcTbl.getDcName();
    }

    private int getShardStart(int shardIndex, String dbName){
        String regEx="shard[0-9]+db";
        Pattern p = Pattern.compile(regEx);
        Matcher m = p.matcher(dbName);
        return m.find() ? Integer.parseInt(m.group().replaceAll("shard", "").replaceAll("db","")) - shardIndex:-1;
    }

    private boolean isRegistered(String shardZoneId, String zoneId){
        if (shardZoneId.equals(zoneId)){
            return true;
        }
        return false;
    }
}
