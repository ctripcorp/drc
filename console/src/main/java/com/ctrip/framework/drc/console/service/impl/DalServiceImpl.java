package com.ctrip.framework.drc.console.service.impl;

import com.ctrip.framework.drc.console.config.DbClusterRetriever;
import com.ctrip.framework.drc.console.config.DomainConfig;
import com.ctrip.framework.drc.console.dto.MhaInstanceGroupDto;
import com.ctrip.framework.drc.core.service.dal.DalClusterTypeEnum;
import com.ctrip.framework.drc.console.pojo.Mha;
import com.ctrip.framework.drc.console.pojo.TableConfig;
import com.ctrip.framework.drc.console.service.DalService;
import com.ctrip.framework.drc.core.service.dal.DbClusterApiService;
import com.ctrip.framework.drc.console.service.impl.api.ApiContainer;
import com.ctrip.framework.drc.core.http.ApiResult;
import com.ctrip.framework.foundation.Env;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.*;

/**
 * @author shenhaibo
 * @version 1.0
 * date: 2020-04-13
 */

@Service
public class DalServiceImpl implements DalService {

    Logger logger = LoggerFactory.getLogger(getClass());

    private ObjectMapper objectMapper = new ObjectMapper();
    
    private DbClusterApiService dbClusterApiServiceImpl = ApiContainer.getDbClusterApiServiceImpl();
    
    public static final Integer DAL_SUCCESS_CODE = 200;

    @Autowired
    private DbClusterRetriever dbClusterRetriever;
    
    @Autowired
    private DomainConfig domainConfig;

    @Override
    public List<Mha> getMhasFromDal(String clusterName) {
        return dbClusterRetriever.getMhas(dbClusterRetriever.getMhasNode(clusterName));
    }

    @Override
    public JsonNode getDalClusterInfo(String dalClusterName, String env) {
        String dalServicePrefix = domainConfig.getDalServicePrefix();
        return dbClusterApiServiceImpl.getDalClusterInfo(dalServicePrefix,dalClusterName);
    }

    @Override
    public Map<String, String> getInstanceGroupsInfo(List<String> mhas, Env env) {
        String dalServicePrefix = domainConfig.getDalServicePrefix();
        JsonNode resultNode = dbClusterApiServiceImpl.getInstanceGroupsInfo(dalServicePrefix,mhas);
        Map<String, String> mhaDalClusterInfo = new HashMap<>();
        try {
            for(JsonNode instanceNode : resultNode) {
                String mhaName = instanceNode.get("mhaName").asText();
                List<String> dalClusterNames = Lists.newArrayList();
                JsonNode clusters = instanceNode.get("clusters");
                for(JsonNode cluster : clusters) {
                    dalClusterNames.add(cluster.get("clusterName").asText());
                }
                if(dalClusterNames.size() != 0) {
                    mhaDalClusterInfo.put(mhaName, StringUtils.join(dalClusterNames, ","));
                }
            }
        } catch (Exception e) {
            logger.error("Fail generate mhaDalClusterInfo, ", e);
        }

        return mhaDalClusterInfo;
    }

    public List<TableConfig> getTableConfigs(String dalClusterName) {
        return dbClusterRetriever.getIgnoreTableConfigs(dalClusterName);
    }

    public Map<String, Map<String, List<String>>> getDbNames(List<String> mhas, Env env) { // key:mhaName, value: {key:dalClusterName, value:databaseName list}
        String dalServicePrefix = domainConfig.getDalServicePrefix();
        JsonNode resultNode = dbClusterApiServiceImpl.getInstanceGroupsInfo(dalServicePrefix,mhas);
        Map<String, Map<String, List<String>>> mhaDbNames = new HashMap<>();
        try {
            for(JsonNode instanceNode : resultNode) {
                String mhaName = instanceNode.get("mhaName").asText();
                Map<String, List<String>> dalClusterDbs = new HashMap<>();

                JsonNode clusters = instanceNode.get("clusters");
                for(JsonNode cluster : clusters) {
                    String dalClusterName = cluster.get("clusterName").asText();
                    List<String> dbNames = Lists.newArrayList();
                    JsonNode shards = cluster.get("shards");
                    for (JsonNode shard : shards) {
                        dbNames.add(shard.get("dbName").asText());
                    }
                    if (!dbNames.isEmpty()) {
                        dalClusterDbs.put(dalClusterName, dbNames);
                    }
                }

                mhaDbNames.put(mhaName, dalClusterDbs);
            }
        } catch (Exception e) {
            logger.error("Fail generate getDbNames, ", e);
        }

        return mhaDbNames;
    }

    @Override
    public Map<String, MhaInstanceGroupDto> getMhaList(Env env) {
        String dalServicePrefix = domainConfig.getDalServicePrefix();
        JsonNode resultNode = dbClusterApiServiceImpl.getMhaList(dalServicePrefix);
        Map<String, MhaInstanceGroupDto> mhaInstancesMap = new HashMap<>();
        for(JsonNode mhaInstanceGroupNode : resultNode) {
            try {
                String mhaName = mhaInstanceGroupNode.get("mhaName").asText();
                MhaInstanceGroupDto mhaInstanceGroupDto = objectMapper.treeToValue(mhaInstanceGroupNode, MhaInstanceGroupDto.class);
                mhaInstancesMap.put(mhaName, mhaInstanceGroupDto);
            } catch (Throwable t) {
                logger.error("Fail parse node, ", t);
            }
        }
        return mhaInstancesMap;
    }

    @Override
    public String getDc(String mha, Env env) {
        logger.debug("[getDc] for {} in {}", mha, env.getName());
        Map<String, MhaInstanceGroupDto> mhaList = getMhaList(env);
        MhaInstanceGroupDto mhaInstanceGroupDto = mhaList.get(mha);
        logger.debug("[getDc] for {} in {}: {}", mha, env.getName(), mhaInstanceGroupDto);
        return null == mhaInstanceGroupDto ? null : mhaInstanceGroupDto.getZoneId();
    }

    @Override
    public List<DalClusterInfoWrapper> getDalClusterInfoWrappers(Set<String> dalClusterNames, String env) {
        List<DalClusterInfoWrapper> wrappers = Lists.newArrayList();
        dalClusterNames.forEach(dalClusterName -> {
            JsonNode dalClusterInfo = getDalClusterInfo(dalClusterName, env);
            DalClusterInfoWrapper wrapper = new DalClusterInfoWrapper();
            wrapper.setDalClusterName(dalClusterName);
            wrapper.setType(dalClusterInfo.get("type").asText());
            objectMapper.configure(DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY, true);
            Set<String> zoneIds = new HashSet<>();
            JsonNode shards = dalClusterInfo.get("shards");
            if(null != shards) {
                for(JsonNode shard : shards) {
                    JsonNode zones = shard.get("zones");
                    if(null != zones) {
                        for(JsonNode zone : zones) {
                            JsonNode zoneId = zone.get("zoneId");
                            if(null != zoneId) {
                                zoneIds.add(zoneId.asText());
                            }
                        }
                    }
                }
            }
            wrapper.setZoneIds(zoneIds);
            wrappers.add(wrapper);
        });
        return wrappers;
    }

    @Override
    public ApiResult switchDalClusterType(String dalClusterName, String env, DalClusterTypeEnum typeEnum, String zoneId) throws Exception {
        String dalServicePrefix = domainConfig.getDalServicePrefix();
        return dbClusterApiServiceImpl.switchDalClusterType(dalClusterName,dalServicePrefix,typeEnum,zoneId);
    }
    

    public static final class DalClusterInfoWrapper {
        private String dalClusterName;

        private String type;

        private Set<String> zoneIds;

        public String getDalClusterName() {
            return dalClusterName;
        }

        public void setDalClusterName(String dalClusterName) {
            this.dalClusterName = dalClusterName;
        }

        public Set<String> getZoneIds() {
            return zoneIds;
        }

        public void setZoneIds(Set<String> zoneIds) {
            this.zoneIds = zoneIds;
        }

        public String getType() {
            return type;
        }

        public void setType(String type) {
            this.type = type;
        }

        @Override
        public String toString() {
            return "DalClusterInfoWrapper{" +
                    "dalClusterName='" + dalClusterName + '\'' +
                    ", type='" + type + '\'' +
                    ", zoneIds=" + zoneIds +
                    '}';
        }
    }
}
