package com.ctrip.framework.drc.console.config;

import com.ctrip.framework.drc.console.pojo.DalCluster;
import com.ctrip.framework.drc.console.pojo.Mha;
import com.ctrip.framework.drc.console.pojo.TableConfig;
import com.ctrip.framework.drc.console.pojo.TableConfigs;
import com.ctrip.framework.drc.core.service.dal.DbClusterApiService;
import com.ctrip.framework.drc.console.service.impl.api.ApiContainer;
import com.ctrip.xpipe.api.config.Config;
import com.ctrip.xpipe.codec.JsonCodec;
import com.ctrip.xpipe.config.AbstractConfigBean;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * @author shenhaibo
 * @version 1.0
 * date: 2020-04-08
 */
@Component
public class DbClusterRetriever extends AbstractConfigBean {

    private ObjectMapper objectMapper;
    
    @Autowired
    private DomainConfig domainConfig;
    
    private DbClusterApiService dbClusterApiService = ApiContainer.getDbClusterApiServiceImpl();

    public DbClusterRetriever() {
    }

    public DbClusterRetriever(Config config) {
        super(config);
    }

    /**
     * get all the cluster names from DAL
     * @return
     */
    public List<String> getDalClusterNames(JsonNode result) {
        List<String> res = Lists.newArrayList();
        for(final JsonNode objNode : result) {
            res.add(objNode.get("clusterName").asText());
        }
        return res;
    }

    /**
     * get all the cluster objects from DAL
     */
    public List<DalCluster> getDalClusters(JsonNode result) {
        if(null == objectMapper) {
            objectMapper = new ObjectMapper();
        }
        List<DalCluster> res = Lists.newArrayList();
        for(final JsonNode objNode : result) {
            try {
                res.add(objectMapper.treeToValue(objNode, DalCluster.class));
            } catch (JsonProcessingException e) {
                logger.error("JsonProcessingException: ", e);
            }
        }
        return res;
    }

    /**
     * get all mhas objects for give JsonNode list
     */
    public List<Mha> getMhas(JsonNode result) {
        if(null == objectMapper) {
            objectMapper = new ObjectMapper();
        }
        objectMapper.configure(DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY, true);
        List<Mha> res = Lists.newArrayList();
        for(final JsonNode objNode : result) {
            try {
                res.add(objectMapper.treeToValue(objNode, Mha.class));
            } catch (JsonProcessingException e) {
                logger.error("JsonProcessingException: ", e);
            }
        }
        return res;
    }

    /**
     * get all table config objects for give JsonNode list
     */
    public List<TableConfig> getIgnoreTableConfigs(String clusterName) {
        List<TableConfig> res = Lists.newArrayList();
        String dalClusterUrl = domainConfig.getDalClusterUrl();
        JsonNode result = dbClusterApiService.getIgnoreTableConfigs(dalClusterUrl,clusterName);
        if (result != null) {
            String configs = result.toString();
            TableConfigs tableConfigs = JsonCodec.INSTANCE.decode(configs, TableConfigs.class);
            List<TableConfig> tableConfigList = tableConfigs.getTableConfigs();
            if (tableConfigList == null) {
                return null;
            }
            for (TableConfig tableConfig : tableConfigList) {
                if (tableConfig.isIgnoreReplication()) {
                    res.add(tableConfig);
                }
            }
        }

        return res;
    }

    /**
     * get all mhas as JsonNode from DAL for given clusterName
     */
    public JsonNode getMhasNode(String clusterName) {
        String dalClusterUrl = domainConfig.getDalClusterUrl();
        return dbClusterApiService.getMhasNode(dalClusterUrl,clusterName);
    }

    /**
     * get all cluster as JsonNode from DAL for given clusterName
     */
    public JsonNode getDalClusterNode() {
        String dalClusterUrl = domainConfig.getDalClusterUrl();
        return dbClusterApiService.getDalClusterNode(dalClusterUrl);
    }

}