package com.ctrip.framework.drc.console.service.remote.qconfig;

import com.ctrip.framework.drc.console.config.DomainConfig;
import com.ctrip.framework.drc.console.service.impl.api.ApiContainer;
import com.ctrip.framework.drc.console.service.remote.qconfig.request.BatchUpdateRequestBody;
import com.ctrip.framework.drc.console.service.remote.qconfig.request.CreateFileRequestBody;
import com.ctrip.framework.drc.console.service.remote.qconfig.request.UpdateRequestBody;
import com.ctrip.framework.drc.console.service.remote.qconfig.response.BatchUpdateResponse;
import com.ctrip.framework.drc.console.service.remote.qconfig.response.FileDetailData;
import com.ctrip.framework.drc.console.service.remote.qconfig.response.FileDetailResponse;
import com.ctrip.framework.drc.console.utils.MySqlUtils.TableSchemaName;
import com.ctrip.framework.drc.core.Constants;
import com.ctrip.framework.drc.core.http.HttpUtils;
import com.ctrip.framework.drc.core.monitor.reporter.DefaultEventMonitorHolder;
import com.ctrip.framework.drc.core.monitor.reporter.EventMonitor;
import com.ctrip.framework.drc.core.server.common.filter.table.aviator.AviatorRegexFilter;
import com.ctrip.framework.drc.core.service.dal.DbClusterApiService;
import com.ctrip.framework.foundation.Foundation;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

/**
 * @ClassName QConfigServiceImpl
 * @Author haodongPan
 * @Date 2023/2/3 17:32
 * @Version: $0  http://pages.release.ctripcorp.com/framework/qconfig/#/5_open-api/5.1_api-domains
 */
@Service
public class QConfigServiceImpl implements QConfigService {
    
    private static final Logger logger = LoggerFactory.getLogger(QConfigServiceImpl.class);
    
    @Autowired private DomainConfig domainConfig;

    private  DbClusterApiService dbClusterService = ApiContainer.getDbClusterApiServiceImpl();
    private  EventMonitor eventMonitor = DefaultEventMonitorHolder.getInstance();                
    

    private static final String FILE_DETAIL = "/configs";
    private static final String BATCH_UPDATE_PROPERTIES = "restapi/properties/%s/envs/%s/subenvs/%s";
    private static final String PROPERTIES_SUFFIX = ".properties";
    private static final String FWS = "fws";
    private static final String FAT = "fat";
    private static final String BINLOG_TOPIC_REGISTRY = "binlog-topic-registry";
    private static final String STATUS = "status";
    private static final String DBNAME = "dbName";
    private static final String TABLENAME = "tableName";
    private static final String TAG = "tag";
    private static final String ON = "on";
    private static final String OFF = "off";


    /**
     * 
     * @param fileDc
     * @param topic
     * @param fullTableName schema\.table 
     * @param tag
     * @param matchTables
     * @return
     */
    @Override
    public boolean addOrUpdateDalClusterMqConfig(String fileDc, String topic, String fullTableName, String tag,
            List<TableSchemaName> matchTables) {
    /*
        dbadalclustertest01db_dalcluster.properties
        bbz.fx.drc.qmq.test.status=on
        bbz.fx.drc.qmq.test.dbName=dbadalclustertest01db
        bbz.fx.drc.qmq.test.tableName=test
        bbz.fx.drc.qmq.test.tag=bbzDrcTest
         */

        logger.info("[[tag=BINLOG_TOPIC_REGISTRY]] generate todo,topic:{},table:{},matchTable size:{}",
                topic,fullTableName,matchTables.size());

        // machine,fileConfig  env & subEnv is same
        String localEnv = getLocalEnv();
        String fileSubEnv = getFileSubEnv(fileDc);

        String dalClusterName = dbClusterService.getDalClusterName(domainConfig.getDalClusterUrl(),
                fullTableName.split("\\\\.")[0]);
        String fileName = dalClusterName + PROPERTIES_SUFFIX;

        FileDetailResponse fileDetailResponse = queryFileDetail(fileName, localEnv, fileSubEnv, BINLOG_TOPIC_REGISTRY);
        if (fileDetailResponse.isExist()) {
            FileDetailData fileDetailData = fileDetailResponse.getData();
            Map<String, String> originalConfig = string2config(fileDetailData.getData());
            Map<String, String> configContext = processAddOrUpdateConfig(topic,tag, matchTables,originalConfig);
            int version = fileDetailResponse.getData().getEditVersion();
            BatchUpdateRequestBody request = transformRequest(configContext, fileName, version);
            BatchUpdateResponse batchUpdateResponse = batchUpdateConfigFile(BINLOG_TOPIC_REGISTRY, localEnv, fileSubEnv,
                    request);
            if (batchUpdateResponse.getStatus().equals(0)) {
                // success
                logger.info("[[tag=BINLOG_TOPIC_REGISTRY]] update success,fileName:{}",fileName);
                return true;
            } else {
                // fail
                logger.error("[[tag=BINLOG_TOPIC_REGISTRY]] update fail,fileName:{},topic:{},table:{}",
                        fileName,topic,fullTableName);
                return false;
            }
        } else {
            Map<String, String> configContext = processAddOrUpdateConfig(topic,tag, matchTables,null);
            CreateFileRequestBody requestBody = transformRequest(configContext, localEnv, fileSubEnv);
            Integer status = createFile(requestBody);
            if (status == 0) {
                // success
                logger.info("[[tag=BINLOG_TOPIC_REGISTRY]] create success,fileName:{}",fileName);
                return true;
            } else {
                logger.error("[[tag=BINLOG_TOPIC_REGISTRY]] create fail,fileName:{},topic:{},table:{}",
                        fileName,topic,fullTableName);
                return false;
            }

        }
    }
    
    @Override
    public boolean removeDalClusterMqConfigIfNecessary(String fileDc, String topic, String table, String tag,
            List<TableSchemaName> matchTables, List<String> otherTablesByTopic) {
        // machine,fileConfig  env & subEnv is same
        String localEnv = getLocalEnv();
        String fileSubEnv = getFileSubEnv(fileDc);

        String dalClusterName = dbClusterService.getDalClusterName(domainConfig.getDalClusterUrl(),table.split("\\\\.")[0]);
        String fileName = dalClusterName + PROPERTIES_SUFFIX;

        logger.info("[[tag=BINLOG_TOPIC_REGISTRY]] delete todo, fileName:{},topic:{},table:{}",fileName,topic,table);

        FileDetailResponse fileDetailResponse = queryFileDetail(fileName, localEnv, fileSubEnv, BINLOG_TOPIC_REGISTRY);
        if (fileDetailResponse.isExist()) {
            FileDetailData fileDetailData = fileDetailResponse.getData();
            Map<String, String> originalConfig = string2config(fileDetailData.getData());
            int version = fileDetailResponse.getData().getEditVersion();
            BatchUpdateResponse batchUpdateResponse = null;
            if (CollectionUtils.isEmpty(otherTablesByTopic)) {
                // topic no use,remove directly
                Map<String, String> configContext = processRemoveAllConfig(topic,originalConfig);
                BatchUpdateRequestBody request = transformRequest(configContext, fileName, version);
                batchUpdateResponse = batchUpdateConfigFile(BINLOG_TOPIC_REGISTRY, localEnv, fileSubEnv,
                        request);

            } else {
                // check table is in use or not
                List<TableSchemaName> tablesToDeleted = Lists.newArrayList();
                List<TableSchemaName> tablesNotToDeleted = Lists.newArrayList();
                List<AviatorRegexFilter> filters = Lists.newArrayList();
                otherTablesByTopic.forEach(logicalTable -> filters.add(new AviatorRegexFilter(logicalTable)));
                for (TableSchemaName match : matchTables) {
                    String tableName = match.getDirectSchemaTableName();
                    for (AviatorRegexFilter filter : filters) {
                        if (filter.filter(tableName)) {
                            tablesNotToDeleted.add(match);
                            break;
                        } else {
                            tablesToDeleted.add(match);
                        }
                    }
                }

                if (CollectionUtils.isEmpty(tablesNotToDeleted)) {
                    // topic no use in this dalcluster ,remove director
                    Map<String, String> configContext = processRemoveAllConfig(topic,originalConfig);
                    BatchUpdateRequestBody request = transformRequest(configContext, fileName, version);
                    batchUpdateResponse = batchUpdateConfigFile(BINLOG_TOPIC_REGISTRY, localEnv, fileSubEnv, request);
                } else {
                    // remove only some table
                    Map<String, String> configContext = processRemovePartialConfig(topic, tag, tablesToDeleted,
                            originalConfig);
                    BatchUpdateRequestBody request = transformRequest(configContext, fileName, version);
                    batchUpdateResponse = batchUpdateConfigFile(BINLOG_TOPIC_REGISTRY, localEnv, fileSubEnv, request);
                }
            }

            if (batchUpdateResponse.getStatus().equals(0)) {
                // success
                logger.info("[[tag=BINLOG_TOPIC_REGISTRY]] remove success,fileName:{}",fileName);
                return true;
            } else {
                // fail
                logger.error("[[tag=BINLOG_TOPIC_REGISTRY]] remove fail,fileName:{},topic:{},table:{}", fileName,topic,table);
                return false;
            }
        } else {
            logger.warn("[[tag=BINLOG_TOPIC_REGISTRY]] file not exist,no need to remove,fileName:{},topic:{},table:{}",
                    fileName,topic,table);
            return true;
        }

    }
    

    private Map<String, String> processRemoveAllConfig(String topic, Map<String, String> originalConfig) {
        Map<String, String> config = Maps.newLinkedHashMap();
        config.put(topic + "." + STATUS,OFF);
        config.put(topic + "." + DBNAME,"");
        config.put(topic + "." + TABLENAME,"");
        config.put(topic + "." + TAG,"");
        return config;
    }

    private CreateFileRequestBody transformRequest(Map<String, String> context,String env,String subEnv) {
        CreateFileRequestBody requestBody = new CreateFileRequestBody();
        requestBody.setOperator(Constants.ENTITY_DRC);
        requestBody.setRequesttime(LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
        
        Map<String, Object> config = Maps.newLinkedHashMap();
        config.put("groupid", Foundation.app().getAppId());
        config.put("targetgroupid", "qconfig-test-2");
        config.put("env", env);
        config.put("subenv", subEnv);
        config.put("serverenv", Foundation.server().getEnv().getName().toLowerCase());
        config.put("serversubenv", Foundation.server().getSubEnv() == null ? "" : Foundation.server().getSubEnv());
        config.put("dataid", BINLOG_TOPIC_REGISTRY);
        config.put("version", 0);
        config.put("content", config2String(context));
        config.put("public", "true");
        config.put("description", "upload mqConfig for dal");
        
        requestBody.setConfig(config);
        return requestBody;
    }

    
    
    private String config2String(Map<String,String> context) {
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<String, String> entry : context.entrySet()) {
            sb.append(entry.getKey()).append("=").append(entry.getValue()).append("\n");
        }
        return sb.toString();
    }

    private  Map<String,String> string2config(String context) {
        Map<String,String>  res = Maps.newHashMap();
        String[] configs = context.split("\n");
        for (int i = 0; i < configs.length; i++){
            String config = configs[i];
            String[] kv = config.split("=");
            res.put(kv[0],kv[1]);
        }
        return res;
    }
    
    
    
    private BatchUpdateRequestBody transformRequest(Map<String, String> context,String fileName,int currentVersion) {
        BatchUpdateRequestBody batchUpdateRequestBody = new BatchUpdateRequestBody();
        UpdateRequestBody updateRequestBody = new UpdateRequestBody();
        updateRequestBody.setDataid(fileName);
        updateRequestBody.setVersion(currentVersion);
        updateRequestBody.setData(context);
        batchUpdateRequestBody.setUpdateRequestBodies(Lists.newArrayList(updateRequestBody));
        return batchUpdateRequestBody;
    }

    private Map<String, String> processRemovePartialConfig(String topic,String tag, List<TableSchemaName> tablesToBeDelete,
            Map<String, String> originalConfig) {
        // only update tableName is enough
        Set<String> tables = Sets.newHashSet();
        if (!CollectionUtils.isEmpty(originalConfig)) {
            // remove some originalConfig
            String tableString = originalConfig.get(topic + "." + TABLENAME);
            if (StringUtils.isNotEmpty(tableString)) {
                tables.addAll(Sets.newHashSet(tableString.split(",")));
                for (TableSchemaName table : tablesToBeDelete) {
                    tables.remove(table.getName());
                }
                Map<String, String> config = Maps.newLinkedHashMap();
                config.put(tableString,StringUtils.join(tables, ","));
                return config;
            }
        }
        throw new IllegalArgumentException("originalConfig is empty");
    }
    
    
    
    
    private Map<String, String> processAddOrUpdateConfig(String topicRelated,String tagRelated, List<TableSchemaName> matchTables,
            Map<String, String> originalConfig) {
        Set<String> dbs = Sets.newHashSet();
        Set<String> tables = Sets.newHashSet();
        String tag = null;
        for (TableSchemaName table: matchTables) {
            dbs.add(table.getSchema());
            tables.add(table.getName());
        }
        
        String statusKey = topicRelated + "." + STATUS;
        String dbNameKey = topicRelated + "." + DBNAME;
        String tableNameKey = topicRelated + "." + TABLENAME;
        String tagKey = topicRelated + "." + TAG;
        
        if (!CollectionUtils.isEmpty(originalConfig)) {
            // merge originalConfig
            String dbString = originalConfig.get(dbNameKey);
            String tableString = originalConfig.get(tableNameKey);
            String tagString = originalConfig.get(tagKey);
            if (StringUtils.isNotEmpty(dbString)) {
                dbs.addAll(Sets.newHashSet(dbString.split(",")));
            }
            if (StringUtils.isNotEmpty(tableString)) {
                tables.addAll(Sets.newHashSet(tableString.split(",")));
            }
            if (StringUtils.isNotEmpty(tagString)) {
                if (StringUtils.isNotEmpty(tagRelated) && tagString.equalsIgnoreCase(tagRelated)) {
                    throw new IllegalArgumentException("tag error ,original: " + tagString + "new: " + tagRelated);
                }
                tag = tagString;
            }
        }

        Map<String, String> config = Maps.newLinkedHashMap();
        config.put(statusKey, ON);
        config.put(dbNameKey, StringUtils.join(dbs,","));
        config.put(tableNameKey, StringUtils.join(tables,","));
        if (StringUtils.isNotEmpty(tag)) {
            config.put(tagKey, tag);
        }
        return config;
    }
    
    private Integer createFile(CreateFileRequestBody requestBody) {
        String fileName = (String) requestBody.getConfig().get("dataid");
        eventMonitor.logEvent("QConfig.OpenApi.MqConfig.Crate",fileName);
        LinkedHashMap<String, Object> urlParams = Maps.newLinkedHashMap();
        urlParams.put("token", domainConfig.getQConfigAPIToken());
        String url = domainConfig.getQConfigRestApiUrl() + FILE_DETAIL;
        return HttpUtils.post(url,requestBody,Integer.class,urlParams);
    }
    
    
    private BatchUpdateResponse batchUpdateConfigFile(
            String targetgroupid,String targetenv, String targetsubenv, BatchUpdateRequestBody requestBody) {
        String fileName = requestBody.getUpdateRequestBodies().get(0).getDataid();
        eventMonitor.logEvent("QConfig.OpenApi.MqConfig.Update",fileName);
        LinkedHashMap<String, Object> urlParams = Maps.newLinkedHashMap();
        urlParams.put("token", domainConfig.getQConfigAPIToken());
        urlParams.put("operator", Constants.ENTITY_DRC);
        urlParams.put("serverenv", Foundation.server().getEnv().getName().toLowerCase());
        urlParams.put("groupid", Foundation.app().getAppId());
        String format = domainConfig.getQConfigRestApiUrl() + BATCH_UPDATE_PROPERTIES;
        String url = String.format(format, targetgroupid, targetenv, targetsubenv);
        return HttpUtils.post(url,requestBody,BatchUpdateResponse.class,urlParams);
    }
    
    private FileDetailResponse queryFileDetail(String fileName,String env,String subEnv,String targetgroupid) {
        eventMonitor.logEvent("QConfig.OpenApi.MqConfig.Query",fileName);
        LinkedHashMap<String, Object> urlParams = Maps.newLinkedHashMap();
        urlParams.put("token", domainConfig.getQConfigAPIToken());
        urlParams.put("groupid", Foundation.app().getAppId());
        urlParams.put("dataid", fileName);
        urlParams.put("env", env);
        urlParams.put("subenv", subEnv);
        urlParams.put("targetgroupid", targetgroupid);
        String url = domainConfig.getQConfigRestApiUrl() + FILE_DETAIL;
        return HttpUtils.get(url,FileDetailResponse.class,urlParams);
    }

    private String getLocalEnv() {
        String env = Foundation.server().getEnv().getName().toLowerCase();
        if (StringUtils.isEmpty(env)) {
            return "";
        }
        if (FWS.equalsIgnoreCase(env)) {
            return FAT;
        } else {
            return env;
        }
    }
    
    private String getFileSubEnv(String dc) {
        Map<String, String> dc2QConfigSubEnvMap = domainConfig.getDc2QConfigSubEnvMap();
        String subEnv = dc2QConfigSubEnvMap.get(dc);
        if (StringUtils.isEmpty(subEnv)) {
            return "";
        } else {
            return subEnv;
        }
    }
    
}
