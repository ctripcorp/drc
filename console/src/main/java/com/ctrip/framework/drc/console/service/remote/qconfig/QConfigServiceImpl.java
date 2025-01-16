package com.ctrip.framework.drc.console.service.remote.qconfig;

import com.ctrip.framework.drc.console.config.DomainConfig;
import com.ctrip.framework.drc.console.service.impl.api.ApiContainer;
import com.ctrip.framework.drc.console.service.remote.qconfig.request.CreateFileRequestBody;
import com.ctrip.framework.drc.console.service.remote.qconfig.request.UpdateRequestBody;
import com.ctrip.framework.drc.console.service.remote.qconfig.response.BatchUpdateResponse;
import com.ctrip.framework.drc.console.service.remote.qconfig.response.CreateFileResponse;
import com.ctrip.framework.drc.console.service.remote.qconfig.response.FileDetailData;
import com.ctrip.framework.drc.console.service.remote.qconfig.response.FileDetailResponse;
import com.ctrip.framework.drc.console.utils.MySqlUtils.TableSchemaName;
import com.ctrip.framework.drc.core.Constants;
import com.ctrip.framework.drc.core.http.HttpUtils;
import com.ctrip.framework.drc.core.monitor.reporter.DefaultEventMonitorHolder;
import com.ctrip.framework.drc.core.monitor.reporter.EventMonitor;
import com.ctrip.framework.drc.core.server.common.filter.table.aviator.AviatorRegexFilter;
import com.ctrip.framework.drc.core.service.dal.DbClusterApiService;
import com.ctrip.framework.drc.core.service.utils.JsonUtils;
import com.ctrip.framework.foundation.Foundation;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.gson.JsonObject;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Collectors;

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
    private static final String BATCH_UPDATE_PROPERTIES = "/properties/%s/envs/%s/subenvs/%s";
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
        bbz.fx.drc.qmq.test.tag=bbzDrcTestTag
         */
        Set<String> dcsInSameRegion = domainConfig.getIDCsInSameRegion(fileDc);
        boolean batchActionFlag = true;
        for (String affectedDc : dcsInSameRegion) {
            logger.info("[[tag=BINLOG_TOPIC_REGISTRY]] generate todo,topic:{},table:{},matchTable size:{}",
                    topic,fullTableName,matchTables.size());

            // machine,fileConfig  env & subEnv is same
            String localEnv = getLocalEnv();
            String fileSubEnv = getFileSubEnv(affectedDc);
            
            String dalClusterName = dbClusterService.getDalClusterName(domainConfig.getDalClusterUrl(),
                    matchTables.get(0).getSchema());
            String fileName = dalClusterName + PROPERTIES_SUFFIX;

            FileDetailResponse fileDetailResponse = queryFileDetail(fileName, localEnv, fileSubEnv, BINLOG_TOPIC_REGISTRY);
            if (fileDetailResponse.isExist()) {
                FileDetailData fileDetailData = fileDetailResponse.getData();
                Map<String, String> originalConfig = string2config(fileDetailData.getData());
                List<TableSchemaName> tablesNeedChange = filterTablesWithAnotherMqInQConfig(originalConfig, matchTables, topic);
                if (CollectionUtils.isEmpty(tablesNeedChange)) {
                    continue;
                }
                Map<String, String> configContext = processAddOrUpdateConfig(topic,tag, tablesNeedChange,originalConfig);
                int version = fileDetailResponse.getData().getEditVersion();
                List<UpdateRequestBody> updateRequestBodies = transformRequest(configContext, fileName, version);
                BatchUpdateResponse batchUpdateResponse = batchUpdateConfigFile(BINLOG_TOPIC_REGISTRY, localEnv, fileSubEnv,
                        updateRequestBodies);
                if (batchUpdateResponse.getStatus() == 0) {
                    // success
                    logger.info("[[tag=BINLOG_TOPIC_REGISTRY]] update success,fileName:{}",fileName);
                } else {
                    // fail
                    logger.error("[[tag=BINLOG_TOPIC_REGISTRY]] update fail,fileName:{},topic:{},table:{}",
                            fileName,topic,fullTableName);
                    batchActionFlag = false;
                }
            } else {
                Map<String, String> configContext = processAddOrUpdateConfig(topic,tag, matchTables,null);
                CreateFileRequestBody requestBody = transformRequest(fileName,configContext, localEnv, fileSubEnv);
                CreateFileResponse response = createFile(requestBody);
                if (response.getStatus() == 0) {
                    // success
                    logger.info("[[tag=BINLOG_TOPIC_REGISTRY]] create success,fileName:{}",fileName);
                } else {
                    logger.error("[[tag=BINLOG_TOPIC_REGISTRY]] create fail,fileName:{},topic:{},table:{}",
                            fileName,topic,fullTableName);
                    batchActionFlag = false;
                }

            }
        }
        return batchActionFlag;
    }

    @Override
    public boolean reWriteDalClusterMqConfig(String dcName, String dalClusterName, Map<String, String> configContext) {
        Set<String> dcsInSameRegion = domainConfig.getIDCsInSameRegion(dcName);
        boolean batchActionFlag = true;
        for (String affectedDc : dcsInSameRegion) {
            String localEnv = getLocalEnv();
            String fileSubEnv = getFileSubEnv(affectedDc);
            String fileName = dalClusterName + PROPERTIES_SUFFIX;

            // query current config
            logger.info("[[tag=BINLOG_TOPIC_REGISTRY]] delete todo, fileName:{}", fileName);
            FileDetailResponse fileDetailResponse = queryFileDetail(fileName, localEnv, fileSubEnv, BINLOG_TOPIC_REGISTRY);
            if (!fileDetailResponse.isExist()) {
                logger.warn("[[tag=BINLOG_TOPIC_REGISTRY]] file not exist,no need to remove,fileName:{}", fileName);
                continue;
            }

            FileDetailData fileDetailData = fileDetailResponse.getData();
            processRemovedTopic(configContext, fileDetailData);

            int version = fileDetailResponse.getData().getEditVersion();

            // put result
            List<UpdateRequestBody> updateRequestBodies = transformRequest(configContext, fileName, version);
            BatchUpdateResponse batchUpdateResponse = batchUpdateConfigFile(BINLOG_TOPIC_REGISTRY, localEnv, fileSubEnv, updateRequestBodies);

            if (batchUpdateResponse.getStatus() == 0) {
                // success
                logger.info("[[tag=BINLOG_TOPIC_REGISTRY]] update success,fileName:{}", fileName);
            } else {
                // fail
                logger.error("[[tag=BINLOG_TOPIC_REGISTRY]] update fail,fileName:{}", fileName);
                batchActionFlag = false;
            }
        }
        return batchActionFlag;
    }

    private void processRemovedTopic(Map<String, String> configContext, FileDetailData fileDetailData) {
        Map<String, String> originalConfig = string2config(fileDetailData.getData());
        Set<String> originTopics = originalConfig.entrySet().stream()
                .filter(e -> e.getKey().endsWith("." + STATUS) && e.getValue().equals(ON))
                .map(e -> e.getKey().substring(0, e.getKey().length() - STATUS.length() - 1))
                .collect(Collectors.toSet());
        for (String originTopic : originTopics) {
            if (!configContext.containsKey(originTopic + "." + STATUS)) {
                // topic removed
                processRemoveAllConfig(originTopic, configContext);
            }
        }
    }

    @Override
    public boolean updateDalClusterMqConfig(String dcName, String topic, String dalClusterName, List<TableSchemaName> matchTables) {
        Set<String> dcsInSameRegion = domainConfig.getIDCsInSameRegion(dcName);
        boolean batchActionFlag = true;
        for (String affectedDc : dcsInSameRegion) {
            String localEnv = getLocalEnv();
            String fileSubEnv = getFileSubEnv(affectedDc);
            String fileName = dalClusterName + PROPERTIES_SUFFIX;

            // query current config
            logger.info("[[tag=BINLOG_TOPIC_REGISTRY]] delete todo, fileName:{},topic:{}", fileName, topic);
            FileDetailResponse fileDetailResponse = queryFileDetail(fileName, localEnv, fileSubEnv, BINLOG_TOPIC_REGISTRY);
            if (!fileDetailResponse.isExist()) {
                logger.warn("[[tag=BINLOG_TOPIC_REGISTRY]] file not exist,no need to remove,fileName:{},topic:{}", fileName, topic);
                continue;
            }

            FileDetailData fileDetailData = fileDetailResponse.getData();
            Map<String, String> originalConfig = string2config(fileDetailData.getData());
            if (!originalConfig.containsKey(topic + "." + STATUS)) {
                continue;
            }
            List<TableSchemaName> tablesNeedChange = filterTablesWithAnotherMqInQConfig(originalConfig, matchTables, topic);

            int version = fileDetailResponse.getData().getEditVersion();

            // put result
            Map<String, String> configContext = convertToContext(topic, tablesNeedChange);
            List<UpdateRequestBody> updateRequestBodies = transformRequest(configContext, fileName, version);
            BatchUpdateResponse batchUpdateResponse = batchUpdateConfigFile(BINLOG_TOPIC_REGISTRY, localEnv, fileSubEnv, updateRequestBodies);

            if (batchUpdateResponse.getStatus() == 0) {
                // success
                logger.info("[[tag=BINLOG_TOPIC_REGISTRY]] update success,fileName:{}", fileName);
            } else {
                // fail
                logger.error("[[tag=BINLOG_TOPIC_REGISTRY]] update fail,fileName:{},topic:{}", fileName, topic);
                batchActionFlag = false;
            }
        }
        return batchActionFlag;
    }
    
    @Override
    public boolean removeDalClusterMqConfigIfNecessary(String fileDc, String topic, String table, String tag,
            List<TableSchemaName> matchTables, List<String> otherTablesByTopic) {
        Set<String> dcsInSameRegion = domainConfig.getIDCsInSameRegion(fileDc);
        boolean batchActionFlag = true;
        for (String affectedDc : dcsInSameRegion) {
            // machine,fileConfig  env & subEnv is same
            String localEnv = getLocalEnv();
            String fileSubEnv = getFileSubEnv(affectedDc);

            String dalClusterName = dbClusterService.getDalClusterName(domainConfig.getDalClusterUrl(),matchTables.get(0).getSchema());
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
                    Map<String, String> configContext = processRemoveAllConfig(topic);
                    List<UpdateRequestBody> updateRequestBodies = transformRequest(configContext, fileName, version);
                    batchUpdateResponse = batchUpdateConfigFile(BINLOG_TOPIC_REGISTRY, localEnv, fileSubEnv,
                            updateRequestBodies);

                } else {
                    // check table is in use or not
                    List<TableSchemaName> tablesToDeleted = Lists.newArrayList();
                    List<AviatorRegexFilter> filters = Lists.newArrayList();
                    otherTablesByTopic.forEach(logicalTable -> filters.add(new AviatorRegexFilter(logicalTable)));
                    for (TableSchemaName match : matchTables) {
                        String tableName = match.getDirectSchemaTableName();
                        for (AviatorRegexFilter filter : filters) {
                            if (filter.filter(tableName)) {
                                break;
                            } else {
                                tablesToDeleted.add(match);
                            }
                        }
                    }
                    
                    // remove only some tables
                    Map<String, String> configContext = processRemovePartialConfig(topic, tag, tablesToDeleted,
                            originalConfig);
                    List<UpdateRequestBody> updateRequestBodies= transformRequest(configContext, fileName, version);
                    batchUpdateResponse = batchUpdateConfigFile(BINLOG_TOPIC_REGISTRY, localEnv, fileSubEnv, updateRequestBodies);
                    
                }

                if (batchUpdateResponse.getStatus() == 0) {
                    // success
                    logger.info("[[tag=BINLOG_TOPIC_REGISTRY]] remove success,fileName:{}",fileName);
                } else {
                    // fail
                    logger.error("[[tag=BINLOG_TOPIC_REGISTRY]] remove fail,fileName:{},topic:{},table:{}", fileName,topic,table);
                    batchActionFlag = false;
                }
            } else {
                logger.warn("[[tag=BINLOG_TOPIC_REGISTRY]] file not exist,no need to remove,fileName:{},topic:{},table:{}",
                        fileName,topic,table);
            }
        }
        return batchActionFlag;
    }
    

    private Map<String, String> processRemoveAllConfig(String topic) {
        Map<String, String> config = Maps.newLinkedHashMap();
        return processRemoveAllConfig(topic, config);
    }

    private static Map<String, String> processRemoveAllConfig(String topic, Map<String, String> config) {
        config.put(topic + "." + STATUS, OFF);
        config.put(topic + "." + DBNAME, "");
        config.put(topic + "." + TABLENAME, "");
        return config;
    }

    private Map<String, String> convert(String topic, String dbs, String tables) {
        Map<String, String> config = Maps.newLinkedHashMap();
        config.put(topic + "." + STATUS, ON);
        config.put(topic + "." + DBNAME, dbs);
        config.put(topic + "." + TABLENAME, tables);
        return config;
    }

    private CreateFileRequestBody transformRequest(String fileName,Map<String, String> context,String env,String subEnv) {
        CreateFileRequestBody requestBody = new CreateFileRequestBody();
        requestBody.setOperator(Constants.ENTITY_DRC);
        requestBody.setRequesttime(LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
        
        Map<String, Object> config = Maps.newLinkedHashMap();
        config.put("groupid", Foundation.app().getAppId());
        config.put("targetgroupid", BINLOG_TOPIC_REGISTRY);
        config.put("env", env);
        config.put("subenv", subEnv);
        config.put("serverenv", env);
        config.put("serversubenv", subEnv);
        config.put("dataid", fileName);
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
        Map<String,String>  res = Maps.newLinkedHashMap();
        if (StringUtils.isBlank(context)) {
            return res;
        }
        String[] configs = context.split("\n");
        for (int i = 0; i < configs.length; i++){
            String config = configs[i];
            String[] kv = config.split("=");
            if (kv.length != 2) {
                res.put(kv[0],"");
            } else {
                res.put(kv[0],kv[1]);  
            }
        }
        return res;
    }
    
    
    
    public List<UpdateRequestBody>  transformRequest(Map<String, String> context,String fileName,int currentVersion) {
        UpdateRequestBody updateRequestBody = new UpdateRequestBody();
        updateRequestBody.setDataid(fileName);
        updateRequestBody.setVersion(currentVersion);
        updateRequestBody.setData(context);
        return Lists.newArrayList(updateRequestBody);
    }

    private Map<String, String> processRemovePartialConfig(String topic,String tag, List<TableSchemaName> tablesToBeDelete,
            Map<String, String> originalConfig) {
        // only update tableName is enough
        Set<String> tables = Sets.newLinkedHashSet();
        if (!CollectionUtils.isEmpty(originalConfig)) {
            // remove some originalConfig
            String tableString = originalConfig.get(topic + "." + TABLENAME);
            if (StringUtils.isNotEmpty(tableString)) {
                tables.addAll(Lists.newArrayList(tableString.split(",")));
                for (TableSchemaName tableToBeDeleted : tablesToBeDelete) {
                    tables.remove(tableToBeDeleted.getName());
                }
                
                Map<String, String> config = Maps.newLinkedHashMap();
                if (tables.isEmpty()) {
                    // all Table remove
                    config =  processRemoveAllConfig(topic);
                } else {
                    config.put(topic + "." + TABLENAME,StringUtils.join(tables, ","));
                }
                return config;
            }
        }
        throw new IllegalArgumentException("originalConfig is empty");
    }

    private Map<String, String> convertToContext(String topic, List<TableSchemaName> tables) {
        if (CollectionUtils.isEmpty(tables)) {
            return processRemoveAllConfig(topic);
        }
        String topicDb = tables.stream().map(TableSchemaName::getSchema).distinct().collect(Collectors.joining(","));
        String topicTable = tables.stream().map(TableSchemaName::getName).distinct().collect(Collectors.joining(","));
        return convert(topic, topicDb, topicTable);
    }

    private Map<String, String> processAddOrUpdateConfig(String topicRelated,String tagRelated, List<TableSchemaName> matchTables,
            Map<String, String> originalConfig) {
        Set<String> dbs = Sets.newLinkedHashSet();
        Set<String> tables = Sets.newLinkedHashSet();
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
                dbs.addAll(Lists.newArrayList(dbString.split(",")));
            }
            if (StringUtils.isNotEmpty(tableString)) {
                tables.addAll(Lists.newArrayList(tableString.split(",")));
            }
            if (StringUtils.isNotEmpty(tagString)) {
                if (StringUtils.isNotEmpty(tagRelated) && !tagString.equalsIgnoreCase(tagRelated)) {
                    throw new IllegalArgumentException("tag error ,original: " + tagString + ",new: " + tagRelated);
                }
                tag = tagString;
            } else {
                // first time
                if (StringUtils.isEmpty(tableString) && StringUtils.isEmpty(tableString)) {
                    tag = tagRelated;
                }
            }
        } else {
            tag = tagRelated;
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
    
    private CreateFileResponse createFile(CreateFileRequestBody requestBody) {
        String fileName = (String) requestBody.getConfig().get("dataid");
        eventMonitor.logEvent("QConfig.OpenApi.MqConfig.Crate",fileName);
        LinkedHashMap<String, Object> urlParams = Maps.newLinkedHashMap();
        urlParams.put("token", domainConfig.getQConfigAPIToken());
        String url = domainConfig.getQConfigRestApiUrl() + FILE_DETAIL + "?token={token}";
        return HttpUtils.post(url,requestBody, CreateFileResponse.class,urlParams);
    }
    
    
    private BatchUpdateResponse batchUpdateConfigFile(
            String targetgroupid,String targetenv, String targetsubenv, List<UpdateRequestBody> requestBody) {
        String fileName = requestBody.get(0).getDataid();
        eventMonitor.logEvent("QConfig.OpenApi.MqConfig.Update",fileName);
        LinkedHashMap<String, Object> urlParams = Maps.newLinkedHashMap();
        urlParams.put("token", domainConfig.getQConfigAPIToken());
        urlParams.put("operator", Constants.ENTITY_DRC);
        urlParams.put("serverenv", Foundation.server().getEnv().getName().toLowerCase());
        urlParams.put("groupid", Foundation.app().getAppId());
        String format = domainConfig.getQConfigRestApiUrl() + BATCH_UPDATE_PROPERTIES +
                "?token={token}&operator={operator}&serverenv={serverenv}&groupid={groupid}";
        String url = String.format(format, targetgroupid, targetenv, targetsubenv);
        String request = JsonUtils.toJson(requestBody);
        return HttpUtils.post(url,request,BatchUpdateResponse.class,urlParams);
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
        String url = domainConfig.getQConfigRestApiUrl() + FILE_DETAIL + 
                "?token={token}&groupid={groupid}&dataid={dataid}&env={env}&subenv={subenv}&targetgroupid={targetgroupid}";
        String resJson = HttpUtils.get(url, String.class, urlParams);
        JsonObject jsonObject = JsonUtils.parseObject(resJson);
        FileDetailResponse fileDetailResponse;
        int status = jsonObject.get("status").getAsInt();
        if (0 == status) {
            fileDetailResponse = JsonUtils.fromJson(resJson, FileDetailResponse.class);
        } else {
            fileDetailResponse = new FileDetailResponse(status);
        }
        return fileDetailResponse;
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

    @VisibleForTesting
    protected List<TableSchemaName> filterTablesWithAnotherMqInQConfig(Map<String, String> originalConfig, List<TableSchemaName> matchTables, String topic) {
        Map<String, String> db2Key = originalConfig.entrySet().stream()
                .filter(entry -> entry.getKey().endsWith("." + DBNAME))
                .flatMap(entry -> {
                    String key = entry.getKey();
                    String[] dbs = entry.getValue().split(",");
                    return Arrays.stream(dbs).map(db -> Map.entry(db.trim(), key));
                })
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        Map.Entry::getValue,
                        (existing, replacement) -> existing
                ));

        String targetTopicKey = topic + "." + STATUS;
        List<TableSchemaName> tables = Lists.newArrayList();
        for (TableSchemaName name : matchTables) {
            String dbName = name.getSchema();
            if (!db2Key.containsKey(dbName)) {
                tables.add(name);
                continue;
            }

            String topicDbNameKey = db2Key.get(dbName);
            String topicTableNameKey = topicDbNameKey.substring(0, topicDbNameKey.length() - DBNAME.length()) + TABLENAME;
            String topicStatusKey = topicDbNameKey.substring(0, topicDbNameKey.length() - DBNAME.length()) + STATUS;

            if (OFF.equalsIgnoreCase(originalConfig.get(topicStatusKey)) || topicStatusKey.equalsIgnoreCase(targetTopicKey)) {
                tables.add(name);
                continue;
            }

            String tblStr = originalConfig.get(topicTableNameKey);
            Set<String> tblsInQConfig = Sets.newHashSet(tblStr.split(","));
            if (!tblsInQConfig.contains(name.getName())) {
                tables.add(name);
            }
        }

        return tables;
    }
    
}
