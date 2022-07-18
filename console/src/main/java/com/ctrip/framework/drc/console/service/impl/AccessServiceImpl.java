package com.ctrip.framework.drc.console.service.impl;


import com.ctrip.framework.drc.console.config.DomainConfig;
import com.ctrip.framework.drc.console.dao.entity.MhaGroupTbl;
import com.ctrip.framework.drc.console.dto.BuildMhaDto;
import com.ctrip.framework.drc.console.dto.MhaInstanceGroupDto;
import com.ctrip.framework.drc.console.enums.BooleanEnum;
import com.ctrip.framework.drc.console.enums.EstablishStatusEnum;
import com.ctrip.framework.drc.console.monitor.delay.config.MonitorTableSourceProvider;
import com.ctrip.framework.drc.console.service.AccessService;
import com.ctrip.framework.drc.console.service.impl.api.ApiContainer;
import com.ctrip.framework.drc.console.utils.DalUtils;
import com.ctrip.framework.drc.console.utils.MySqlUtils;
import com.ctrip.framework.drc.core.driver.command.netty.endpoint.MySqlEndpoint;
import com.ctrip.framework.drc.core.driver.command.packet.ResultCode;
import com.ctrip.framework.drc.core.http.ApiResult;
import com.ctrip.framework.drc.core.server.utils.ThreadUtils;
import com.ctrip.framework.drc.core.service.dal.DbClusterApiService;
import com.ctrip.framework.drc.core.service.enums.EnvEnum;
import com.ctrip.framework.drc.core.service.exceptions.UnexpectedEnumValueException;
import com.ctrip.framework.drc.core.service.mysql.MySQLToolsApiService;
import com.ctrip.framework.drc.core.service.utils.JacksonUtils;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;


/**
 * @author maojiawei
 * @version 1.0
 * date: 2020-07-28
 */

@Service
public class AccessServiceImpl implements AccessService {

    private ObjectMapper objectMapper = new ObjectMapper();

    private final Logger logger = LoggerFactory.getLogger(getClass());
    
    private DalUtils dalUtils = DalUtils.getInstance();

    @Autowired
    private MhaServiceImpl mhaService;
    
    @Autowired
    private MetaInfoServiceImpl metaInfoService;

    @Autowired
    private MonitorTableSourceProvider monitorTableSourceProvider;

    @Autowired
    private DalServiceImpl dalService;

    @Autowired
    private DrcMaintenanceServiceImpl drcMaintenanceService;
    
    @Autowired 
    private DomainConfig domainConfig;
    
    private MySQLToolsApiService mySQLToolsApiServiceImpl = ApiContainer.getMySQLToolsApiServiceImpl();
    private DbClusterApiService dbClusterApiServiceImpl = ApiContainer.getDbClusterApiServiceImpl();

    protected ScheduledExecutorService checkNewMhaBuiltService = ThreadUtils.newSingleThreadScheduledExecutor("build-new-mha");

    public static final int INITIAL_DELAY = 0;

    public static final int PERIOD = 30;

    public static final TimeUnit TIME_UNIT = TimeUnit.MINUTES;

    protected Map<String, ScheduledFuture> checkNewMhaBuiltMap = Maps.newConcurrentMap();
    

    private static final String COMPLETED = "T";

//    private static final String INCOMPLETED = "W";

    private static final String NORMAL = "T";

    private static final String DISCONNECT_REPL = "1";

    private static final Boolean DNS_SUCCESS = true;

    public static final String READ_USER_KEY = "readUser";
    public static final String READ_PASSWORD_KEY = "readPassword";
    public static final String WRITE_USER_KEY = "writeUser";
    public static final String WRITE_PASSWORD_KEY = "writePassword";
    public static final String MONITOR_USER_KEY = "monitorUser";
    public static final String MONITOR_PASSWORD_KEY = "monitorPassword";

    @Override
    public Map<String, Object> applyPreCheck(String requestBody) {
        String mysqlPrecheckUrl = domainConfig.getMysqlPrecheckUrl();
        JsonNode root = mySQLToolsApiServiceImpl.applyPreCheck(mysqlPrecheckUrl,requestBody);
        Map<String, Object> res = new HashMap<>();
        if (root.has("fail")) {
            res.put("status", "F");
            res.put("message", root.get("fail").asText());
        } else {
            boolean consoleAccountReady = root.get("message").asText().contains("ready");
            res.put("status", consoleAccountReady ? "T" : "F");
            res.put("message", root.get("message").asText());
            if (consoleAccountReady) {
                try {
                    JsonNode requestNode = objectMapper.readTree(requestBody);
                    String mha = requestNode.get("clustername").asText();
                    logger.info("do pre check {}", mha);
                    res.put("result", doPreCheck(mha));
                } catch (IOException e) {
                    logger.error("Fail to parse {}", requestBody, e);
                }
            }
        }
        return res;
    }

    /**
     * build a new cluster or check if the Mha cluster has been built
     */
    @Override
    public Map<String, Object> buildMhaCluster(String requestBody) {
        String buildMhaClusterRequestBody = generateRequestBody(requestBody, "clustername", "drczone");
        String buildNewClusterUrl = domainConfig.getBuildNewClusterUrl();
        JsonNode root = mySQLToolsApiServiceImpl.buildMhaCluster(buildNewClusterUrl,buildMhaClusterRequestBody);
        Map<String, Object> res = new HashMap();
        if (root.has("fail")) {
            res.put("status", "W");
            res.put("drcCluster", root.get("fail").asText());
        } else {
            String destMha = root.get("drcCluster").asText();
            res.put("status", root.get("status").asText());
            res.put("drcCluster", destMha);
            if (COMPLETED.equalsIgnoreCase(root.get("status").asText())) {
                try {
                    JsonNode requestNode = objectMapper.readTree(requestBody);
                    String buName = requestNode.get("bu").asText();
                    String dalClusterName = requestNode.get("dalclustername").asText();
                    Long appid = requestNode.get("appid").asLong();
                    String mha = requestNode.get("clustername").asText();
                    String destMhaDcName = requestNode.get("drczone").asText();
                    Map<String, String> usersAndPasswords = getUsersAndPasswords(root);
                    initMhaGroup(buName, dalClusterName, appid, mha, null, destMha, destMhaDcName, usersAndPasswords);
                } catch (Exception e) {
                    logger.error("Failed to generate mha group for requestBody: {}", requestBody, e);
                }
            }
        }
        return res;
    }


    @Override
    public Map<String, Object> buildMhaClusterV2(String requestBody) throws Exception {
        String buildMhaClusterRequestBody = generateRequestBody(requestBody, "clustername", "drczone");
        Map<String, Object> res = Maps.newHashMap();
        String originalClusterName = JacksonUtils.getVal(requestBody, "clustername");

        if (originalClusterName != null) {
            CountDownLatch countDownLatch = new CountDownLatch(1);
            AtomicBoolean newMhaBuilt = new AtomicBoolean(false);
            ScheduledFuture<?> checkNewMhaBuiltFuture = checkNewMhaBuiltService.scheduleWithFixedDelay(() -> {
                try {
                    logger.info("[SCHEDULE BUILD MHA][{}] {}. stop it if built", originalClusterName, newMhaBuilt.get());
                    if (!newMhaBuilt.get()) {
                        String buildNewClusterUrl = domainConfig.getBuildNewClusterUrl();
                        JsonNode root = mySQLToolsApiServiceImpl.buildMhaClusterV2(buildNewClusterUrl,buildMhaClusterRequestBody);
                        if (root.has("fail")) {
                            logger.info("[SCHEDULE BUILD MHA][{}] fail", originalClusterName);
                            res.put("status", "W");
                            res.put("drcCluster", root.get("fail").asText());
                        } else {
                            logger.info("[SCHEDULE BUILD MHA][{}]", originalClusterName);
                            String destMha = root.get("drcCluster").asText();
                            res.put("status", root.get("status").asText());
                            res.put("drcCluster", destMha);
                            if (COMPLETED.equalsIgnoreCase(root.get("status").asText())) {
                                logger.info("[BUILD MHA][{}] built", originalClusterName);
                                try {
                                    JsonNode requestNode = objectMapper.readTree(requestBody);
                                    String buName = requestNode.get("bu").asText();
                                    String dalClusterName = requestNode.get("dalclustername").asText();
                                    Long appid = requestNode.get("appid").asLong();
                                    String mha = requestNode.get("clustername").asText();
                                    String destMhaDcName = requestNode.get("drczone").asText();
                                    Map<String, String> usersAndPasswords = getUsersAndPasswords(root);
                                    initMhaGroup(buName, dalClusterName, appid, mha, null, destMha, destMhaDcName, usersAndPasswords);
                                } catch (Exception e) {
                                    logger.error("Failed to generate mha group for requestBody: {}", requestBody, e);
                                }
                                newMhaBuilt.set(true);
                            }
                        }
                        countDownLatch.countDown();
                    }
                } catch (Throwable t) {
                    logger.error("build mha cluster schedule error", t);
                }
            }, INITIAL_DELAY, PERIOD, TIME_UNIT);
            checkNewMhaBuiltMap.put(originalClusterName, checkNewMhaBuiltFuture);

            // make sure to get first res nevertheless the new mha may not be built
            countDownLatch.await();
        } else {
            logger.warn("UNLIKELY-originalClusterName is null, {}", requestBody);
        }

        return res;
    }

    public ApiResult stopCheckNewMhaBuilt(String originalClusterName) {
        ScheduledFuture future = checkNewMhaBuiltMap.remove(originalClusterName);
        if (null != future) {
            boolean cancelResult = future.cancel(true);
            return ApiResult.getSuccessInstance(String.format("cancel checkMhaBuilt task for %s result: %s", originalClusterName, cancelResult));
        }
        return ApiResult.getFailInstance(String.format("no checkMhaBuilt task for %s", originalClusterName));
    }

    /**
     * disconnect the new slave cluster from old one
     * or check the copy status of the new cluster
     */
    @Override
    public Map<String, Object> getCopyResult(String requestBody) {
        String slaveCascadeUrl = domainConfig.getSlaveCascadeUrl();
        JsonNode root = mySQLToolsApiServiceImpl.getCopyResult(slaveCascadeUrl,requestBody);
        Map<String, Object> res = new HashMap();
        if (root.has("fail")) {
            res.put("status", "F");
            res.put("message", root.get("fail").asText());
        } else {
            res.put("status", root.get("status").asText());
            res.put("message", root.get("message").asText());
            if (NORMAL.equalsIgnoreCase(root.get("status").asText())) {
                try {
                    JsonNode requestNode = objectMapper.readTree(requestBody);
                    if (DISCONNECT_REPL.equalsIgnoreCase(requestNode.get("rplswitch").asText())) {
                        logger.info("update disconnect repl info");
                        String srcMha = requestNode.get("OriginalCluster").asText();
                        String destMha = requestNode.get("DrcCluster").asText();
                        try {
                            drcMaintenanceService.updateMhaGroup(srcMha, destMha, EstablishStatusEnum.CUT_REPLICATION);
                        } catch (SQLException e) {
                            logger.error("Failed to update status({}) for {}:{}", EstablishStatusEnum.CUT_REPLICATION.getDescription(), srcMha, destMha, e);
                        }
                    }
                } catch (IOException e) {
                    logger.error("Fail to parse requestBody: {}", requestBody);
                }
            }
        }
        return res;
    }

    /**
     * deploy the dns of old and new drc cluster
     */
    @Override
    public Map<String, Object> deployDns(String requestBody) {
        String dnsDeployUrl = domainConfig.getDnsDeployUrl();
        JsonNode root = mySQLToolsApiServiceImpl.deployDns(dnsDeployUrl,requestBody);
        Map<String, Object> res = new HashMap();
        if (root.has("fail")) {
            res.put("success", false);
            res.put("content", root.get("fail").asText());
        } else {
            res.put("success", root.get("success").asBoolean());
            res.put("content", root.get("content").asText());
            if (DNS_SUCCESS.equals(root.get("success").asBoolean())) {
                try {
                    JsonNode requestNode = objectMapper.readTree(requestBody);
                    String mha = requestNode.get("cluster").asText();
                    String dbNames = requestNode.get("dbnames").asText();
                    String env = requestNode.get("env").asText();
                    logger.info("update deploy dns info for {}:{}:{}", env, mha, dbNames);
                    try {
                        drcMaintenanceService.updateMhaDnsStatus(mha, BooleanEnum.TRUE);
                    } catch (Throwable t) {
                        logger.error("Fail to update dns status({}) for {}", BooleanEnum.TRUE.getDesc(), mha, t);
                    }

                    logger.info("insert mha mysql instances for {}", mha);
                    Map<String, MhaInstanceGroupDto> mhaList = dalService.getMhaList(EnvEnum.getEnvEnum(env).getFoundationEnv());
                    MhaInstanceGroupDto mhaInstanceGroupDto = mhaList.get(mha);
                    if (null != mhaInstanceGroupDto) {
                        try {
                            drcMaintenanceService.updateMhaInstances(mhaInstanceGroupDto);
                        } catch (Throwable t) {
                            logger.error("Fail init mha instances for {}", mha, t);
                        }
                    } else {
                        logger.error("not retrieve mha instances info for: {}", mha);
                    }

                } catch (IOException e) {
                    logger.error("Fail to parse requestBody: {}", requestBody);
                }
            }
        }
        return res;
    }

    /**
     * initialize dal cluster for respective environment
     */
    @Override
    public Map<String, Object> registerDalCluster(String requestBody, String env, String goal) {
        try {
            String dalRegisterPrefix = domainConfig.getDalRegisterPrefix();
            JsonNode root = dbClusterApiServiceImpl.registerDalCluster(dalRegisterPrefix,requestBody,goal);
            Map<String, Object> res = new HashMap();
            if (root.has("fail")) {
                res.put("success", false);
                res.put("message", root.get("fail").asText());
            } else {
                res.put("success", root.get("success").asBoolean());
                res.put("message", root.get("message").asText());
            }
            return res;
        } catch (UnexpectedEnumValueException e) {
            logger.error("wrong enum type", e);
            return null;
        }
    }

    /**
     * release dal cluster
     */
    @Override
    public Map<String, Object> releaseDalCluster(String dalClusterName, String env) {
        String dalServicePrefix = domainConfig.getDalServicePrefix();
        JsonNode root = dbClusterApiServiceImpl.releaseDalCluster(dalServicePrefix,dalClusterName);
        return JacksonUtils.getStringObjectMap(root);
    }

    public void queryDbDnsHelper(JsonNode content, String role, String zoneId, Map<String, Object> res) {
        zoneId += '.';
        JsonNode jn = content.get(role);
        String dnsBefore;
        String dnsAfter;
        if (jn != null && jn.isArray()) {
            for (JsonNode nodeAfter : jn) {
                dnsAfter = nodeAfter.asText();
                if (dnsAfter.contains(zoneId)) {
                    for (JsonNode nodeBefore : jn) {
                        dnsBefore = nodeBefore.asText();
                        if (dnsAfter.replace(zoneId, "").equals(dnsBefore)) {
                            res.put(role + "Before", dnsBefore);
                            res.put(role + "After", dnsAfter);
                            break;
                        }
                    }
                }
            }
        }
    }

    public ApiResult initMhaGroup(BuildMhaDto dto) {
        try {
            Long mhaGroupId = metaInfoService.getMhaGroupId(dto.getOriginalMha(), dto.getNewBuiltMha(), BooleanEnum.TRUE);
            if (mhaGroupId != null) {
                logger.warn("Fail init mha group: {}, already exist in deleted ,please rollback ", dto);
                return ApiResult.getInstance(false, ResultCode.HANDLE_FAIL.getCode(),"already exist group in deleted ,please rollback");
            }
            initMhaGroup(dto.getBuName(), dto.getDalClusterName(), dto.getAppid(), dto.getOriginalMha(), dto.getOriginalMhaDc(), dto.getNewBuiltMha(), dto.getNewBuiltMhaDc(), getUsersAndPasswords(null));
            return ApiResult.getSuccessInstance(true);
        } catch (Exception e) {
            logger.error("Fail init mha group: {}, ", dto, e);
            return ApiResult.getFailInstance(false);
        }
    }

    private void initMhaGroup(String buName, String dalClusterName, Long appid, String originalMha, String originalMhaDc, String newBuiltMha, String newBuiltMhaDc, Map<String, String> usersAndPasswords) throws Exception {
        Long buId = dalUtils.updateOrCreateBu(buName);

        Long clusterId = dalUtils.updateOrCreateCluster(dalClusterName, appid, buId);

        String dc = StringUtils.isNotBlank(originalMhaDc) ? originalMhaDc : mhaService.getDcForMha(originalMha);
        Long originalDcId = dalUtils.updateOrCreateDc(dc);
        Long newBuiltDcId = dalUtils.updateOrCreateDc(newBuiltMhaDc);
            // Based on new Model, many-to-many mapping for mha group and mha, it will create a new mha group nevertheless mha has a group before
        Long mhaGroupId = dalUtils.insertMhaGroup(
                BooleanEnum.FALSE,
                EstablishStatusEnum.BUILT_NEW_MHA, 
                usersAndPasswords.get(READ_USER_KEY), 
                usersAndPasswords.get(READ_PASSWORD_KEY), 
                usersAndPasswords.get(WRITE_USER_KEY), 
                usersAndPasswords.get(WRITE_PASSWORD_KEY), 
                usersAndPasswords.get(MONITOR_USER_KEY), 
                usersAndPasswords.get(MONITOR_PASSWORD_KEY));
        Long originalMhaId = dalUtils.updateOrCreateMha(originalMha, originalDcId);
        Long newBuiltMhaId = dalUtils.updateOrCreateMha(newBuiltMha, newBuiltDcId);

        dalUtils.updateOrCreateGroupMapping(mhaGroupId, originalMhaId);
        dalUtils.updateOrCreateGroupMapping(mhaGroupId, newBuiltMhaId);

        dalUtils.updateOrCreateClusterMhaMap(clusterId, originalMhaId);
        dalUtils.updateOrCreateClusterMhaMap(clusterId, newBuiltMhaId);
    }

    protected Map<String, Object> doPreCheck(String mha) {
        Map<String, Object> res = new HashMap<>();
        Endpoint endpoint = mhaService.getMasterMachineInstance(mha);
        endpoint = new MySqlEndpoint(endpoint.getHost(), endpoint.getPort(), monitorTableSourceProvider.getMonitorUserVal(), monitorTableSourceProvider.getMonitorPasswordVal(), BooleanEnum.TRUE.isValue());
        logger.info("precheck {}({}:{})", mha, endpoint.getHost(), endpoint.getPort());
        List<String> tablesWithOutOnUpdate = MySqlUtils.checkOnUpdate(endpoint, null);
        List<String> tablesWithOutOnUpdateKey = MySqlUtils.checkOnUpdateKey(endpoint);
        List<String> tablesWithOutPkOrUk = MySqlUtils.checkUniqOrPrimary(endpoint, null);
        String gtidMode = MySqlUtils.checkGtidMode(endpoint);
        String binlogTransactionDependency = MySqlUtils.checkBinlogTransactionDependency(endpoint);
        List<String> approvedTruncateList = MySqlUtils.checkApprovedTruncateTableList(endpoint,true);
        res.put("noOnUpdate", String.join(", ", tablesWithOutOnUpdate));
        res.put("noOnUpdateKey", String.join(", ", tablesWithOutOnUpdateKey));
        res.put("noPkUk", String.join(", ", tablesWithOutPkOrUk));
        res.put("gtidMode", gtidMode);
        res.put("binlogTransactionDependency", binlogTransactionDependency);
        res.put("approvedTruncatelist", String.join(", ", approvedTruncateList));
        return res;
    }

    protected Map<String, String> getUsersAndPasswords(JsonNode root) {
        Map<String, String> usersAndPasswords = new HashMap<>();

        if (null != root) {
            JsonNode readUserNode = root.get(READ_USER_KEY);
            JsonNode readPasswordNode = root.get(READ_PASSWORD_KEY);
            JsonNode writeUserNode = root.get(WRITE_USER_KEY);
            JsonNode writePasswordNode = root.get(WRITE_PASSWORD_KEY);
            JsonNode monitorUserNode = root.get(MONITOR_USER_KEY);
            JsonNode monitorPasswordNode = root.get(MONITOR_PASSWORD_KEY);
            if (null != readUserNode && null != readPasswordNode && null != writeUserNode && null != writePasswordNode && null != monitorUserNode && null != monitorPasswordNode) {
                logger.info("use user and password from DBA");
                usersAndPasswords.put(READ_USER_KEY, readUserNode.asText());
                usersAndPasswords.put(READ_PASSWORD_KEY, readPasswordNode.asText());
                usersAndPasswords.put(WRITE_USER_KEY, writeUserNode.asText());
                usersAndPasswords.put(WRITE_PASSWORD_KEY, writePasswordNode.asText());
                usersAndPasswords.put(MONITOR_USER_KEY, monitorUserNode.asText());
                usersAndPasswords.put(MONITOR_PASSWORD_KEY, monitorPasswordNode.asText());
                return usersAndPasswords;
            }
        }

        usersAndPasswords.put(READ_USER_KEY, monitorTableSourceProvider.getReadUserVal());
        usersAndPasswords.put(READ_PASSWORD_KEY, monitorTableSourceProvider.getReadPasswordVal());
        usersAndPasswords.put(WRITE_USER_KEY, monitorTableSourceProvider.getWriteUserVal());
        usersAndPasswords.put(WRITE_PASSWORD_KEY, monitorTableSourceProvider.getWritePasswordVal());
        usersAndPasswords.put(MONITOR_USER_KEY, monitorTableSourceProvider.getMonitorUserVal());
        usersAndPasswords.put(MONITOR_PASSWORD_KEY, monitorTableSourceProvider.getMonitorPasswordVal());
        return usersAndPasswords;
    }

    protected String generateRequestBody(String rawRequestBody, String... keys) {
        try {
            JsonNode rawRequestNode = objectMapper.readTree(rawRequestBody);
            ObjectNode requestBodyNode = objectMapper.createObjectNode();
            for (String key : keys) {
                requestBodyNode.put(key, rawRequestNode.get(key).asText());
            }
            String requestBody = objectMapper.writeValueAsString(requestBodyNode);
            logger.info("raw requestBody: {}, generated requestBody: {}", rawRequestBody, requestBody);
            return requestBody;
        } catch (Exception e) {
            logger.error("Fail generate requestbody from {}", rawRequestBody, e);
            return rawRequestBody;
        }
    }
}
