package com.ctrip.framework.drc.console.config;

import com.ctrip.framework.drc.console.enums.log.CflBlacklistType;
import com.ctrip.framework.drc.core.utils.EncryptUtils;
import com.ctrip.xpipe.api.codec.GenericTypeReference;
import com.ctrip.xpipe.codec.JsonCodec;
import com.ctrip.xpipe.config.AbstractConfigBean;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.stream.Collectors;

/**
 * @ClassName DomainConifg
 * @Author haodongPan
 * @Date 2021/12/9 20:39
 * @Version: $
 */
@Component
public class DomainConfig extends AbstractConfigBean {
    
    @Autowired
    private DefaultConsoleConfig consoleConfig;
    
    private static final String MYSQL_API_URL = "mysql.api.url";
    private static final String MYSQL_API_URL_V2 = "mysql.api.url.v2";
    private static final String DEFAULT_MYSQL_API_URL = "http://localhost:8080/mysqlapi/";
    private static final String DEFAULT_MYSQL_API_URL_V2 = "http://localhost:8080/mysqlapi/";

    private static final String DAL_SERVICE_PREFIX = "dal.service.prefix";
    private static final String DEFAULT_DAL_SERVICE_PREFIX = "http://localhost:8080/database/";

    private static final String DAL_INSTANCEGROUPS_URL = "dal.instancegroups.url";
    private static final String DEFAULT_DAL_INSTANCEGROUPS_URL = "http://localhost:8080/database/instanceGroups?operator=DRCConsole";


    private static final String DAL_CLUSTER_URL = "dal.cluster.url";
    private static final String DEFAULT_DAL_CLUSTER_URL = "http://localhost:8080/dalcluster/";

    private static final String CMS_GET_SERVER_URL = "cms.get.server";
    private static final String DEFAULT_CMS_GET_SERVER_URL = "http://localhost:8080/ops/getFATServers";
    private static final String OPS_ACCESS_TOKEN = "ops.access.token";
    private static final String DEFAULT_OPS_ACCESS_TOKEN = "";
    private static final String DBAAPI_OPS_ACCESS_TOKEN = "dba.ops.access.token";

    private static final String CMS_GET_DB_INFO_URL = "cms.get.db.info.url";
    private static final String DEFAULT_CMS_GET_DB_INFO_URL = "http://localhost:8080/cms/getAllDbInfo";
    private static final String CMS_GET_BU_INFO_URL = "cms.get.bu.info.url";
    private static final String DEFAULT_CMS_GET_BU_INFO_URL = "http://localhost:8080/cms/getAllBuInfo";

    private static final String TRAFFIC_FROM_HICK_WALL_URL = "traffic.from.hick.wall.url";
    private static final String DEFAULT_TRAFFIC_FROM_HICK_WALL_URL = "http://osg.ops.ctripcorp.com/api/22853";

    private static final String QMQ_APPLICATION_URL = "qmq.application.url";
    private static final String QMQ_API_TOKEN_KEY = "qmq.http.api.token";
    private static final String TOPIC_SUFFIX = "/api/subject/save";
    private static final String PRODUCER_SUFFIX = "/api/producer/save";
    private static final String BU_SUFFIX = "/api/producer/getBuList";

    // QConfig
    private static String QCONFIG_REGION_IDCS_MAP = "qconfig.region.idcs.map";
    private static String DC_QCONFIG_SUBENV_MAP = "dc.qconfig.subenv.map";
    private static String QCONFIG_REST_API_URL = "qconfig.rest.api.url";
    private static String QCONFIG_API_TOKEN = "qconfig.api.token";

    private static final String QCONFIG_API_CONSOLE_TOKEN = "qconfig.api.console.token";
    private static final String ROWS_FILTER_WHITELIST_TARGET_SUB_ENV = "rows.filter.whitelist.targetSubenv";
    private static final String ROWS_FILTER_WHITELIST_TARGET_GROUP_ID = "rows.filter.whitelist.targetGroupId";

    private static final String DEFAULT_OPS_APPROVAL_URL = "http://osg.ops.ctripcorp.com/api/11102";
    private static final String DEFAULT_APPROVAL_DETAIL_URL = "http://rc.ops.ctripcorp.com/#/approvalcenter/approve-detail?ticketId=";
    private static final String OPS_APPROVAL_URL = "ops.approval.url";
    private static final String OPS_APPROVAL_TOKEN = "aps.approval.token";
    private static final String CONFLICT_APPROVE_CC_EMAIL = "conflict_cc_email";
    private static final String APPROVAL_CALLBACK_URL = "approval.callback.url";
    private static final String APPROVAL_DETAIL_URL = "approval.detail.url";
    private static final String CONFLICT_DETAIL_URL = "conflict.detail.url";
    private static final String DBA_APPROVERS = "dba.approvers";

    private static final String DOT_TOKEN = "dot.token";
    private static final String DOT_QUERY_API_URL = "dot.query.api.url";

    
    private static final String CFL_ALARM_SEND_EMAIL_SWITCH = "cfl.alarm.send.email.switch";
    private static final String CFL_ALARM_SEND_DB_OWNER_SWITCH = "cfl.alarm.send.db.owner.switch";
    private static final String CFL_ALARM_SENDER_EMAIL = "cfl.alarm.sender.email";
    private static final String CFL_ALARM_CC_EMAILS = "cfl.alarm.cc.emails";
    private static final String CFL_ALARM_DRC_URL= "cfl.alarm.drc.url";
    private static final String CFL_ALARM_HICKWALL_URL = "cfl.alarm.hickwall.url";
    private static final String CFL_ADD_BLACKLIST_URL = "cfl.add.blacklist.url";
    private static final String CFL_USER_DOCUMENT_URL = "cfl.user.document.url";

    private static final String CFL_ALARM_THRESHOLD_COMMIT_ROW = "cfl.alarm.threshold.commit.row";
    private static final String CFL_ALARM_THRESHOLD_COMMIT_TRX = "cfl.alarm.threshold.commit.trx";
    private static final String CFL_ALARM_THRESHOLD_ROLLBACK_ROW = "cfl.alarm.threshold.rollback.row";
    private static final String CFL_ALARM_THRESHOLD_ROLLBACK_TRX = "cfl.alarm.threshold.rollback.trx";
    
    private static final String CFL_ALARM_LIMIT_PER_HOUR = "cfl.alarm.limit.per.hour";
    private static final String CFL_BLACKLIST_ALARM_HOTSPOT_THRESHOLD = "cfl.blacklist.alarm.hotspot.threshold";
    private static final String CFL_BLACKLIST_SWITCH_FORMATTER = "cfl.blacklist.%s.clear.switch";
    private static final String CFL_BLACKLIST_EXPIRATION_HOUR_FORMATTER = "cfl.blacklist.%s.expiration.hour";

    private static String CFL_ALARM_TOP_NUM = "cfl.alarm.top.num";
    private static String CFL_ALARM_ROLLBACK_TOP_NUM = "cfl.alarm.rollback.top.num";
    private static String CFL_ALARM_SEND_TIME_HOUR = "cfl.alarm.send.time.hour";

    private static final String DRC_CONFIG_EMAIL_SEND_SWITCH = "drc.config.send.email.switch";
    private static final String DRC_CONFIG_SENDER_EMAIL = "drc.config.sender.email";
    private static final String DRC_CONFIG_CC_EMAIL = "drc.config.cc.email";
    private static final String DRC_CONFIG_DBA_EMAIL = "drc.config.dba.email";

    private static final String CENTER_REGION_USER_DML_COUNT_QUERY_TOKEN = "center.region.user.dml.count.query.token";
    private static final String CENTER_REGION_USER_DML_COUNT_QUERY_URL = "center.region.user.dml.count.query.url";
    private static final String OVERSEA_USER_DML_QUERY_TOKEN = "oversea.user.dml.query.token";
    private static final String OVERSEA_USER_DML_QUERY_URL = "oversea.user.dml.query.url";
    

    public String getDbaApprovers() {
        return getProperty(DBA_APPROVERS);
    }

    public String getApprovalDetailUrl() {
        return getProperty(APPROVAL_DETAIL_URL, DEFAULT_APPROVAL_DETAIL_URL);
    }

    public String getConflictDetailUrl() {
        return getProperty(CONFLICT_DETAIL_URL, "");
    }

    public String getOpsApprovalUrl() {
        return getProperty(OPS_APPROVAL_URL, DEFAULT_OPS_APPROVAL_URL);
    }

    public String getOpsApprovalToken() {
        String rawToken = getProperty(OPS_APPROVAL_TOKEN, "");
        return EncryptUtils.decryptRawToken(rawToken);
    }

    public String getConflictApproveCcEmail() {
        return getProperty(CONFLICT_APPROVE_CC_EMAIL);
    }

    public String getApprovalCallbackUrl() {
        return getProperty(APPROVAL_CALLBACK_URL);
    }

    public String getCmsGetServerUrl() {
        return getProperty(CMS_GET_SERVER_URL, DEFAULT_CMS_GET_SERVER_URL);
    }

    public String getDalServicePrefix() {
        return getProperty(DAL_SERVICE_PREFIX, DEFAULT_DAL_SERVICE_PREFIX);
    }

    public String getDalInstanceGroupsUrl() {
        return getProperty(DAL_INSTANCEGROUPS_URL, DEFAULT_DAL_INSTANCEGROUPS_URL);
    }

    public String getDalClusterUrl() {
        return getProperty(DAL_CLUSTER_URL, DEFAULT_DAL_CLUSTER_URL);
    }

    public String getOpsAccessToken() {
        return EncryptUtils.decryptRawToken(getProperty(OPS_ACCESS_TOKEN, DEFAULT_OPS_ACCESS_TOKEN));
    }

    public String getDBAApiOpsAccessToken() {
        String opsToken = getOpsAccessToken();
        String token = getProperty(DBAAPI_OPS_ACCESS_TOKEN, opsToken);
        return EncryptUtils.decryptRawToken(token);
    }

    public String getCmsGetDbInfoUrl() {
        return getProperty(CMS_GET_DB_INFO_URL, DEFAULT_CMS_GET_DB_INFO_URL);
    }

    public String getCmsGetBuInfoUrl() {
        return getProperty(CMS_GET_BU_INFO_URL, DEFAULT_CMS_GET_BU_INFO_URL);
    }

    public String getTrafficFromHickWall() {
        return getProperty(TRAFFIC_FROM_HICK_WALL_URL, DEFAULT_TRAFFIC_FROM_HICK_WALL_URL);
    }

    public String getQmqBuListUrl() {
        String qmqUrl = getQmqUrlByRegion("sha");
        return qmqUrl + BU_SUFFIX;
    }

    public String getQmqTopicApplicationUrl(String dc) {
        String region = consoleConfig.getRegionForDc(dc);
        String qmqUrl = getQmqUrlByRegion(region);
        return qmqUrl + TOPIC_SUFFIX;
    }

    public String getQmqProducerApplicationUrl(String dc) {
        String region = consoleConfig.getRegionForDc(dc);
        String qmqUrl = getQmqUrlByRegion(region);
        return qmqUrl + PRODUCER_SUFFIX;
    }
    
    public String getQmqUrlByRegion(String region) {
        Map<String, String> qmqUrls = JsonCodec.INSTANCE.decode(getProperty(QMQ_APPLICATION_URL, "{}"),
                new GenericTypeReference<Map<String, String>>() {
                });
        return qmqUrls.get(region);
    }

    public Map<String, String> getDc2QConfigSubEnvMap() {
        String mapString = getProperty(DC_QCONFIG_SUBENV_MAP, "{}");
        return JsonCodec.INSTANCE.decode(mapString, new GenericTypeReference<Map<String, String>>() {
        });
    }

    public String getQmqApiToken(){
        String rawToken =  getProperty(QMQ_API_TOKEN_KEY, "");
        return EncryptUtils.decryptRawToken(rawToken);
    }

    public String getQConfigRestApiUrl() {
        return getProperty(QCONFIG_REST_API_URL, "");
    }

    public String getQConfigAPIToken() {
        String rawToken = getProperty(QCONFIG_API_TOKEN, "");
        return EncryptUtils.decryptRawToken(rawToken);
    }

    public String getQConfigApiConsoleToken() {
        String rawToken = getProperty(QCONFIG_API_CONSOLE_TOKEN, "");
        return EncryptUtils.decryptRawToken(rawToken);
    }

    public String getWhitelistTargetGroupId() {
        return getProperty(ROWS_FILTER_WHITELIST_TARGET_GROUP_ID, "");
    }

    public List<String> getWhiteListTargetSubEnv() {
        String targetSubEnvStr = getProperty(ROWS_FILTER_WHITELIST_TARGET_SUB_ENV, "");
        if (StringUtils.isBlank(targetSubEnvStr)) {
            return new ArrayList<>();
        }
        return Arrays.stream(targetSubEnvStr.split(",")).collect(Collectors.toList());
    }
    
    
    // QConfig Region to IDCs  Mapping
    public Map<String, Set<String>> getRegion2IDCsMapping() {
        String regionsInfo = getProperty(QCONFIG_REGION_IDCS_MAP, "{}");
        if (StringUtils.isEmpty(regionsInfo)) {
            return Maps.newHashMap();
        } else {
            return JsonCodec.INSTANCE.decode(regionsInfo, new GenericTypeReference<Map<String, Set<String>>>() {
            });
        }
    }

    public Set<String> getIDCsInSameRegion(String dc) {
        Map<String, Set<String>> region2IDCsMapping = getRegion2IDCsMapping();
        for (Map.Entry<String, Set<String>> entry : region2IDCsMapping.entrySet()) {
            Set<String> idcs = entry.getValue();
            if (idcs.contains(dc)) {
                return idcs;
            }
        }
        return Sets.newHashSet();
    }

    public String getMysqlApiUrl() {
        return getProperty(MYSQL_API_URL, DEFAULT_MYSQL_API_URL);
    }

    public String getMysqlApiUrlV2() {
        return getProperty(MYSQL_API_URL_V2, DEFAULT_MYSQL_API_URL_V2);
    }

    public String getDotToken() {
        return EncryptUtils.decryptRawToken(getProperty(DOT_TOKEN, ""));
    }

    public String getDotQueryApiUrl() {
        return getProperty(DOT_QUERY_API_URL, "");
    }
    
    public boolean getConflictAlarmSendEmailSwitch() {
        return getBooleanProperty(CFL_ALARM_SEND_EMAIL_SWITCH, false);
    }
    
    public boolean getConflictAlarmSendDBOwnerSwitch() {
        return getBooleanProperty(CFL_ALARM_SEND_DB_OWNER_SWITCH, false);
    }
    
    public String getConflictAlarmSenderEmail() {
        return getProperty(CFL_ALARM_SENDER_EMAIL, "");
    }
    
    public List<String> getConflictAlarmCCEmails() {
        String ccEmails = getProperty(CFL_ALARM_CC_EMAILS, "");
        if (StringUtils.isBlank(ccEmails)) {
            return new ArrayList<>();
        } else {
            return Arrays.stream(ccEmails.split(",")).collect(Collectors.toList());
        }
    }
    
    public String getConflictAlarmDrcUrl() {
        return getProperty(CFL_ALARM_DRC_URL, "");
    }
    
    public String getConflictAlarmHickwallUrl() {
        return getProperty(CFL_ALARM_HICKWALL_URL, "");
    }

    public String getCflAddBlacklistUrl() {
        return getProperty(CFL_ADD_BLACKLIST_URL, "");
    }

    public String getCflUserDocumentUrl() {
        return getProperty(CFL_USER_DOCUMENT_URL, "");
    }

    public long getConflictAlarmThresholdCommitRow() {
        return getLongProperty(CFL_ALARM_THRESHOLD_COMMIT_ROW,60*1000L);
    }
    
    public long getConflictAlarmThresholdRollbackRow() {
        return getLongProperty(CFL_ALARM_THRESHOLD_ROLLBACK_ROW,100L);
    }
    
    public long getConflictAlarmThresholdCommitTrx() {
        return getLongProperty(CFL_ALARM_THRESHOLD_COMMIT_TRX,60*100L);
    }
    
    public long getConflictAlarmThresholdRollbackTrx() {
        return getLongProperty(CFL_ALARM_THRESHOLD_ROLLBACK_TRX,10L);
    }

    public int getConflictAlarmTopNum() {
        return getIntProperty(CFL_ALARM_TOP_NUM, 10);
    }

    public int getConflictAlarmRollbackTopNum() {
        return getIntProperty(CFL_ALARM_ROLLBACK_TOP_NUM, 10);
    }

    public int getConflictAlarmSendTimeHour() {
        return getIntProperty(CFL_ALARM_SEND_TIME_HOUR, 10);
    }
    
    public int getConflictAlarmLimitPerHour() {
        return getIntProperty(CFL_ALARM_LIMIT_PER_HOUR, 4);
    }

    public long getBlacklistAlarmHotspotThreshold() {
        return getLongProperty(CFL_BLACKLIST_ALARM_HOTSPOT_THRESHOLD, 6 * 60L);
    }

    public boolean getDrcConfigEmailSendSwitch() {
        return getBooleanProperty(DRC_CONFIG_EMAIL_SEND_SWITCH, false);
    }

    public String getDrcConfigSenderEmail() {
        return getProperty(DRC_CONFIG_SENDER_EMAIL, "");
    }

    public String getDrcConfigDbaEmail() {
        return getProperty(DRC_CONFIG_DBA_EMAIL, "");
    }

    public List<String> getDrcConfigCcEmail() {
        String ccEmails = getProperty(DRC_CONFIG_CC_EMAIL, "");
        if (StringUtils.isBlank(ccEmails)) {
            return new ArrayList<>();
        } else {
            return Arrays.stream(ccEmails.split(",")).collect(Collectors.toList());
        }
    }

    public boolean getBlacklistClearSwitch(CflBlacklistType type) {
        String key = type.name().toLowerCase().replaceAll("_", ".");
        return getBooleanProperty(String.format(CFL_BLACKLIST_SWITCH_FORMATTER,key), false);
    }

    public int getBlacklistExpirationHour(CflBlacklistType type) {
        String key = type.name().toLowerCase().replaceAll("_", ".");
        return getIntProperty(String.format(CFL_BLACKLIST_EXPIRATION_HOUR_FORMATTER,key), 24);
    }

    public String getCenterRegionUserDMLCountQueryToken() {
        return getProperty(CENTER_REGION_USER_DML_COUNT_QUERY_TOKEN, "");
    }

    public String getCenterRegionUserDMLCountQueryUrl() {
        return getProperty(CENTER_REGION_USER_DML_COUNT_QUERY_URL, "");
    }

    public String getOverSeaUserDMLQueryToken() {
        return getProperty(OVERSEA_USER_DML_QUERY_TOKEN, "");
    }

    public String getOverSeaUserDMLQueryUrl() {
        return getProperty(OVERSEA_USER_DML_QUERY_URL, "");
    }


}
