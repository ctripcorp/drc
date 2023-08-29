package com.ctrip.framework.drc.console.config;

import com.ctrip.xpipe.api.codec.GenericTypeReference;
import com.ctrip.xpipe.codec.JsonCodec;
import com.ctrip.xpipe.config.AbstractConfigBean;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import java.util.*;

import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.stream.Collectors;

/**
 * @ClassName DomainConifg
 * @Author haodongPan
 * @Date 2021/12/9 20:39
 * @Version: $
 */
@Component
public class DomainConfig extends AbstractConfigBean {
    
    @Autowired private DefaultConsoleConfig consoleConfig;

    private static final String DAL_SERVICE_PREFIX = "dal.service.prefix";
    private static final String DEFAULT_DAL_SERVICE_PREFIX = "http://localhost:8080/database/";

    private static final String DAL_REGISTER_PREFIX = "dal.register.prefix";
    private static final String DEFAULT_DAL_REGISTER_PREFIX = "http://localhost:8080/api/dal/v2/";

    private static final String BEACON_PREFIX = "beacon.prefix";
    private static final String DEFAULT_BEACON_PREFIX = "http://localhost:8080/api/v1/";

    private static final String DAL_CLUSTER_URL = "dal.cluster.url";
    private static final String DEFAULT_DAL_CLUSTER_URL = "http://localhost:8080/dalcluster/";

    private static final String MYSQL_PRECHECK_URL ="mysql.precheck.url";
    private static final String DEFAULT_MYSQL_PRECHECK_URL ="http://localhost:8080/mysqltool/precheck/";

    private static final String BUILD_NEW_CLUSTER_URL ="build.newcluster.url";
    private static final String DEFAULT_BUILD_NEW_CLUSTER_URL ="http://localhost:8080/mysqltool/buildnewCluster/";

    private static final String SLAVE_CASCADE_URL ="slave.cascade.url";
    private static final String DEFAULT_SLAVE_CASCADE_URL ="http://localhost:8080/mysqltool/slavecascade/";

    private static final String DNS_DEPLOY_URL ="dns.deploy.url";
    private static final String DEFAULT_DNS_DEPLOY_URL ="http://localhost:8080/mysqltool/dnsdeploy/";

    private static final String GET_ALL_CLUSTER_URL = "get.allcluster.url";
    private static final String DEFAULT_GET_ALL_CLUSTER_URL = "http://localhost:8080/ops/getallcluster";

    private static final String MYSQL_DB_CLUSTER_URL = "mysql.dbcluster.url";
    private static final String DEFAULT_MYSQL_DB_CLUSTER_URL = "http://localhost:8080/ops/mysqldbcluster";

    private static final String CMS_GET_SERVER_URL ="cms.get.server";
    private static final String DEFAULT_CMS_GET_SERVER_URL ="http://localhost:8080/ops/getFATServers";
    private static final String OPS_ACCESS_TOKEN = "ops.access.token";
    private static final String OPS_ACCESS_TOKEN_FAT = "ops.access.token.fat";
    private static final String DEFAULT_OPS_ACCESS_TOKEN = "";
    private static final String DEFAULT_OPS_ACCESS_FAT_TOKEN = "";

    private static final String CMS_GET_DB_INFO_URL = "cms.get.db.info.url";
    private static final String DEFAULT_CMS_GET_DB_INFO_URL = "http://localhost:8080/cms/getAllDbInfo";
    private static final String CMS_GET_BU_INFO_URL = "cms.get.bu.info.url";
    private static final String DEFAULT_CMS_GET_BU_INFO_URL = "http://localhost:8080/cms/getAllBuInfo";
    private static final String CMS_ACCESS_TOKEN = "cms.access.token";

    private static final String TRAFFIC_FROM_HICK_WALL_URL = "traffic.from.hick.wall.url";
    private static final String TRAFFIC_FROM_HICK_WALL_FAT_URL = "traffic.from.hick.wall.fat.url";
    private static final String DEFAULT_TRAFFIC_FROM_HICK_WALL_URL = "http://osg.ops.ctripcorp.com/api/22853";
    private static final String DEFAULT_TRAFFIC_FROM_HICK_WALL_FAT_URL = "http://uat.osg.ops.qa.nt.ctripcorp.com/api/19049";

    private static final String QMQ_APPLICATION_URL = "qmq.application.url";
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
    

    public String getCmsGetServerUrl() {
        return getProperty(CMS_GET_SERVER_URL,DEFAULT_CMS_GET_SERVER_URL);
    }

    public String getDalServicePrefix() {
        return getProperty(DAL_SERVICE_PREFIX,DEFAULT_DAL_SERVICE_PREFIX);
    }

    public String getDalRegisterPrefix() {
        return getProperty(DAL_REGISTER_PREFIX,DEFAULT_DAL_REGISTER_PREFIX);
    }

    public String getBeaconPrefix() {
        return getProperty(BEACON_PREFIX,DEFAULT_BEACON_PREFIX);
    }

    public String getDalClusterUrl() {
        return getProperty(DAL_CLUSTER_URL,DEFAULT_DAL_CLUSTER_URL);
    }

    public String getMysqlPrecheckUrl() {
        return getProperty(MYSQL_PRECHECK_URL,DEFAULT_MYSQL_PRECHECK_URL);
    }

    public String getBuildNewClusterUrl() {
        return getProperty(BUILD_NEW_CLUSTER_URL,DEFAULT_BUILD_NEW_CLUSTER_URL);
    }

    public String getSlaveCascadeUrl() {
        return getProperty(SLAVE_CASCADE_URL,DEFAULT_SLAVE_CASCADE_URL);
    }

    public String getDnsDeployUrl() {
        return getProperty(DNS_DEPLOY_URL,DEFAULT_DNS_DEPLOY_URL);
    }

    public String getGetAllClusterUrl() {
        return getProperty(GET_ALL_CLUSTER_URL,DEFAULT_GET_ALL_CLUSTER_URL);
    }

    public String getMysqlDbClusterUrl() {
        return getProperty(MYSQL_DB_CLUSTER_URL,DEFAULT_MYSQL_DB_CLUSTER_URL);
    }

    public String getOpsAccessToken() {
        return getProperty(OPS_ACCESS_TOKEN,DEFAULT_OPS_ACCESS_TOKEN);
    }

    public String getOpsAccessTokenFat() {
        return getProperty(OPS_ACCESS_TOKEN_FAT, DEFAULT_OPS_ACCESS_FAT_TOKEN);
    }

    public String getCmsGetDbInfoUrl() {
        return getProperty(CMS_GET_DB_INFO_URL,DEFAULT_CMS_GET_DB_INFO_URL);
    }

    public String getCmsGetBuInfoUrl() {
        return getProperty(CMS_GET_BU_INFO_URL,DEFAULT_CMS_GET_BU_INFO_URL);
    }

    public String getCmsAccessToken() {
        return getProperty(CMS_ACCESS_TOKEN,"");
    }

    public String getTrafficFromHickWall() {
        return getProperty(TRAFFIC_FROM_HICK_WALL_URL, DEFAULT_TRAFFIC_FROM_HICK_WALL_URL);
    }
    public String getTrafficFromHickWallFat() {
        return getProperty(TRAFFIC_FROM_HICK_WALL_FAT_URL, DEFAULT_TRAFFIC_FROM_HICK_WALL_FAT_URL);
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
                new GenericTypeReference<Map<String, String>>() {});
        return qmqUrls.get(region);
    }

    public Map<String,String> getDc2QConfigSubEnvMap() {
        String mapString = getProperty(DC_QCONFIG_SUBENV_MAP, "{}");
        return JsonCodec.INSTANCE.decode(mapString, new GenericTypeReference<Map<String, String>>() {});
    }

    public String getQConfigRestApiUrl() {
        return getProperty(QCONFIG_REST_API_URL,"");
    }

    public String  getQConfigAPIToken() {
        return getProperty(QCONFIG_API_TOKEN,"");
    }

    public String getQConfigApiConsoleToken () {
        return getProperty(QCONFIG_API_CONSOLE_TOKEN, "");
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
        String regionsInfo = getProperty(QCONFIG_REGION_IDCS_MAP,"{}");
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
}
