package com.ctrip.framework.drc.console.config;

import com.ctrip.framework.drc.console.config.meta.DcInfo;
import com.ctrip.xpipe.api.codec.GenericTypeReference;
import com.ctrip.xpipe.api.config.Config;
import com.ctrip.xpipe.codec.JsonCodec;
import com.ctrip.xpipe.config.AbstractConfigBean;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.springframework.stereotype.Component;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author shenhaibo
 * @version 1.0
 * date: 2020-07-02
 */
@Component("defaultConsoleConfig")
public class DefaultConsoleConfig extends AbstractConfigBean {

    public static String KEY_DC_INFOS = "drc.dcinfos";

    public static String DBA_DC_INFOS = "dba.dcinfos";

    public static String CONSOLE_DC_INFOS = "drc.console.dcinfos";

    public static String VALIDATION_DC_INFOS = "drc.validation.dcinfos";

    public static String CONSOLE_DC_ENDPOINT_INFOS = "drc.console.dcinfos.endpoint";

    public static String BEACON_PREFIX = "beacon.prefix";

    public static String UID_DATA_PREFIX = "uid.data.%s";

    public static String UCS_STRATEGY_PREFIX = "ucs.strategy.%s";

    private String defaultBeaconPrefix = "";

    public static String MHA_DAL_CLUSTER_INFOS = "mha.dalcluster.info";

    public static String DELAY_EXCEPTION_TIME = "delay.exception";

    public static String DEFAULT_DELAY_EXCEPTION_TIME = "864500000";

    private Map<String, String> dbaDcInfos = Maps.newConcurrentMap();

    private Map<String, String> consoleDcInfos = Maps.newConcurrentMap();

    private Map<String, String> validationDcInfos = Maps.newConcurrentMap();

    private Map<String, String> consoleDcEndpointInfos = Maps.newConcurrentMap();

    private String defaultDcInfos = "{}";

    private String defaultDbaDcInfos = "{}";

    private String defaultConsoleDcInfos = "{}";

    private String defaultValidationDcInfos = "{}";

    private String defaultMhaDalClusterInfos = "{}";

    private String DEFAULT_MAPPING_INFOS = "{}";

    private static String PUBLIC_CLOUD_DC = "drc.public.cloud.dc";
    private static String DEFAULT_PUBLIC_CLOUD_DC = "shali";

    private static String CONSOLE_GRAY_MHA = "console.gray.mha";
    private static String DEFAULT_CONSOLE_GRAY_MHA = "";

    private static String CONSOLE_GRAY_MHA_SWITCH = "console.gray.mha.switch";
    private static String DEFAULT_CONSOLE_GRAY_MHA_SWITCH = "on";

    public DefaultConsoleConfig(Config config) {
        super(config);
    }

    public DefaultConsoleConfig() {
    }

    public long getDelayExceptionTime() {
        String timeStr = getProperty(DELAY_EXCEPTION_TIME, DEFAULT_DELAY_EXCEPTION_TIME);
        return Long.parseLong(timeStr);
    }

    public Map<String, DcInfo> getDcInfos() {
        String dcInfoStr = getProperty(KEY_DC_INFOS, defaultDcInfos);
        logger.info("[[monitor=delay]] {}={}", KEY_DC_INFOS, dcInfoStr);
        Map<String, DcInfo> dcInfos = JsonCodec.INSTANCE.decode(dcInfoStr, new GenericTypeReference<Map<String, DcInfo>>() {});

        Map<String, DcInfo> result = Maps.newConcurrentMap();
        for(Map.Entry<String, DcInfo> entry : dcInfos.entrySet()){
            result.put(entry.getKey().toLowerCase(), entry.getValue());
        }

        logger.debug("[getDcInofs]{}", result);
        return result;
    }

    public Map<String, String> getDbaDcInfos() {
        if(dbaDcInfos.size() == 0) {
            dbaDcInfos = getDbaDcInfoMapping();
        }
        return dbaDcInfos;
    }

    public Map<String, String> getConsoleDcInfos() {
        if(consoleDcInfos.size() == 0) {
            consoleDcInfos = getConsoleDcInfoMapping();
        }
        return consoleDcInfos;
    }

    public Map<String, String> getValidationDcInfos() {
        if(validationDcInfos.size() == 0) {
            validationDcInfos = getValidationDcInfoMapping();
        }
        return validationDcInfos;
    }

    public Map<String, String> getConsoleDcEndpointInfos() {
        if(consoleDcEndpointInfos.size() == 0) {
            consoleDcEndpointInfos = getConsoleDcEndpointMapping();
        }
        return consoleDcEndpointInfos;
    }

    public void setDefaultDcInfos(String defaultDcInfos) {
        this.defaultDcInfos = defaultDcInfos;
    }

    public void setDefaultDbaDcInfos(String defaultDbaDcInfos) {
        this.defaultDbaDcInfos = defaultDbaDcInfos;
    }

    public void setDefaultConsoleDcInfos(String defaultConsoleDcInfos) {
        this.defaultConsoleDcInfos = defaultConsoleDcInfos;
    }

    public void setDefaultBeaconPrefix(String defaultBeaconPrefix) {
        this.defaultBeaconPrefix = defaultBeaconPrefix;
    }

    public String getBeaconPrefix() {
        return getProperty(BEACON_PREFIX, defaultBeaconPrefix);
    }

    public void setDefaultMhaDalClusterInfos(String defaultMhaDalClusterInfos) {
        this.defaultMhaDalClusterInfos = defaultMhaDalClusterInfos;
    }

    public String getCMMetaServerAddress(String dc) {
        Map<String, DcInfo> dcInfos = getDcInfos();
        DcInfo dcInfo = dcInfos.get(dc);
        if (dcInfo != null) {
            return dcInfo.getMetaServerAddress();
        }
        logger.warn("[getCMMetaServerAddress] not configured for {}", dc);
        return null;
    }

    private Map<String, String> getDbaDcInfoMapping() {

        String dcInfoStr = getProperty(DBA_DC_INFOS, defaultDbaDcInfos);
        logger.info("DBA dcInfos {}={}", DBA_DC_INFOS, dcInfoStr);
        Map<String, String> dbaDcInfos = JsonCodec.INSTANCE.decode(dcInfoStr, new GenericTypeReference<Map<String, String>>() {});

        Map<String, String> result = Maps.newConcurrentMap();
        for(Map.Entry<String, String> entry : dbaDcInfos.entrySet()){
            result.put(entry.getKey(), entry.getValue().toLowerCase());
        }

        logger.debug("[getDbaDcInofs]{}", result);
        return result;
    }

    public String getValidationDomain(String dc) {
        Map<String, String> validationDcInfos = getValidationDcInfoMapping();
        return validationDcInfos.get(dc);
    }

    private Map<String, String> getValidationDcInfoMapping() {
        String dcInfoStr = getProperty(VALIDATION_DC_INFOS, defaultValidationDcInfos);
        logger.info("validation dcInfos {}={}", VALIDATION_DC_INFOS, dcInfoStr);
        Map<String, String> validationDcInfos = JsonCodec.INSTANCE.decode(dcInfoStr, new GenericTypeReference<Map<String, String>>() {});

        Map<String, String> result = Maps.newConcurrentMap();
        for(Map.Entry<String, String> entry : validationDcInfos.entrySet()){
            result.put(entry.getKey(), entry.getValue().toLowerCase());
        }

        logger.debug("[getValidationDcInfoMapping]{}", result);
        return result;
    }

    private Map<String, String> getConsoleDcInfoMapping() {
        String dcInfoStr = getProperty(CONSOLE_DC_INFOS, defaultConsoleDcInfos);
        logger.info("console dcInfos {}={}", CONSOLE_DC_INFOS, dcInfoStr);
        Map<String, String> consoleDcInfos = JsonCodec.INSTANCE.decode(dcInfoStr, new GenericTypeReference<Map<String, String>>() {});

        Map<String, String> result = Maps.newConcurrentMap();
        for(Map.Entry<String, String> entry : consoleDcInfos.entrySet()){
            result.put(entry.getKey(), entry.getValue().toLowerCase());
        }

        logger.debug("[getConsoleDcInofs]{}", result);
        return result;
    }

    private Map<String, String> getConsoleDcEndpointMapping() {
        String dcEndpointInfoStr = getProperty(CONSOLE_DC_ENDPOINT_INFOS, defaultConsoleDcInfos);
        logger.info("console dcInfos {}={}", CONSOLE_DC_ENDPOINT_INFOS, dcEndpointInfoStr);
        Map<String, String> consoleDcInfos = JsonCodec.INSTANCE.decode(dcEndpointInfoStr, new GenericTypeReference<Map<String, String>>() {});

        Map<String, String> result = Maps.newConcurrentMap();
        for(Map.Entry<String, String> entry : consoleDcInfos.entrySet()){
            result.put(entry.getKey(), entry.getValue().toLowerCase());
        }

        logger.debug("[getConsoleDcInfos]{}", result);
        return result;
    }

    public Map<String, Integer> getUcsStrategyIdMap(String cluster) {
        String key = String.format(UCS_STRATEGY_PREFIX, cluster);
        String ucsStrategyIdMappingStr = getProperty(key, DEFAULT_MAPPING_INFOS);
        logger.info("ucs strategy mapping for {}: {}", cluster, ucsStrategyIdMappingStr);
        Map<String, Integer> ucsStrategyInfos = JsonCodec.INSTANCE.decode(ucsStrategyIdMappingStr, new GenericTypeReference<Map<String, Integer>>() {});

        Map<String, Integer> ucsStrategyIdMap = Maps.newHashMap();
        for(Map.Entry<String, Integer> entry : ucsStrategyInfos.entrySet()){
            ucsStrategyIdMap.put(entry.getKey().toLowerCase(), entry.getValue());
        }

        logger.debug("[uidNameMap]: {}", ucsStrategyIdMap);
        return ucsStrategyIdMap;
    }

    public Map<String, String> getUidMap(String cluster) {
        String key = String.format(UID_DATA_PREFIX, cluster);
        String uidNameMappingStr = getProperty(key, DEFAULT_MAPPING_INFOS);
        logger.info("uid name mapping for {}: {}", cluster, uidNameMappingStr);
        Map<String, String> uidNameInfos = JsonCodec.INSTANCE.decode(uidNameMappingStr, new GenericTypeReference<Map<String, String>>() {});

        Map<String, String> uidNameMap = Maps.newHashMap();
        for(Map.Entry<String, String> entry : uidNameInfos.entrySet()){
            uidNameMap.put(entry.getKey().toLowerCase(), entry.getValue().toLowerCase());
        }

        logger.debug("[uidNameMap]: {}", uidNameMap);
        return uidNameMap;
    }

    public Map<String, String> getMhaDalClusterInfoMapping() {
        String mhaDalClusterInfoStr = getProperty(MHA_DAL_CLUSTER_INFOS, defaultMhaDalClusterInfos);
        logger.info("mha dal cluster info {}={}", MHA_DAL_CLUSTER_INFOS, mhaDalClusterInfoStr);
        Map<String, String> mhaDalClusterInfos = JsonCodec.INSTANCE.decode(mhaDalClusterInfoStr, new GenericTypeReference<Map<String, String>>() {});

        Map<String, String> result = Maps.newConcurrentMap();
        for(Map.Entry<String, String> entry : mhaDalClusterInfos.entrySet()){
            result.put(entry.getKey(), entry.getValue().toLowerCase());
        }

        logger.debug("[getMhaDalClusterInfos]{}", result);
        return result;
    }

    public Set<String> getPublicCloudDc() {
        String publicCloudDc = getProperty(PUBLIC_CLOUD_DC, DEFAULT_PUBLIC_CLOUD_DC);
        logger.info("public cloud dc: {}", publicCloudDc);
        return Sets.newHashSet(publicCloudDc.split(","));
    }

    public Set<String> getGrayMha() {
        String grayMha = getProperty(CONSOLE_GRAY_MHA, DEFAULT_CONSOLE_GRAY_MHA);
        return Sets.newHashSet(grayMha.split(","));
    }

    public String getGrayMhaSwitch() {
        return getProperty(CONSOLE_GRAY_MHA_SWITCH, DEFAULT_CONSOLE_GRAY_MHA_SWITCH);
    }
}
