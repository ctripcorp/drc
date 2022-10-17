package com.ctrip.framework.drc.core.config;

import com.ctrip.xpipe.api.codec.GenericTypeReference;
import com.ctrip.xpipe.codec.JsonCodec;
import com.ctrip.xpipe.config.AbstractConfigBean;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.StringUtils;

import java.util.Map;
import java.util.Set;

/**
 * @ClassName RegionConfig
 * @Author haodongPan
 * @Date 2022/8/9 11:48
 * @Version: $
 * @Description: common config in console and cm
 */
public class RegionConfig extends AbstractConfigBean {

    private static final String KEY_LOCAL_REGION = "region";
    private static final String DEFAULT_REGION = "";

    public static String KEY_REGION_IDC_MAPPING = "drc.region.idc.mapping";
    public static String DEFAULT_REGION_IDC_MAPPING = "{}";

    public static final String KEY_DRC_CM_REGION_INFOS = "drc.cm.region.infos";
    public static final String DEFAULT_DRC_CM_REGION_INFOS = "{}";

    public static final String KEY_DRC_CONSOLE_REGION_INFOS = "drc.console.region.infos";
    public static final String DEFAULT_DRC_CONSOLE_REGION_INFOS = "{}";

    public String getRegion() {
        return getProperty(KEY_LOCAL_REGION, DEFAULT_REGION);
    }

    public Map<String, Set<String>> getRegion2dcsMapping() {
        String regionsInfo = getProperty(KEY_REGION_IDC_MAPPING, DEFAULT_REGION_IDC_MAPPING);
        if (StringUtils.isEmpty(regionsInfo)) {
            return Maps.newHashMap();
        } else {
            return JsonCodec.INSTANCE.decode(regionsInfo, new GenericTypeReference<Map<String, Set<String>>>() {
            });
        }
    }

    public Map<String, String> getDc2regionMap() {
        Map<String, Set<String>> regionsInfo = getRegion2dcsMapping();
        Map<String, String> dc2regionMap = Maps.newHashMap();
        regionsInfo.forEach(
                (region, dcs) -> dcs.forEach(dc -> dc2regionMap.put(dc, region))
        );
        return dc2regionMap;
    }

    public Map<String, String> getCMRegionUrls() {
        String cmUrlsStr = getProperty(KEY_DRC_CM_REGION_INFOS, DEFAULT_DRC_CM_REGION_INFOS);
        return JsonCodec.INSTANCE.decode(cmUrlsStr, new GenericTypeReference<Map<String, String>>() {
        });
    }

    public Map<String, String> getConsoleRegionUrls() {
        String cmUrlsStr = getProperty(KEY_DRC_CONSOLE_REGION_INFOS, DEFAULT_DRC_CONSOLE_REGION_INFOS);
        return JsonCodec.INSTANCE.decode(cmUrlsStr, new GenericTypeReference<Map<String, String>>() {
        });
    }

    public String getRegionForDc(String dc) {
        Map<String, String> dc2regionMap = getDc2regionMap();
        return dc2regionMap.get(dc);
    }
    

    public static RegionConfig getInstance() {
        return RegionConfigHolder.INSTANCE;
    }


    private RegionConfig() {
        super();
    }

    private static class RegionConfigHolder {
        private static final RegionConfig INSTANCE = new RegionConfig();
    }


}
