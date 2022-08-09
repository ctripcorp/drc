package com.ctrip.framework.drc.core.config;

import com.ctrip.framework.drc.core.http.AuthorityConfig;
import com.ctrip.xpipe.api.codec.GenericTypeReference;
import com.ctrip.xpipe.codec.JsonCodec;
import com.ctrip.xpipe.config.AbstractConfigBean;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.StringUtils;

import java.util.Map;
import java.util.Set;

/**
 * @ClassName CommonConfig
 * @Author haodongPan
 * @Date 2022/8/9 11:48
 * @Version: $
 * @Description: common config in console and cm
 */
public class CommonConfig extends AbstractConfigBean {
    
    private static final String KEY_LOCAL_REGION = "x.access.token";
    private static final String DEFAULT_REGION = "sha";
    
    public static String KEY_REGION_IDC_MAPPING = "drc.region.idc.mapping";
    public static String DEFAULT_REGION_IDC_MAPPING = "{\"sha\": [\"shaoy\", \"sharb\"], \"ntgxh\": [\"ntgxh\"], \"ntgxy\": [\"ntgxy\"]}";
    
    public static final String KEY_DRC_CM_REGION_INFOS= "drc.cm.region.infos";
    public static final String DEFAULT_DRC_CM_REGION_INFOS= "{\"sha\":\"http://cm.drc.sha\", \"ntgxh\":\"http://cm.drc.ntgxh\", \"ntgxy\":\"http://cm.drc.ntgxy\"}";

    public static final String KEY_DRC_CONSOLE_REGION_INFOS = "drc.console.region.infos";
    public static final String DEFAULT_DRC_CONSOLE_REGION_INFOS = "{\"sha\":\"http://console.drc.sha\", \"ntgxh\":\"http://console.drc.ntgxh\", \"ntgxy\":\"http://console.drc.ntgxy\"}";

    public String getRegion(){
        return getProperty(KEY_LOCAL_REGION,DEFAULT_REGION);
    }

    public Map<String, Set<String>> getRegion2dcsMapping(){
        String regionsInfo = getProperty(KEY_REGION_IDC_MAPPING, DEFAULT_REGION_IDC_MAPPING);
        if(StringUtils.isEmpty(regionsInfo)) {
            return Maps.newHashMap();
        } else {
            return JsonCodec.INSTANCE.decode(regionsInfo, new GenericTypeReference<Map<String, Set<String>>>() {});
        }
    }

    public Map<String,String> getDc2regionMap (){
        Map<String, Set<String>> regionsInfo = getRegion2dcsMapping();
        Map<String,String> dc2regionMap = Maps.newHashMap();
        regionsInfo.forEach(
                (region, dcs) -> dcs.forEach(dc -> dc2regionMap.put(dc, region))
        );
        return dc2regionMap;
    }

    public Map<String,String> getCMRegionUrls() {
        String cmUrlsStr = getProperty(KEY_DRC_CM_REGION_INFOS,DEFAULT_DRC_CM_REGION_INFOS);
        return JsonCodec.INSTANCE.decode(cmUrlsStr, new GenericTypeReference<Map<String, String>>() {});
    }

    public Map<String,String> getConsoleRegionUrls() {
        String cmUrlsStr = getProperty(KEY_DRC_CONSOLE_REGION_INFOS,DEFAULT_DRC_CONSOLE_REGION_INFOS);
        return JsonCodec.INSTANCE.decode(cmUrlsStr, new GenericTypeReference<Map<String, String>>() {});
    }
    
    
    public static CommonConfig getInstance() {
        return CommonConfigHolder.INSTANCE;
    }
    
    
    private CommonConfig() {
        super();
    }
    
    private static class CommonConfigHolder {
        private static final CommonConfig INSTANCE = new CommonConfig();
    }
    
    
}
