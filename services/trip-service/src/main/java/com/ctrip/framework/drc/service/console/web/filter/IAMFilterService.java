package com.ctrip.framework.drc.service.console.web.filter;


import com.ctrip.framework.drc.service.config.QConfig;
import com.ctrip.xpipe.api.config.ConfigChangeListener;
import java.util.Set;

/**
 * @ClassName IAMFilterService
 * @Author haodongPan
 * @Date 2023/10/23 17:16
 * @Version: $
 */
public class IAMFilterService implements ConfigChangeListener {

    private static final String CONFIG_FILE_NAME = "iamfilter.properties";
    private QConfig qConfig;
    private Set<String> apiPrefixSet;
    
    // config keys & default values
    private static final String IAM_FILTER_SWITCH = "iam.filter.switch";

    public IAMFilterService() {
        if ("off".equalsIgnoreCase(System.getProperty("iam.config.enable"))) { // for test
            return;
        } 
        qConfig = new QConfig(CONFIG_FILE_NAME);
        qConfig.addConfigChangeListener(this);
        cacheAllKey();
    }
    
    @Override
    public void onChange(String key, String oldValue, String newValue) {
        cacheAllKey();
    }
    
    public String getApiPermissionCode(String requestURL) {
        String api = removeDomain(requestURL);
        String matchedKey = null;
        for (String apiPrefix : apiPrefixSet) {
            if (apiPrefix.endsWith("*")) {
                if (api.startsWith(apiPrefix.substring(0, apiPrefix.length() - 1))) {
                    matchedKey = apiPrefix;
                    break;
                }
            } else {
                if (api.equals(apiPrefix)) {
                    matchedKey = apiPrefix;
                    break;
                }
            }
            
        }
        return matchedKey == null ? null : qConfig.get(matchedKey,null);
    }
    
    public boolean iamFilterEnable() {
        String iamFilterSwitch = qConfig.get(IAM_FILTER_SWITCH, "off");
        return iamFilterSwitch.equalsIgnoreCase("on");
    }
    
    private String removeDomain(String requestURL) { 
        return requestURL.substring(requestURL.indexOf("/", 8)); // http:// or https:// -> 8
    }

    private void cacheAllKey() {
        apiPrefixSet = qConfig.getKeys();
    }
}
