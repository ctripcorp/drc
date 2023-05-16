package com.ctrip.framework.drc.console.service.assistant;

import com.ctrip.framework.drc.console.param.filter.QConfigQueryParam;
import com.ctrip.framework.drc.console.utils.EnvUtils;
import com.ctrip.framework.foundation.Foundation;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Created by dengquanliang
 * 2023/5/11 16:33
 */
public class RowsFilterServiceAssistant {

    private static final String CONFIG_NAME = "drc.properties";
    private static final int CONFIG_SPLIT_LENGTH = 2;

    public static QConfigQueryParam buildQueryParam(String subEnv, String token, String targetGroupId) {
        QConfigQueryParam queryParam = new QConfigQueryParam();
        queryParam.setToken(token);
        queryParam.setGroupId(Foundation.app().getAppId());
        queryParam.setDataId(CONFIG_NAME);
        queryParam.setEnv(EnvUtils.getEnv());
        queryParam.setSubEnv(subEnv);
        queryParam.setTargetGroupId(targetGroupId);

        return queryParam;
    }

    public static List<String> content2Whitelist(String content, String configKey) {
        String configValue = getConfigValue(content, configKey);
        if (StringUtils.isBlank(configValue)) {
            return new ArrayList<>();
        }
        return Arrays.stream(configValue.split(",")).collect(Collectors.toList());
    }

    public static String getConfigValue(String content, String configKey) {
        Map<String, String> configMap = string2Map(content);
        String configValue = configMap.getOrDefault(configKey, "");

        return configValue;
    }

    private static Map<String, String> string2Map(String content) {
        Map<String, String> configMap = Maps.newLinkedHashMap();
        if (StringUtils.isEmpty(content)) {
            return configMap;
        }
        String[] configs = content.split("\n");
        for (int i = 0; i < configs.length; i++) {
            String[] entry = configs[i].split("=");
            if (entry.length == CONFIG_SPLIT_LENGTH) {
                configMap.put(entry[0], entry[1]);
            }
        }
        return configMap;
    }
}
