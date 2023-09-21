package com.ctrip.framework.drc.replicator.impl.inbound.schema;

import com.ctrip.xpipe.api.codec.GenericTypeReference;
import com.ctrip.xpipe.codec.JsonCodec;
import com.ctrip.xpipe.config.AbstractConfigBean;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.StringUtils;

import java.util.HashMap;
import java.util.Map;

/**
 * @Author limingdong
 * @create 2022/4/26
 */
public class MySQLVariablesConfiguration extends AbstractConfigBean {

    private static final String MYSQL_STRING_VARIABLES = "embedded.mysql.string.%s";

    private static final String MYSQL_INT_VARIABLES = "embedded.mysql.int.%s";

    private static final String MYSQL_BOOLEAN_VARIABLES = "embedded.mysql.boolean.%s";

    private MySQLVariablesConfiguration() {}

    private static class MySQLVariablesConfigurationHolder {
        public static final MySQLVariablesConfiguration INSTANCE = new MySQLVariablesConfiguration();
    }

    public static MySQLVariablesConfiguration getInstance() {
        return MySQLVariablesConfigurationHolder.INSTANCE;
    }

    public Map<String, Object> getVariables(String key) {
        Map<String, Object> variableMap = new HashMap<>();
        getDefaultVariables(variableMap);
        getVariables(variableMap, String.format(MYSQL_STRING_VARIABLES, key), String.class);
        getVariables(variableMap, String.format(MYSQL_INT_VARIABLES, key), Integer.class);
        getVariables(variableMap, String.format(MYSQL_BOOLEAN_VARIABLES, key), Boolean.class);
        return variableMap;
    }

    private void getVariables(Map<String, Object> variableMap, String key, Class<?> clazz) {
        String value = getProperty(key);
        if (StringUtils.isBlank(value)) {
            return;
        }
        if (clazz.isAssignableFrom(String.class)) {
            Map<String, String> map = JsonCodec.INSTANCE.decode(value, new GenericTypeReference<>() {});
            variableMap.putAll(map);
        } else if (clazz.isAssignableFrom(Integer.class)) {
            Map<String, Integer> map = JsonCodec.INSTANCE.decode(value, new GenericTypeReference<>() {});
            variableMap.putAll(map);
        } else if (clazz.isAssignableFrom(Boolean.class)) {
            Map<String, Boolean> map = JsonCodec.INSTANCE.decode(value, new GenericTypeReference<>() {});
            variableMap.putAll(map);
        }
    }

    private void getDefaultVariables(Map<String, Object> variableMap) {
        Map<String, String> map = Maps.newHashMap();
//        map.put("sql_mode", "NO_AUTO_CREATE_USER");
        variableMap.putAll(map);
    }
}
