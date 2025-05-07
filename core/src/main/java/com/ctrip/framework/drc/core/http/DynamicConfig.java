package com.ctrip.framework.drc.core.http;

import com.ctrip.framework.drc.core.utils.EncryptUtils;
import com.ctrip.xpipe.config.AbstractConfigBean;

/**
 * @ClassName DynamicConfig
 * @Author haodongPan
 * @Date 2021/12/13 14:05
 * @Version: $
 */
public class DynamicConfig extends AbstractConfigBean { // drc.properties
    private static final String X_ACCESS_TOKEN = "x.access.token";
    private static final String DEFAULT_X_ACCESS_TOKEN = "";

    private static final String ACCOUNT_HERALD_FORCE_CHECK_SWITCH = "account.herald.force.check.switch";
    private static final String HERALD_SERVER_URL = "herald.server.url";
    private static final String HERALD_AUTH_TOKEN = "herald.auth.token";
    private static final String KAFKA_BOOTSTRAP_SERVERS = "kafka.bootstrap.servers";

    private DynamicConfig(){
        super();
    };


    public String getXAccessToken() {
        String rawToken =  getProperty(X_ACCESS_TOKEN,DEFAULT_X_ACCESS_TOKEN);
        return EncryptUtils.decryptRawToken(rawToken);
    }

    public boolean shouldForceHeraldTokenCheck() {
        return getBooleanProperty(ACCOUNT_HERALD_FORCE_CHECK_SWITCH, false);
    }

    public String getKafkaBootStrapServers() {
        return getProperty(KAFKA_BOOTSTRAP_SERVERS, null);
    }


    public static DynamicConfig getInstance(){
        return DynamicConfigHolder.INSTANCE;
    }

    public String getHeraldServerUrl() {
        return getProperty(HERALD_SERVER_URL);
    }

    public String getHeraldAuthToken() {
        return EncryptUtils.decryptRawToken(getProperty(HERALD_AUTH_TOKEN));
    }

    private static class DynamicConfigHolder {
        private static DynamicConfig INSTANCE = new DynamicConfig();
    }

}
