package com.ctrip.framework.drc.monitor.module.config;

import com.ctrip.framework.drc.core.server.config.SystemConfig;
import com.ctrip.xpipe.lifecycle.AbstractLifecycle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.Socket;

/**
 * Created by mingdongli
 * 2019/10/11 上午11:50.
 */
public abstract class AbstractConfigTest extends AbstractLifecycle {

    static {
        System.setProperty("host.ip", "127.0.0.1");  //设置本机IP
    }

    protected final Logger logger = LoggerFactory.getLogger(this.getClass());

    public static final String LOCALHOST = SystemConfig.LOCAL_SERVER_ADDRESS;

    public static final String SOURCE_MASTER_IP = LOCALHOST;

    public static final int BASE_PORT = 3306;

    public static final int SOURCE_MASTER_PORT = available(BASE_PORT);

    public static final int DESTINATION_MASTER_PORT = SOURCE_MASTER_PORT + 1;
    
    public static final int META_PORT = DESTINATION_MASTER_PORT + 1;

    public static final String USER = SystemConfig.MYSQL_USER_NAME;

    public static final String PASSWORD = SystemConfig.MYSQL_PASSWORD;

    public static final String REGISTRY_KEY = SystemConfig.INTEGRITY_TEST;

    public static final String MHA_NAME = "mhaName";

//    public static final String DESTINATION_REVERSE = SystemConfig.INTEGRITY_TEST;
    public static final String DESTINATION_REVERSE = SystemConfig.INTEGRITY_TEST + "_reverse";

    public static final int BASE_MASTER_PORT = 8383;

    public static final int REPLICATOR_MASTER_PORT = available(BASE_MASTER_PORT);

    public static boolean isUsed(int port) {
        try (Socket ignored = new Socket(LOCALHOST, port)) {
            return false;
        } catch (IOException e) {
            return true;
        }
    }

    private static int available(int port) {
        while (true) {
            try (Socket ignored = new Socket(LOCALHOST, port)) {
                port++;
            } catch (IOException ignored) {
                break;  //true
            }
        }
        return port;
    }

}
