package com.ctrip.framework.drc.console.utils;

import com.ctrip.framework.foundation.Env;
import com.ctrip.framework.foundation.Foundation;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.util.Strings;

/**
 * Created by dengquanliang
 * 2023/4/24 16:06
 */
public class EnvUtils {
    private static final String FWS = "fws";
    private static final String FAT = "fat";
    private static final String PRO = "pro";

    public static String getEnvStr() {
        String env = Foundation.server().getEnv().getName().toLowerCase();
        if (StringUtils.isEmpty(env)) {
            return Strings.EMPTY;
        }
        if (FWS.equalsIgnoreCase(env)) {
            return FAT;
        }
        return env;
    }

    public static Env getEnv() {
        return Foundation.server().getEnv();
    }

    public static String getSubEnv() {
        return Foundation.server().getSubEnv();
    }

    public static boolean fat() {
        return FAT.equalsIgnoreCase(getEnvStr());
    }

    public static boolean pro() {
        return PRO.equalsIgnoreCase(getEnvStr());
    }
}
