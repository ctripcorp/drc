package com.ctrip.framework.drc.console.utils;

import org.apache.commons.lang.time.FastDateFormat;

/**
 * Created by dengquanliang
 * 2023/9/26 16:56
 */
public class DateUtils {

    public static final String FORMATTER = "yyyy-MM-dd HH:mm:ss";
    private static final FastDateFormat fdf = FastDateFormat.getInstance(FORMATTER);

    public static String longToString(long time) {
        return fdf.format(time);
    }
}
