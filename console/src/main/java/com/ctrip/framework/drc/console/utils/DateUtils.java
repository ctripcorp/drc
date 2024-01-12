package com.ctrip.framework.drc.console.utils;

import org.apache.commons.lang.time.FastDateFormat;

import java.util.Calendar;

/**
 * Created by dengquanliang
 * 2023/9/26 16:56
 */
public class DateUtils {

    public static final String FORMATTER = "yyyy-MM-dd HH:mm:ss";
    public static final long ONE_DAY_MILLIS = 24 * 60 * 60 * 1000;
    private static final FastDateFormat fdf = FastDateFormat.getInstance(FORMATTER);

    public static String longToString(long time) {
        return fdf.format(time);
    }

    /**
     * 获取当天零点时间
     */
    public static String getStartDateOfDay(long time) {
        return longToString(getStartTimeOfDay(time));
    }

    /**
     * 获取当天结束时间
     */
    public static String getEndDateOfDay(long time) {
        return longToString(getEndTimeOfDay(time));
    }

    /**
     * 获取当天零点时间戳
     */
    public static long getStartTimeOfDay(long time) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeInMillis(time);

        calendar.set(Calendar.HOUR_OF_DAY, 0);
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.SECOND, 0);
        calendar.set(Calendar.MILLISECOND, 0);

        return calendar.getTimeInMillis();
    }

    /**
     * 获取当天某个整点时间
     */
    public static long getTimeOfHour(int hour) {
        long curTime = System.currentTimeMillis();
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeInMillis(curTime);
        calendar.set(Calendar.HOUR_OF_DAY, hour);
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.SECOND, 0);
        calendar.set(Calendar.MILLISECOND, 0);
        return calendar.getTime().getTime();
    }

    /**
     * 获取当天结束时间戳
     */
    public static long getEndTimeOfDay(long time) {
       return getStartTimeOfDay(time) + ONE_DAY_MILLIS - 1;
    }
}
