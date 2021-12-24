package com.ctrip.framework.drc.console.monitor.consistency.utils;

import java.util.Calendar;
import java.util.Date;

/**
 * Created by mingdongli
 * 2019/11/20 下午10:06.
 */
public class DateUtils {

    public static Date NSecondsAgo(Date date, Integer n) {

        Calendar cal = Calendar.getInstance();
        cal.setTime(date);
        cal.add(Calendar.SECOND, -n);
        return cal.getTime();
    }
}
