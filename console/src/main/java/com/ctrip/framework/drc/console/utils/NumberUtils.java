package com.ctrip.framework.drc.console.utils;

public class NumberUtils {
    public static boolean isPositive(Long aLong) {
        return aLong != null && aLong > 0;
    }
}
