package com.ctrip.framework.drc.console.utils;

import com.google.common.base.Preconditions;
import org.apache.commons.lang3.StringUtils;
import org.springframework.util.CollectionUtils;

import java.util.Collection;

/**
 * Created by dengquanliang
 * 2023/4/26 11:29
 */
public class PreconditionUtils {

    public static <T> void checkNotNull(T param) {
        checkNotNull(param, "param requires not null");
    }

    public static <T> void checkNotNull(T param, String errorMessage) {
        try {
            Preconditions.checkNotNull(param, errorMessage);
        } catch (Exception e) {
            throw new IllegalArgumentException(errorMessage);
        }
    }

    public static void checkString(String str, String errorMessage) {
        checkArgument(StringUtils.isNotBlank(str), errorMessage);
    }

    public static void checkId(Long id, String errorMessage) {
        checkArgument(id != null && id > 0L, errorMessage);
    }

    public static <T> void checkCollection(Collection<T> collection, String errorMessage) {
        checkArgument(!CollectionUtils.isEmpty(collection), errorMessage);
    }
    public static void checkArgument(boolean expression, String errorMessage) {
        Preconditions.checkArgument(expression, errorMessage);
    }
}
