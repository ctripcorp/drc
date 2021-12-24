package com.ctrip.framework.drc.fetcher.system;

import java.lang.reflect.Field;
import java.lang.reflect.Method;

/**
 * @Author Slight
 * Jan 24, 2020
 */
public interface ConfigLoader {

    String PREFIX_GET = "get";

    Class getConfigType();

    Object getConfig();

    default void loadConfig(Object object, Field field, String path) throws Exception {
        Object target = getConfig();
        if (target == null) {
            return;
        }
        Class targetType = getConfigType();
        String[] names = path.split("\\.");
        for (int i = 0; i < names.length; i++) {
            String methodName = PREFIX_GET + names[i].substring(0, 1).toUpperCase() + names[i].substring(1);
            Method method;
            try {
                method = targetType.getMethod(methodName);
            } catch (NoSuchMethodException e) {
                return;
            }
            target = method.invoke(target);
            targetType = method.getReturnType();
        }
        if (target instanceof Integer) {
            field.set(object, ((Number) target).intValue());
        } else if (target instanceof Long){
            field.set(object, ((Number) target).longValue());
        } else {
            field.set(object, targetType.cast(target));
        }
    }
}
