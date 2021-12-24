package com.ctrip.framework.drc.applier.confirmed.reflect;

import org.junit.Test;

import java.lang.reflect.Method;

import static org.junit.Assert.*;

/**
 * @Author Slight
 * Jan 24, 2020
 */
public class Getter {

    class Group {
        public User getUser() {
            return new User();
        }
    }

    class User {
        public String getName() {
            return "Phi";
        }
    }

    Class configType = Group.class;
    Group config = new Group();
    String path = "user.name";

    @Test
    public void testGetter() throws Exception {
        Object target = config;
        Class targetType = configType;
        String[] names = path.split("\\.");
        for (int i = 0; i < names.length; i++) {
            String methodName = "get" + names[i].substring(0, 1).toUpperCase() + names[i].substring(1);
            Method method = targetType.getMethod(methodName);
            target = method.invoke(target);
            targetType = method.getReturnType();
        }
        assertEquals("Phi", targetType.cast(target));
        assertEquals("String", targetType.getSimpleName());
    }
}
