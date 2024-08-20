package com.ctrip.framework.drc.core.server.utils;

import com.ctrip.framework.drc.core.entity.CloneableInstance;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.SerializationException;
import org.apache.commons.lang3.SerializationUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * @Author limingdong
 * @create 2020/4/21
 */
public class MetaClone {

    private static Logger logger = LoggerFactory.getLogger(MetaClone.class);

    public static <T extends CloneableInstance<T>> T clone(T obj) {
        if (obj == null) {
            return null;
        }
        try {
            return obj.cloneInstance();
        } catch (CloneNotSupportedException e) {
            logger.error("[clone]", e);
            throw new IllegalArgumentException(e);
        }
    }

    public static <T extends CloneableInstance<T>> List<T> cloneList(List<T> objs) {
        try {
            List<T> list = Lists.newArrayList();
            for (T obj : objs) {
                list.add(clone(obj));
            }
            return list;
        }catch (Exception e){
            logger.error("[clone]", e);
            throw e;
        }
    }

    public static <K, V extends CloneableInstance<V>> Map<K, V> cloneMap(Map<K, V> objMap) {
        try {
            Map<K, V> map = new LinkedHashMap<>();
            for (Map.Entry<K, V> entry : objMap.entrySet()) {
                map.put(entry.getKey(), clone(entry.getValue()));
            }
            return map;
        }catch (Exception e){
            logger.error("[clone]", e);
            throw e;
        }
    }
}