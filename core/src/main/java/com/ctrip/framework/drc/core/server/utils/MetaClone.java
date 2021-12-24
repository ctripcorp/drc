package com.ctrip.framework.drc.core.server.utils;

import org.apache.commons.lang3.SerializationException;
import org.apache.commons.lang3.SerializationUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;

/**
 * @Author limingdong
 * @create 2020/4/21
 */
public class MetaClone {

    private static Logger logger = LoggerFactory.getLogger(MetaClone.class);

    public static <T extends Serializable> T clone(T obj){
        try {
            return SerializationUtils.clone(obj);
        }catch (SerializationException e){
            logger.error("[clone]", e);
            throw e;
        }
    }

}