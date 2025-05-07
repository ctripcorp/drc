package com.ctrip.framework.drc.fetcher.resource.context;

import com.ctrip.framework.drc.fetcher.system.AbstractResource;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @Author Slight
 * Oct 22, 2019
 */
public abstract class AbstractContext extends AbstractResource implements Context.Simple {

    protected Map<String, Object> keyValues = new ConcurrentHashMap<>();

    @Override
    public void update(String key, Object value) {
        keyValues.put(key, value);
    }

    @Override
    public Object fetch(String key) {
        Object value = keyValues.get(key);
        if (value == null) {
            throw new RuntimeException("unavailable context value when fetch(), key: " + key);
        }
        return value;
    }

    @Override
    protected void doInitialize() throws Exception {
        keyValues = new ConcurrentHashMap<>();
    }

    public Map<String, Object> getKeyValues() {
        return keyValues;
    }
}
