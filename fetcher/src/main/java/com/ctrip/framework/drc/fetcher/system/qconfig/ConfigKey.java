package com.ctrip.framework.drc.fetcher.system.qconfig;

/**
 * @Author Slight
 * Jun 16, 2020
 */
public class ConfigKey {

    public final String filename;
    public final String key;

    public ConfigKey(String filename, String key) {
        this.filename = filename;
        this.key = key;
    }

    public static ConfigKey from(String filename, String key) {
        return new ConfigKey(filename, key);
    }
}
