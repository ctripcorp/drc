package com.ctrip.framework.drc.core.server.config;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static com.ctrip.framework.drc.core.server.config.SystemConfig.DEFAULT_CONFIG_FILE_NAME;

/**
 * @Author limingdong
 * @create 2021/12/7
 */
public class DefaultFileConfigTest {

    private DefaultFileConfig fileConfig;

    @Before
    public void setUp() throws Exception {
        String path = this.getClass().getClassLoader().getResource(DEFAULT_CONFIG_FILE_NAME).getPath();
        fileConfig = new DefaultFileConfig(path, DEFAULT_CONFIG_FILE_NAME);
    }

    @Test
    public void testGet() {
        Assert.assertEquals("value1", fileConfig.get("key1"));
        Assert.assertEquals("value2", fileConfig.get("key2"));
        Assert.assertEquals("value3", fileConfig.get("key3"));
        Assert.assertEquals("not_exist", fileConfig.get("key4", "not_exist"));
    }
}