package com.ctrip.framework.drc.core.server.common.filter.row;

import com.ctrip.framework.drc.core.monitor.util.IsolateHashCache;
import org.junit.Assert;
import org.junit.Test;

import java.util.Set;

import static com.ctrip.framework.drc.core.server.common.filter.row.UidConfiguration.UID_BLACKLIST;
import static com.ctrip.framework.drc.core.server.common.filter.row.UidConfiguration.UID_WHITELIST;


/**
 * @Author limingdong
 * @create 2022/6/30
 */
public class UidConfigurationTest {

    private static final String registryKey = "drc_ut";

    private static final String notExist = "not_exist";

    private UidConfiguration uidConfiguration = UidConfiguration.getInstance();

    @Test
    public void tearDown() {
        UidConfiguration.getInstance().clear();
    }

    @Test
    public void filterRowsWithBlackList() throws Exception {
        Assert.assertFalse(uidConfiguration.filterRowsWithBlackList("uid2", registryKey));
        Assert.assertTrue(uidConfiguration.filterRowsWithBlackList("uid2", registryKey + notExist));

        String key = String.format(UID_BLACKLIST, registryKey);
        uidConfiguration.onChange(key, "uid1,uid2,uid3", "uid1");

        IsolateHashCache<String, Set<String>> cache =  uidConfiguration.getBlackListCache();
        Assert.assertEquals(1, cache.size());
        Assert.assertTrue(cache.asMap().containsKey(registryKey + notExist));
    }

    @Test
    public void filterRowsWithWhiteList() throws Exception {
        Assert.assertTrue(uidConfiguration.filterRowsWithWhiteList("uid4", registryKey));
        Assert.assertFalse(uidConfiguration.filterRowsWithWhiteList("uid4", registryKey + notExist));

        String key = String.format(UID_WHITELIST, registryKey);
        uidConfiguration.onChange(key, "uid4,uid5", "uid4");

        IsolateHashCache<String, Set<String>> cache =  uidConfiguration.getWhiteListCache();
        Assert.assertEquals(1, cache.size());
        Assert.assertTrue(cache.asMap().containsKey(registryKey + notExist));
    }
}