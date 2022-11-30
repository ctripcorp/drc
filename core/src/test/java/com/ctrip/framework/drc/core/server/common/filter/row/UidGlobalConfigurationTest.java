package com.ctrip.framework.drc.core.server.common.filter.row;

import org.junit.Assert;
import org.junit.Test;

import java.io.File;

/**
 * Created by jixinwang on 2022/11/30
 */
public class UidGlobalConfigurationTest {

    private static final String registryKey = "drc_ut";

    @Test
    public void filterRowsWithBlackListWithRemoteBlacklist() throws Exception {
        UidGlobalConfiguration uidGlobalConfiguration = UidGlobalConfiguration.getInstance();

        Assert.assertFalse(uidGlobalConfiguration.filterRowsWithBlackList(fetchUidContext("uid1", registryKey)));
        Assert.assertTrue(uidGlobalConfiguration.filterRowsWithBlackList(fetchUidContext("uid3", registryKey)));
    }


    @Test
    public void filterRowsWithBlackListWithLocalBlacklist() throws Exception {
        UidGlobalConfiguration uidGlobalConfiguration = UidGlobalConfiguration.getInstance();
        File localUidFile = new File("src/test/resources/uid.filter.blacklist.global");;
        uidGlobalConfiguration.setFile(localUidFile);
        uidGlobalConfiguration.updateBlacklist();

        Assert.assertFalse(uidGlobalConfiguration.filterRowsWithBlackList(fetchUidContext("uid10", registryKey)));
        Assert.assertTrue(uidGlobalConfiguration.filterRowsWithBlackList(fetchUidContext("uid13", registryKey)));
    }

    private UserContext fetchUidContext(String uid, String registryKey) {
        UserContext uidContext = new UserContext();
        uidContext.setUserAttr(uid);
        uidContext.setRegistryKey(registryKey);
        uidContext.setIllegalArgument(false);

        return uidContext;
    }
}
