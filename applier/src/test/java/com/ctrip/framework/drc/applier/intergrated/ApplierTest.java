package com.ctrip.framework.drc.applier.intergrated;

import com.ctrip.framework.drc.applier.server.LocalApplierServer;
import org.junit.Test;

/**
 * @Author Slight
 * Sep 30, 2019
 */
public class ApplierTest {

    @Test
    public void aCompleteLifecycle() throws Exception {
        LocalApplierServer server = new LocalApplierServer();
        server.initialize();
        server.start();
        server.stop();
        server.dispose();
    }
}