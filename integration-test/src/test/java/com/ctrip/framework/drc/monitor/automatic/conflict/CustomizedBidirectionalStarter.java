package com.ctrip.framework.drc.monitor.automatic.conflict;

import com.ctrip.framework.drc.monitor.BidirectionalStarter;
import org.junit.Before;
import org.junit.Test;

import static com.ctrip.framework.drc.monitor.module.config.AbstractConfigTest.*;

/**
 * @Author Slight
 * Oct 22, 2019
 */
public class CustomizedBidirectionalStarter extends BidirectionalStarter {

    boolean isMySQLPortFixed = true;
    boolean forceRebootMysql = false;
    //use the command before to force reboot
    //docker ps -a | awk '{print $1}' | grep -v CONTAINER | xargs docker rm -f
    //docker volume prune

    @Override
    @Before
    public void setUp() {
        if (isMySQLPortFixed) {
            mysqlPortA = BASE_PORT;
            mysqlPortB = BASE_PORT + 1;
        }
        super.setUp();
        unidirectionalReplicateModule.setImage("mysql:5.7");
    }

    @Override
    public void doTest() throws Exception {
        blockForManualTest = false;
        skipMonitor = true;
        super.doTest();
    }

    @Test
    public void manualTest() throws Exception {
        blockForManualTest = true;
        skipMonitor = true;
        super.doTest();
    }
}
