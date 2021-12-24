package com.ctrip.framework.drc.console.monitor.delay.config;

import com.ctrip.framework.drc.console.AbstractTest;
import org.junit.Test;

import static com.ctrip.framework.drc.console.AllTests.DRC_XML;

public class LocalConfigTest extends AbstractTest {

    @Test
    public void testPersistConfig() throws InterruptedException {
        LocalConfig config = new LocalConfig();
        config.xml = DRC_XML;
        config.persistConfig();
    }

    public static final class LocalConfig extends AbstractConfig {

        @Override
        public void updateConfig() {

        }
    }
}
