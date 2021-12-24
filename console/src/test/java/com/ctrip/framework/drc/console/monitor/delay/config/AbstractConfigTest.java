package com.ctrip.framework.drc.console.monitor.delay.config;

import com.ctrip.framework.drc.console.AbstractTest;
import org.junit.After;
import org.junit.Before;

public class AbstractConfigTest  extends AbstractTest {

    String FILE_NAME = "./memory_meta_server_dao_file.xml";

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
    }

    @Override
    @After
    public void tearDown() throws Exception {

    }

    protected boolean existFile() {
        return existFile(FILE_NAME);
    }

    protected void deleteFile() {
        deleteFile(FILE_NAME);
    }
}
