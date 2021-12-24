package com.ctrip.framework.drc.applier.confirmed.mysql;

import org.junit.Before;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * @Author Slight
 * Oct 18, 2019
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
        //ManualTest.class
        //manual test cannot run automatically.
})
public class MysqlTests {

    @Before
    public void setUp() throws Exception {
        //set up mysql instance
    }
}
