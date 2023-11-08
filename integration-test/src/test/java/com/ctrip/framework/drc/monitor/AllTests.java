package com.ctrip.framework.drc.monitor;

import com.ctrip.framework.drc.monitor.function.task.TableCompareTaskTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;


@RunWith(Suite.class)
@Suite.SuiteClasses({
        TableCompareTaskTest.class
})
public class AllTests {


    @BeforeClass
    public static void setUp() {

    }


    @AfterClass
    public static void tearDown() {

    }


}
