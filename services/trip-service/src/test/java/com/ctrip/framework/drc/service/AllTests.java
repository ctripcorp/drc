package com.ctrip.framework.drc.service;

import com.ctrip.framework.drc.service.console.web.filter.IAMFilterTest;
import com.ctrip.framework.drc.service.console.web.filter.IAMServiceImplTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses({
        IAMFilterTest.class,
        IAMServiceImplTest.class
})
public class AllTests {


    @BeforeClass
    public static void setUp() {

    }


    @AfterClass
    public static void tearDown() {

    }


}