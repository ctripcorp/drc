package com.ctrip.framework.drc.performance;

import com.ctrip.framework.drc.performance.impl.ParseFileTest;
import com.ctrip.framework.drc.performance.impl.server.JdbcTestServerTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;


/**
 * Created by jixinwang on 2021/8/24
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
        ParseFileTest.class,
        JdbcTestServerTest.class,
        ApplierTestServerContainerTest.class
})

public class AllTests {

}
