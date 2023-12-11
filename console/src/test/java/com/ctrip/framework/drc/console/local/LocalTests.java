package com.ctrip.framework.drc.console.local;

import com.ctrip.framework.drc.console.resource.TransactionContextResourceTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * @author shenhaibo
 * @version 1.0
 * date: 2020-04-30
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({

        // util
        TransactionContextResourceTest.class,

        DigestUtilsTest.class,
})
public class LocalTests {
}
