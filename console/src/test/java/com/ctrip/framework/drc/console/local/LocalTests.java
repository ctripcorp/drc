package com.ctrip.framework.drc.console.local;

import com.ctrip.framework.drc.console.monitor.table.task.LocalCheckTableConsistencyTaskTest;
import com.ctrip.framework.drc.console.resource.TransactionContextResourceTest;
import com.ctrip.framework.drc.console.service.impl.LocalHealthServiceImplTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * @author shenhaibo
 * @version 1.0
 * date: 2020-04-30
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
        // monitor
        LocalCheckTableConsistencyTaskTest.class,
        

        // util
        TransactionContextResourceTest.class,

        DigestUtilsTest.class,

        LocalHealthServiceImplTest.class
})
public class LocalTests {
}
