package com.ctrip.framework.drc.monitor.automatic;

import com.ctrip.framework.drc.monitor.automatic.conflict.SceneBasedTest;
import com.ctrip.framework.drc.monitor.automatic.conflict.UnitConflictConfirmation;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * @Author Slight
 * Oct 18, 2019
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
        UnitConflictConfirmation.class,
        SceneBasedTest.class,
})
public class AutomaticTests {
}
