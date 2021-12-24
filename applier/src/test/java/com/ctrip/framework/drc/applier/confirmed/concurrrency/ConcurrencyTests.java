package com.ctrip.framework.drc.applier.confirmed.concurrrency;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * @Author Slight
 * Oct 27, 2019
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
        ReturnWithinSynchronizedBlock.class,
        OffsetNotifierTest.class,
})
public class ConcurrencyTests {
}
