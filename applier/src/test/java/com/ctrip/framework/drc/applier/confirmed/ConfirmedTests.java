package com.ctrip.framework.drc.applier.confirmed;

import com.ctrip.framework.drc.applier.confirmed.concurrrency.ConcurrencyTests;
import com.ctrip.framework.drc.applier.confirmed.concurrrency.OffsetNotifierTest;
import com.ctrip.framework.drc.applier.confirmed.concurrrency.ReturnWithinSynchronizedBlock;
import com.ctrip.framework.drc.applier.confirmed.java8.*;
import com.ctrip.framework.drc.applier.confirmed.log4j.ClassName;
import com.ctrip.framework.drc.applier.confirmed.reflect.Getter;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * @Author Slight
 * Oct 18, 2019
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
        //concurrency
        ConcurrencyTests.class,
        OffsetNotifierTest.class,
        ReturnWithinSynchronizedBlock.class,

        //java8
        Assert.class,
        DefaultOrSuper.class,
        StringEqual.class,
        List.class,
        CAS.class,
        Primitive.class,
        Map.class,

        //log4j
        ClassName.class,

        //reflect
        Getter.class,

        //MysqlTests.class,
})
public class ConfirmedTests {
}
