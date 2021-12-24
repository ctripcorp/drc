package com.ctrip.framework.drc.applier.confirmed.log4j;

import com.ctrip.xpipe.lifecycle.DefaultLifecycleState;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ClassName {

    @Test
    public void getClassGetName() {
        assertEquals("com.ctrip.xpipe.lifecycle.DefaultLifecycleState", DefaultLifecycleState.class.getName());
    }
}
