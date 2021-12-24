package com.ctrip.framework.drc.applier.activity.event;

import com.ctrip.framework.drc.fetcher.activity.event.EventActivity;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * @Author Slight
 * Jul 07, 2020
 */
public class EventActivityTest {

    public static class HelloActivity extends EventActivity {
        @Override
        public Object doTask(Object task) {
            return null;
        }
    }

    @Test
    public void namespace() {
        assertEquals("hello", new HelloActivity().namespace());
    }
}