package com.ctrip.framework.drc.fetcher.system.qconfig;

import com.ctrip.framework.drc.fetcher.system.AbstractSystem;
import com.ctrip.framework.drc.fetcher.system.InstanceConfig;
import org.junit.Test;

/**
 * @Author limingdong
 * @create 2021/3/15
 */
public class AbstractUnitUsingQConfigTest {

    class TestSystem extends AbstractSystem {

    }

    class SimpleQConfigUnit extends AbstractUnitUsingQConfig {

        @InstanceConfig(path = "number")
        public volatile int number = 1;

        @Override
        public String namespace() {
            return "test";
        }
    }

    @Test
    public void simple() throws Exception {
        SimpleQConfigUnit unit = new SimpleQConfigUnit();
        unit.setSystem(new TestSystem());
        unit.load();
        long n = 1;
        System.out.println("start" + ": " + unit.number);
        for (long i = 0; i < 1000000000; i++) {
            for (long j = 0; j < 20; j++) {
                n *= unit.number;
            }
        }
        System.out.println(n);
    }
}