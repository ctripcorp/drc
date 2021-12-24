package com.ctrip.framework.drc.applier.confirmed.java8;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class DefaultOrSuper {
    interface I {
        default int count() {
            return 1;
        }

        default int getCount() {
            return count();
        }
    }

    class A {
        public int count() {
            return 2;
        }
    }

    class B extends A implements I {}

    class C extends A implements I {
        @Override
        public int count() {
            return 3;
        }
    }

    @Test
    public void doTest() {
        //use super method when the super method is assigned to be public
        B b = new B();
        assertEquals(2, b.count());

        C c = new C();
        assertEquals(3, c.count());
        assertEquals(3, c.getCount());
    }
}
