package com.ctrip.framework.drc.console;

import ch.vorburger.exec.ManagedProcessException;
import org.junit.Test;

/**
 * Created by dengquanliang
 * 2023/10/18 14:40
 */
public class DbTest {

    @Test
    public void init() throws ManagedProcessException {
        AllTests.initTestDb();
        System.out.println("init");
    }
}
