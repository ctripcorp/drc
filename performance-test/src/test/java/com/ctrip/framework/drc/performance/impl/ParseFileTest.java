package com.ctrip.framework.drc.performance.impl;

import com.ctrip.framework.drc.core.driver.command.netty.endpoint.DefaultEndPoint;
import org.junit.Before;
import org.junit.Test;

import java.io.File;

import static org.junit.Assert.*;

/**
 * Created by jixinwang on 2021/9/22
 */
public class ParseFileTest {

    private ParseFile parseFile;

    private DefaultEndPoint endPoint;

    @Before
    public void before() throws Exception {
        System.setProperty("parse.file.path", "src/test/resources/rbinlog");
        endPoint = new DefaultEndPoint("127.0.0.1", 3306, "root", "123456");
        parseFile = new ParseFile(endPoint);
        parseFile.initialize();
    }

    @Test
    public void getEvent() {
        parseFile.handleFiles();
    }
}
