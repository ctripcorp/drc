package com.ctrip.framework.drc.manager.healthcheck.notifier;

import com.ctrip.framework.drc.core.entity.DbCluster;
import com.ctrip.framework.drc.core.entity.Drc;
import com.ctrip.framework.drc.core.transform.DefaultSaxParser;
import com.ctrip.xpipe.foundation.DefaultFoundationService;
import org.junit.After;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;

import static com.ctrip.framework.drc.manager.AllTests.*;

/**
 * @Author limingdong
 * @create 2020/3/4
 */
public abstract class AbstractNotifierTest {

    protected static final Logger logger = LoggerFactory.getLogger(AbstractNotifierTest.class);

    protected Drc drc;

    protected DbCluster dbCluster;

    protected ByteArrayOutputStream baos;

    @Before
    public void setUp() throws Exception {
        drc = DefaultSaxParser.parse(DRC_XML);
        dbCluster = drc.getDcs().get(DC).getDbClusters().get(DAL_CLUSTER_ID);
        System.setProperty(DefaultFoundationService.DATA_CENTER_KEY, DC);
        setSystemOut();
    }

    protected void setSystemOut() {
        baos = new ByteArrayOutputStream(1024);
        PrintStream cacheStream = new PrintStream(baos);
        System.setOut(cacheStream);
    }

    @After
    public void tearDown() throws IOException {
        resetOut();
    }

    protected void resetOut() throws IOException {
        System.setOut(new PrintStream(new FileOutputStream(FileDescriptor.out)));
        baos.close();
    }
}
