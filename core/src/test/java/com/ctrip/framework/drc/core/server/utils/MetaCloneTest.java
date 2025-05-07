package com.ctrip.framework.drc.core.server.utils;

import com.ctrip.framework.drc.core.entity.DbCluster;
import com.ctrip.framework.drc.core.entity.Dc;
import com.ctrip.framework.drc.core.entity.Drc;
import com.ctrip.framework.drc.core.transform.DefaultSaxParser;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.ClassUtils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

/**
 * Created by dengquanliang
 * 2024/7/29 14:31
 */
public class MetaCloneTest {

    private static Logger logger = LoggerFactory.getLogger(MetaCloneTest.class);

    private static final String XML_FILE_DRC = "drc.xml";
    private static String DRC_XML;

    @Test
    public void testClone() throws Exception {
        Drc drc = getDrc();
        Drc clone = MetaClone.clone(drc);
        Assert.assertEquals(drc.toString(), clone.toString());

        Dc dc = drc.getDcs().get("jq");
        Dc cloneDc = clone.getDcs().get("jq");
        DbCluster dbCluster = dc.getDbClusters().get("drcTest-1.abc");
        DbCluster cloneDbCluster = cloneDc.getDbClusters().get("drcTest-1.abc");


        Assert.assertFalse(drc == clone);
        Assert.assertFalse(dc == cloneDc);
        Assert.assertFalse(dc.getRoutes() == cloneDc.getRoutes());
        Assert.assertFalse(dc.getRoutes().get(0) == cloneDc.getRoutes().get(0));
        Assert.assertFalse(dc.getZkServer() == cloneDc.getZkServer());
        Assert.assertFalse(dc.getClusterManagers() == cloneDc.getClusterManagers());
        Assert.assertFalse(dc.getClusterManagers().get(0) == cloneDc.getClusterManagers().get(0));
        Assert.assertFalse(dbCluster == cloneDbCluster);
        Assert.assertFalse(dbCluster.getDbs() == cloneDbCluster.getDbs());
        Assert.assertFalse(dbCluster.getDbs().getDbs() == cloneDbCluster.getDbs().getDbs());
        Assert.assertFalse(dbCluster.getAppliers() == cloneDbCluster.getAppliers());
        Assert.assertFalse(dbCluster.getReplicators() == cloneDbCluster.getReplicators());
        Assert.assertFalse(dbCluster.getMessengers() == cloneDbCluster.getMessengers());
        Assert.assertFalse(dbCluster.getAppliers().get(0) == cloneDbCluster.getAppliers().get(0));
        Assert.assertFalse(dbCluster.getReplicators().get(0) == cloneDbCluster.getReplicators().get(0));

    }


    private DbCluster getDbCluster() throws Exception {
        String file = ClassUtils.getDefaultClassLoader().getResource(XML_FILE_DRC).getPath();
        DRC_XML = readFileContent(file);
        Drc drc = DefaultSaxParser.parse(DRC_XML);
        DbCluster dbCluster = drc.getDcs().get("jq").getDbClusters().get("drcTest-1.abc");
//        logger.info("dbCluster: {}", dbCluster);
        return dbCluster;
    }

    private Drc getDrc() throws Exception {
        String file = ClassUtils.getDefaultClassLoader().getResource(XML_FILE_DRC).getPath();
        DRC_XML = readFileContent(file);
        Drc drc = DefaultSaxParser.parse(DRC_XML);
        return drc;
    }

    public static String readFileContent(String fileName) {
        File file = new File(fileName);
        BufferedReader reader = null;
        StringBuffer sbf = new StringBuffer();
        try {
            reader = new BufferedReader(new FileReader(file));
            String tempStr;
            while ((tempStr = reader.readLine()) != null) {
                sbf.append(tempStr);
            }
            reader.close();
            return sbf.toString();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e1) {
                    e1.printStackTrace();
                }
            }
        }
        return sbf.toString();
    }
}
