package com.ctrip.framework.drc.console;

import com.ctrip.framework.drc.core.entity.Drc;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import org.junit.After;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

/**
 * @Author: hbshen
 * @Date: 2021/4/23
 */
public abstract class AbstractTest {

    Logger logger = LoggerFactory.getLogger(getClass());

    @Before
    public void setUp() throws Exception {
        AllTests.init();
    }

    // fresh Drc object parsed from meta.xml as the Parser misses recognizing master or slave Replicator of Applier
    protected void freshDrc(Drc drc) {
        drc.getDcs().get("dc1").getDbClusters().get("dbcluster1.mha1dc1").getReplicators().stream().filter(r -> r.getIp().equalsIgnoreCase("10.0.3.1")).findFirst().get().setMaster(true);
        drc.getDcs().get("dc1").getDbClusters().get("dbcluster1.mha2dc1").getReplicators().stream().filter(r -> r.getIp().equalsIgnoreCase("10.0.3.1")).findFirst().get().setMaster(true);
        drc.getDcs().get("dc1").getDbClusters().get("dbcluster2.mha3dc1").getReplicators().stream().filter(r -> r.getIp().equalsIgnoreCase("10.0.3.3")).findFirst().get().setMaster(true);
        drc.getDcs().get("dc2").getDbClusters().get("dbcluster1.mha1dc2").getReplicators().stream().filter(r -> r.getIp().equalsIgnoreCase("10.1.3.1")).findFirst().get().setMaster(true);
        drc.getDcs().get("dc2").getDbClusters().get("dbcluster1.mha2dc2").getReplicators().stream().filter(r -> r.getIp().equalsIgnoreCase("10.1.3.1")).findFirst().get().setMaster(true);
        drc.getDcs().get("dc2").getDbClusters().get("dbcluster2.mha3dc2").getReplicators().stream().filter(r -> r.getIp().equalsIgnoreCase("10.1.3.3")).findFirst().get().setMaster(true);
        drc.getDcs().get("dc3").getDbClusters().get("dbcluster2.mha3dc3").getReplicators().stream().filter(r -> r.getIp().equalsIgnoreCase("10.2.3.3")).findFirst().get().setMaster(true);
    }

    protected void prettyPrintJSON(String msg, String json) {
        Gson gson = new GsonBuilder().setPrettyPrinting().create();
        JsonParser jp = new JsonParser();
        JsonElement je = jp.parse(json);
        logger.info("{}:\n{}", msg, gson.toJson(je));
    }

    protected boolean existFile(String fileName) {
        File file = new File(fileName);
        return file.exists();
    }

    protected void deleteFile(String fileName) {
        File file = new File(fileName);
        if(file.exists()) {
            file.delete();
            logger.info("finish delete {}", fileName);
        }
    }

    @After
    public void tearDown() throws Exception {
    }
}
