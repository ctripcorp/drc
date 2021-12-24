package com.ctrip.framework.drc.applier.confirmed.meta;

import com.ctrip.framework.drc.core.meta.ApplierMeta;
import com.ctrip.framework.drc.core.meta.DBInfo;
import com.ctrip.framework.drc.core.meta.InstanceInfo;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @Author Slight
 * Nov 07, 2019
 */
public class ApplierMetaTest {

    private Logger logger = LoggerFactory.getLogger(getClass());

    @Test
    public void doTest() {
        ApplierMeta applier = new ApplierMeta();
        applier.replicator = new InstanceInfo();
        applier.target = new DBInfo();

        applier.ip = "127.0.0.1";
        applier.port = 8001;
        applier.idc = "jq";
        applier.name = "drc-test-applier-1";
        applier.cluster = "drc-test";

        applier.replicator.ip = "127.0.0.2";
        applier.replicator.port = 8002;
        applier.replicator.idc = "oy";
        applier.replicator.name = "drc-test-replicator-1";
        applier.replicator.cluster = "drc-test";

        applier.target.ip = "127.0.0.2";
        applier.target.port = 8003;
        applier.target.idc = "jq";
        applier.target.name = "DBNAME0001";
        applier.target.cluster = "drc-test";
        applier.target.username = "root";
        applier.target.password = "root";
        applier.target.uuid = "a0780e56-d445-11e9-97b4-58a328e0e9f2";

        logger.info("applier is {}", applier);
    }
}
