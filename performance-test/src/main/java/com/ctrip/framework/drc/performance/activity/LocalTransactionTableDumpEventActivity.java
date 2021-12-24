package com.ctrip.framework.drc.performance.activity;

import com.ctrip.framework.drc.applier.activity.event.TransactionTableApplierDumpEventActivity;
import com.ctrip.framework.drc.core.driver.command.netty.endpoint.DefaultEndPoint;
import com.ctrip.framework.drc.fetcher.system.InstanceConfig;
import com.ctrip.framework.drc.performance.impl.TransactionTableParseFile;
import com.ctrip.xpipe.api.endpoint.Endpoint;

/**
 * Created by jixinwang on 2021/9/1
 */
public class LocalTransactionTableDumpEventActivity extends TransactionTableApplierDumpEventActivity {

    @InstanceConfig(path = "target.ip")
    public String ip;

    @InstanceConfig(path = "target.port")
    public int port;

    @InstanceConfig(path = "target.username")
    public String username;

    @InstanceConfig(path = "target.password")
    public String password;

    @Override
    public void doStart() throws Exception {
        Endpoint applyEndpoint = new DefaultEndPoint(ip, port, username, password);
        TransactionTableParseFile parseFile = new TransactionTableParseFile(applyEndpoint);
        parseFile.setEventHandler(eventHandler);
        server = parseFile;
        server.initialize();
        server.start();
        logger.info("[start transaction performance test]");
    }
}
