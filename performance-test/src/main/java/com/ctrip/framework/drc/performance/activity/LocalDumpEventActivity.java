package com.ctrip.framework.drc.performance.activity;

import com.ctrip.framework.drc.applier.activity.event.ApplierDumpEventActivity;
import com.ctrip.framework.drc.core.driver.command.netty.endpoint.DefaultEndPoint;
import com.ctrip.framework.drc.fetcher.system.InstanceConfig;
import com.ctrip.framework.drc.performance.impl.ParseFile;
import com.ctrip.xpipe.api.endpoint.Endpoint;

/**
 * Created by jixinwang on 2021/8/16
 */
public class LocalDumpEventActivity extends ApplierDumpEventActivity {

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
        ParseFile parseFile = new ParseFile(applyEndpoint);
        parseFile.setEventHandler(eventHandler);
        server = parseFile;
        server.initialize();
        server.start();
        logger.info("[start performance test]");
    }
}
