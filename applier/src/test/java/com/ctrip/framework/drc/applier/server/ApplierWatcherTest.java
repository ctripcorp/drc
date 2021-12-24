package com.ctrip.framework.drc.applier.server;

import com.ctrip.framework.drc.applier.container.ApplierServerContainer;
import org.junit.Test;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

/**
 * @Author Slight
 * Aug 19, 2020
 */
public class ApplierWatcherTest {

    private ApplierServer mockServer() throws Throwable {
        ApplierServer server = mock(ApplierServer.class);
        doReturn(100L).when(server).getLWM();
        return server;
    }

    @Test
    public void simple() throws Throwable {
        ConcurrentHashMap<String, ApplierServer> servers = new ConcurrentHashMap<>();
        servers.put("1", mockServer());
        ApplierWatcher watcher = new ApplierWatcher(mock(ApplierServerContainer.class), servers);
        watcher.initialize();
        watcher.start();
        TimeUnit.SECONDS.sleep(3);
        watcher.stop();
        watcher.dispose();
    }

}