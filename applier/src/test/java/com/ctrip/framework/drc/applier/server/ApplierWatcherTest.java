package com.ctrip.framework.drc.applier.server;

import com.ctrip.framework.drc.applier.container.ApplierServerContainer;
import com.ctrip.framework.drc.fetcher.server.FetcherServer;
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

    private FetcherServer mockServer() throws Throwable {
        FetcherServer server = mock(FetcherServer.class);
        doReturn(100L).when(server).getLWM();
        return server;
    }

    @Test
    public void simple() throws Throwable {
        ConcurrentHashMap<String, FetcherServer> servers = new ConcurrentHashMap<>();
        servers.put("1", mockServer());
        ApplierWatcher watcher = new ApplierWatcher(mock(ApplierServerContainer.class), servers);
        watcher.initialize();
        watcher.start();
        TimeUnit.SECONDS.sleep(3);
        watcher.stop();
        watcher.dispose();
    }

}