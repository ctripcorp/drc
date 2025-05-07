package com.ctrip.framework.drc.messenger.server;

import com.ctrip.framework.drc.fetcher.server.FetcherServer;
import com.ctrip.framework.drc.messenger.container.MqServerContainer;
import org.junit.Test;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

/**
 * Created by shiruixin
 * 2024/11/8 16:55
 */
public class MessengerWatcherTest {
    private FetcherServer mockServer() throws Throwable {
        FetcherServer server = mock(FetcherServer.class);
        doReturn(100L).when(server).getLWM();
        return server;
    }

    @Test
    public void simple() throws Throwable {
        ConcurrentHashMap<String, FetcherServer> servers = new ConcurrentHashMap<>();
        servers.put("1", mockServer());
        MessengerWatcher watcher = new MessengerWatcher(mock(MqServerContainer.class), servers);
        watcher.initialize();
        watcher.start();
        TimeUnit.SECONDS.sleep(3);
        watcher.stop();
        watcher.dispose();
    }
}