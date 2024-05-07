package com.ctrip.framework.drc.applier.container.controller.task;

import com.ctrip.framework.drc.applier.activity.monitor.WatchActivity.LastLWM;
import com.ctrip.framework.drc.applier.container.ApplierServerContainer;
import com.ctrip.framework.drc.applier.server.ApplierServer;
import com.ctrip.framework.drc.fetcher.system.SystemStatus;
import java.util.concurrent.ConcurrentHashMap;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

public class WatchKeyedTaskTest {
    
    
    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.openMocks(this);
    }

    @Test
    public void testDoExecute() throws Throwable {
        ConcurrentHashMap<String, LastLWM> lastLWMHashMap = new ConcurrentHashMap<>();
        LastLWM lastLWM = new LastLWM(1, 1, 1);
        lastLWMHashMap.put("applier_key",lastLWM);

        ApplierServer mockServer = Mockito.mock(ApplierServer.class);
        Mockito.when(mockServer.getLWM()).thenReturn(1L);
        Mockito.when(mockServer.getProgress()).thenReturn(1L);
        Mockito.when(mockServer.getStatus()).thenReturn(SystemStatus.RUNNABLE);

        
        
        ApplierServerContainer mockContainer = Mockito.mock(ApplierServerContainer.class);
        WatchKeyedTask watchKeyedTask1 = new WatchKeyedTask("empty_key", null, mockContainer, lastLWMHashMap);
        WatchKeyedTask watchKeyedTask2 = new WatchKeyedTask("applier_key", null, mockContainer, lastLWMHashMap);
        WatchKeyedTask watchKeyedTask3 = new WatchKeyedTask("applier_key", null, mockContainer, lastLWMHashMap);
        Mockito.when(mockContainer.containServer(Mockito.eq("empty_key"))).thenReturn(false);
        Mockito.when(mockContainer.containServer(Mockito.eq("applier_key"))).thenReturn(true);
        Mockito.when(mockContainer.getServer(Mockito.eq("applier_key"))).thenReturn(mockServer);
        
        watchKeyedTask1.doExecute();
        Assert.assertEquals(null,watchKeyedTask1.future().get());
        
        
        watchKeyedTask2.doExecute();
        Mockito.doNothing().when(mockContainer).removeServer(Mockito.eq("applier_key"),Mockito.eq(true));
        Mockito.when(mockContainer.registerServer(Mockito.eq("applier_key"))).thenReturn(null);
        
        Assert.assertEquals(null,watchKeyedTask2.future().get());
        Assert.assertEquals(0,lastLWMHashMap.size());

        lastLWMHashMap.put("applier_key",lastLWM);
        watchKeyedTask3.doExecute();
        Mockito.doThrow(new RuntimeException()).when(mockContainer).removeServer(Mockito.eq("applier_key"),Mockito.eq(true));
        Assert.assertEquals(null,watchKeyedTask3.future().get());
        Assert.assertEquals(1,lastLWMHashMap.size());




    }
}