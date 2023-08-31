package com.ctrip.framework.drc.manager.ha.rest;

import com.ctrip.framework.drc.manager.ha.cluster.ClusterManager;
import org.junit.Test;
import org.mockito.ArgumentMatcher;

import java.util.LinkedList;
import java.util.List;

import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/**
 * Created by jixinwang on 2023/8/31
 */
public class MultiMetaServerTest {

    @Test
    public void testMultiProxy(){

        int serversCount = 10;
        List<ClusterManager> servers = new LinkedList<>();

        for(int i=0; i < serversCount - 1 ; i++){
            servers.add(mock(ClusterManager.class));
        }

        ClusterManager metaServer = MultiMetaServer.newProxy(mock(ClusterManager.class), servers);

        final ForwardInfo forwardInfo = new ForwardInfo();

        metaServer.clusterDeleted("testId", forwardInfo, "test");

        for(ClusterManager mockServer :  servers){
            verify(mockServer).clusterDeleted(eq("testId"),  argThat(new ArgumentMatcher<ForwardInfo>() {

                @Override
                public boolean matches(ForwardInfo item) {
                    //should be cloned
                    if(item == forwardInfo){
                        return false;
                    }
                    return true;
                }
            }), eq("test"));
        }
    }
}