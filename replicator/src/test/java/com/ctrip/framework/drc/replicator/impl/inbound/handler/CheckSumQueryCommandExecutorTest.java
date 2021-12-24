package com.ctrip.framework.drc.replicator.impl.inbound.handler;

import com.ctrip.framework.drc.core.driver.command.packet.server.ResultSetPacket;
import com.ctrip.framework.drc.replicator.MockTest;
import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.netty.commands.NettyClient;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

/**
 * Created by mingdongli
 * 2019/10/27 下午1:42.
 */
public class CheckSumQueryCommandExecutorTest extends MockTest {

    @Mock
    private SimpleObjectPool<NettyClient> simpleObjectPool;

    @Mock
    private NettyClient nettyClient;

    private CheckSumQueryCommandExecutor checkSumQueryCommandExecutor;

    private QueryClientCommandHandler queryCommandHandler;

    @Before
    public void setUp() throws Exception {
        super.initMocks();
        queryCommandHandler = new QueryClientCommandHandler();
        checkSumQueryCommandExecutor = new CheckSumQueryCommandExecutor(queryCommandHandler);
        when(simpleObjectPool.borrowObject()).thenReturn(nettyClient);
        doNothing().when(nettyClient).sendRequest(anyObject(), anyObject());

    }

    @Test
    public void getQueryString() {
        CommandFuture<ResultSetPacket> commandFuture = checkSumQueryCommandExecutor.handle(simpleObjectPool);
        ResultSetPacket resultSetPacket = commandFuture.getNow();
        Assert.assertNull(resultSetPacket);
    }
}