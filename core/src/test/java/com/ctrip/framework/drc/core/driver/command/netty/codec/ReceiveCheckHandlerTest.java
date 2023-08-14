package com.ctrip.framework.drc.core.driver.command.netty.codec;

import io.netty.channel.*;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import static org.mockito.Mockito.when;

/**
 * Created by jixinwang on 2023/8/14
 */
public class ReceiveCheckHandlerTest {

    private ReceiveCheckHandler receiveCheckHandler = new ReceiveCheckHandler();

    @Mock
    private ChannelHandlerContext channelHandlerContext;

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
        receiveCheckHandler.setCheckPeriod(100);
    }

    @Test
    public void receiveCheckTest() throws Exception {
        when(channelHandlerContext.executor()).thenReturn(new DefaultEventLoop());
        receiveCheckHandler.channelActive(channelHandlerContext);
        Thread.sleep(260);
        Mockito.verify(channelHandlerContext, Mockito.times(1)).close();
        receiveCheckHandler.channelRead(channelHandlerContext, null);
        Thread.sleep(110);
        Mockito.verify(channelHandlerContext, Mockito.times(1)).close();
        Thread.sleep(110);
        Mockito.verify(channelHandlerContext, Mockito.times(2)).close();
    }
}