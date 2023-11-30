package com.ctrip.framework.drc.replicator.store.manager.file;

import com.ctrip.xpipe.api.endpoint.Endpoint;
import io.netty.channel.Channel;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import static org.mockito.Mockito.when;

/**
 * Created by jixinwang on 2023/8/18
 */
public class DefaultFileCheckTest {

    private FileManager fileManager = Mockito.mock(FileManager.class);

    @Mock
    private Channel channel;

    DefaultFileCheck fileCheck = new TestDefaultFileCheck("test", fileManager, null);

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
        fileCheck.setCheckPeriod(100);
    }

    @Test
    public void testStart() throws InterruptedException {
        when(fileManager.getCurrentLogSize()).thenReturn(100L);
        fileCheck.start(channel);
        Thread.sleep(180);
        Mockito.verify(channel, Mockito.times(1)).close();
    }

    class TestDefaultFileCheck extends DefaultFileCheck {

        public TestDefaultFileCheck(String registerKey, FileManager fileManager, Endpoint endpoint) {
            super(registerKey, fileManager, endpoint);
        }

        @Override
        protected boolean gtidSetMoving() {
            return true;
        }
    }
}