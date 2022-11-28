package com.ctrip.framework.drc.replicator.store.manager.file;

import com.ctrip.framework.drc.core.driver.binlog.impl.DrcIndexLogEvent;
import com.ctrip.framework.drc.replicator.MockTest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.List;

/**
 * @Author limingdong
 * @create 2022/11/28
 */
public class IndicesEventManagerTest extends MockTest {

    private static final long FIRST_INDEX_POSITION = 123l;

    private IndicesEventManager indicesEventManager;

    @Mock
    private FileChannel logChannel;

    @Before
    public void setUp() throws IOException {
        super.initMocks();
        when(logChannel.position()).thenReturn(FIRST_INDEX_POSITION);
        indicesEventManager = new IndicesEventManager(logChannel, "registryKey", "fileName");
        Assert.assertFalse(indicesEventManager.isEverSeeDdl());
        Assert.assertTrue(indicesEventManager.getIndicesSize() == 0);
    }

    @Test
    public void testOperateIndexEvent() throws IOException {
        long exceptPosition = 321l;
        long step = 100l;
        when(logChannel.position()).thenReturn(exceptPosition);
        DrcIndexLogEvent drcIndexLogEvent = indicesEventManager.createIndexEvent(200);
        List<Long> indices = drcIndexLogEvent.getIndices();
        List<Long> notRevisedIndices = drcIndexLogEvent.getNotRevisedIndices();
        Assert.assertTrue(indicesEventManager.getIndicesSize() == 0);
        Assert.assertTrue(indices.size() == 1);
        Assert.assertTrue(notRevisedIndices.size() == 1);
        Assert.assertTrue(indices.contains(FIRST_INDEX_POSITION));
        Assert.assertTrue(notRevisedIndices.contains(FIRST_INDEX_POSITION));


        exceptPosition = exceptPosition + step;
        drcIndexLogEvent = indicesEventManager.updateIndexEvent(exceptPosition);
        indices = drcIndexLogEvent.getIndices();
        notRevisedIndices = drcIndexLogEvent.getNotRevisedIndices();
        Assert.assertTrue(indicesEventManager.getIndicesSize() == 1);
        Assert.assertTrue(indices.size() == 2);
        Assert.assertTrue(notRevisedIndices.size() == 2);
        Assert.assertTrue(indices.contains(FIRST_INDEX_POSITION));
        Assert.assertTrue(indices.contains(exceptPosition));
        Assert.assertTrue(notRevisedIndices.contains(FIRST_INDEX_POSITION));
        Assert.assertTrue(notRevisedIndices.contains(exceptPosition));

        indicesEventManager.setEverSeeDdl(true);

        exceptPosition = exceptPosition + step;
        drcIndexLogEvent = indicesEventManager.updateIndexEvent(exceptPosition);
        indices = drcIndexLogEvent.getIndices();
        notRevisedIndices = drcIndexLogEvent.getNotRevisedIndices();
        Assert.assertTrue(indicesEventManager.getIndicesSize() == 2);
        Assert.assertTrue(indices.size() == 3);
        Assert.assertTrue(notRevisedIndices.size() == 3);
        Assert.assertTrue(indices.contains(FIRST_INDEX_POSITION));
        Assert.assertFalse(indices.contains(exceptPosition));
        Assert.assertTrue(notRevisedIndices.contains(FIRST_INDEX_POSITION));
        Assert.assertTrue(notRevisedIndices.contains(exceptPosition));
    }

}