package com.ctrip.framework.drc.replicator.impl.oubound.channel;

import com.ctrip.xpipe.utils.Gate;
import org.junit.Assert;
import org.junit.Test;

/**
 * @Author limingdong
 * @create 2022/6/9
 */
public class ChannelAttributeKeyTest {

    private ChannelAttributeKey channelAttributeKey;

    private Gate gate = new Gate("ut");

    @Test
    public void testSendEvent() {
        channelAttributeKey = new ChannelAttributeKey(gate);
        Assert.assertFalse(channelAttributeKey.isTouchProgress());
        Assert.assertFalse(channelAttributeKey.isTouchProgress());

        channelAttributeKey.handleEvent(false);
        Assert.assertTrue(channelAttributeKey.isTouchProgress());

        channelAttributeKey.handleEvent(false);
        Assert.assertTrue(channelAttributeKey.isTouchProgress());

        channelAttributeKey.handleEvent(true);
        Assert.assertFalse(channelAttributeKey.isTouchProgress());

    }

}