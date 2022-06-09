package com.ctrip.framework.drc.replicator.impl.oubound.channel;

import com.ctrip.xpipe.utils.Gate;

import java.util.Objects;

/**
 * @Author limingdong
 * @create 2022/4/7
 */
public class ChannelAttributeKey {

    public static final long NO_SKIP_EVENT = 0;

    private Gate gate;

    private boolean heartBeat;

    private long firstSkipEventTime;

    private long skipCountInARow = 0;

    public ChannelAttributeKey(Gate gate) {
        this(gate, true);
    }

    public ChannelAttributeKey(Gate gate, boolean heartBeat) {
        this.gate = gate;
        this.heartBeat = heartBeat;
        this.firstSkipEventTime = NO_SKIP_EVENT;
    }

    public Gate getGate() {
        return gate;
    }

    public void setGate(Gate gate) {
        this.gate = gate;
    }

    public boolean isHeartBeat() {
        return heartBeat;
    }

    public void setHeartBeat(boolean heartBeat) {
        this.heartBeat = heartBeat;
    }

    public void handleEvent(boolean send) {
        if (send) {
            firstSkipEventTime = NO_SKIP_EVENT;
            skipCountInARow = 0;
        } else {
            if (firstSkipEventTime == NO_SKIP_EVENT) {
                firstSkipEventTime = System.currentTimeMillis();
            }
            skipCountInARow++;
        }
    }

    public boolean isTouchProgress() {
        if (firstSkipEventTime == NO_SKIP_EVENT) {
            return false;
        }
        return skipCountInARow / ((System.currentTimeMillis() - firstSkipEventTime) / 1000 + 1) >= 1; // greater than or equal 1 count/s
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ChannelAttributeKey)) return false;
        ChannelAttributeKey that = (ChannelAttributeKey) o;
        return heartBeat == that.heartBeat &&
                Objects.equals(gate, that.gate);
    }

    @Override
    public int hashCode() {

        return Objects.hash(gate, heartBeat);
    }

    @Override
    public String toString() {
        return "ChannelAttributeKey{" +
                "gate=" + gate +
                ", heartBeat=" + heartBeat +
                ", firstSkipEventTime=" + firstSkipEventTime +
                ", skipCountInARow=" + skipCountInARow +
                '}';
    }
}
