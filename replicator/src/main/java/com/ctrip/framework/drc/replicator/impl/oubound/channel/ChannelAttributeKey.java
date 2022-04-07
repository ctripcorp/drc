package com.ctrip.framework.drc.replicator.impl.oubound.channel;

import com.ctrip.xpipe.utils.Gate;

import java.util.Objects;

/**
 * @Author limingdong
 * @create 2022/4/7
 */
public class ChannelAttributeKey {

    private Gate gate;

    private boolean heartBeat;

    public ChannelAttributeKey(Gate gate) {
        this(gate, true);
    }

    public ChannelAttributeKey(Gate gate, boolean heartBeat) {
        this.gate = gate;
        this.heartBeat = heartBeat;
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
                '}';
    }
}
