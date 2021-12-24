package com.ctrip.framework.drc.applier.event;

/**
 * @Author Slight
 * Oct 16, 2019
 */
public class MockGtidEvent extends ApplierGtidEvent {
    public String gtid;
    public long lastCommitted;
    public long sequenceNumber;

    public MockGtidEvent(String gtid, long lastCommitted, long sequenceNumber) {
        this.gtid = gtid;
        this.lastCommitted = lastCommitted;
        this.sequenceNumber = sequenceNumber;
    }

    @Override
    public String getGtid() {
        return gtid;
    }

    @Override
    public long getLastCommitted() {
        return lastCommitted;
    }

    @Override
    public long getSequenceNumber() {
        return sequenceNumber;
    }
}
