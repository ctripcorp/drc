package com.ctrip.framework.drc.replicator.store.manager.file;

import java.util.Objects;

/**
 * @author yongnian
 * @create 2024/9/14 15:37
 */
public class BinlogPosition implements Comparable<BinlogPosition> {


    private long fileSeq;
    private long position;

    public static BinlogPosition from(long fileSeq, long position) {
        if (fileSeq < 0 || position < 0) {
            throw new IllegalArgumentException("fileSeq and position must be greater than or equal to 0");
        }
        return new BinlogPosition(fileSeq, position);
    }

    /**
     * will move forward
     */
    public static BinlogPosition empty() {
        return new BinlogPosition(-1, -1);
    }

    private BinlogPosition(long fileSeq, long position) {
        this.fileSeq = fileSeq;
        this.position = position;
    }

    public boolean tryMoveForward(BinlogPosition binlogPosition) {
        if (!canMoveForward(binlogPosition)) {
            return false;
        }
        this.position = binlogPosition.position;
        this.fileSeq = binlogPosition.fileSeq;
        return true;
    }

    public boolean canMoveForward(BinlogPosition other) {
        return this.compareTo(other) < 0;
    }

    public boolean tryMoveForward(long position) {
        if (this.position >= position) {
            return false;
        }
        this.position = position;
        return true;
    }

    public long getFileSeq() {
        return fileSeq;
    }

    public long getPosition() {
        return position;
    }

    @Override
    public int compareTo(BinlogPosition o) {
        if (this.fileSeq != o.fileSeq) {
            return Long.compare(this.fileSeq, o.fileSeq);
        } else {
            return Long.compare(this.position, o.position);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof BinlogPosition)) return false;
        BinlogPosition that = (BinlogPosition) o;
        return fileSeq == that.fileSeq && position == that.position;
    }

    @Override
    public int hashCode() {
        return Objects.hash(fileSeq, position);
    }

    public long getGap(BinlogPosition another) {
        if (this.fileSeq != another.fileSeq) {
            return Integer.MAX_VALUE;
        }
        return Math.abs(this.position - another.position);
    }

    @Override
    public String toString() {
        return "[" + fileSeq + ":" + position + "]";
    }
}
