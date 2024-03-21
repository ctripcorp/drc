package com.ctrip.framework.drc.console.monitor.gtid.function;

import java.util.Objects;

/**
 * An interval of contiguous gap transaction identifiers.
 */
public final class GapInterval {

    long start;
    long end;

    public GapInterval(long start, long end) {
        this.start = start;
        this.end = end;
    }

    /**
     * Get the starting transaction number in this interval.
     *
     * @return this interval's first transaction number
     */
    public long getStart() {
        return start;
    }

    /**
     * Get the ending transaction number in this interval.
     *
     * @return this interval's last transaction number
     */
    public long getEnd() {
        return end;
    }

    @Override
    public String toString() {
        return start + "-" + end;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof GapInterval)) return false;
        GapInterval that = (GapInterval) o;
        return getStart() == that.getStart() &&
                getEnd() == that.getEnd();
    }

    @Override
    public int hashCode() {
        return Objects.hash(getStart(), getEnd());
    }
}
