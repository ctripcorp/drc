package com.ctrip.framework.drc.core.utils;

import java.util.concurrent.locks.AbstractQueuedLongSynchronizer;

/**
 * copy from com.ctrip.xpipe.utils.OffsetNotifier
 * <p>
 * modified to get new offset when notify
 *
 * @see com.ctrip.xpipe.utils.OffsetNotifier
 */
public class OffsetNotifier {
    private static final class Sync extends AbstractQueuedLongSynchronizer {
        private static final long serialVersionUID = 4982264981922014374L;

        Sync(long offset) {
            setState(offset);
        }

        @Override
        protected long tryAcquireShared(long startOffset) {
            return (getState() >= startOffset) ? 1 : -1;
        }

        @Override
        protected boolean tryReleaseShared(long newOffset) {
            setState(newOffset);
            return true;
        }

        public long getCurrentOffset() {
            return getState();
        }
    }

    private final OffsetNotifier.Sync sync;

    public OffsetNotifier(long offset) {
        this.sync = new OffsetNotifier.Sync(offset);
    }

    public void await(long startOffset) throws InterruptedException {
        sync.acquireSharedInterruptibly(startOffset);
    }

    /**
     * notified offset ( >= startOffset) if acquired; -1 if not acquired
     */
    public long await(long startOffset, long miliSeconds) throws InterruptedException {
        if (sync.tryAcquireSharedNanos(startOffset, miliSeconds * (1000 * 1000))) {
            return sync.getCurrentOffset();
        } else {
            return -1;
        }
    }

    public void offsetIncreased(long newOffset) {
        sync.releaseShared(newOffset);
    }
}

