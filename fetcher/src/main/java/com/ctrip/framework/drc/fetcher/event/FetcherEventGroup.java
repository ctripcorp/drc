package com.ctrip.framework.drc.fetcher.event;

import com.ctrip.framework.drc.fetcher.event.config.BigTransactionThreshold;
import com.ctrip.framework.drc.fetcher.event.transaction.EventGroup;
import com.ctrip.framework.drc.fetcher.event.transaction.TerminateEvent;
import com.ctrip.framework.drc.fetcher.event.transaction.TransactionEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @Author Slight
 * Jul 07, 2020
 */
public class FetcherEventGroup implements EventGroup, AutoCloseable {

    protected final Logger logger = LoggerFactory.getLogger(getClass());

    public static final int CAPACITY = BigTransactionThreshold.getInstance().getThreshold();
    private final ReentrantLock lock = new ReentrantLock();
    private final Condition notEmpty = lock.newCondition();
    private final Condition notFull= lock.newCondition();
    private final TransactionEvent[] events = new TransactionEvent[CAPACITY];

    private int writeIndex = 0;
    private int readIndex = 0;
    private int length = 0;

    private boolean isClosed = false;

    private boolean isFulfilled = false;
    private boolean isTerminated = false;
    private boolean isOverflowed = false;
    private final Condition fulfilledOrTerminated = lock.newCondition();

    public volatile boolean hasNext = true;

    public FetcherEventGroup() {
    }

    public FetcherEventGroup(TransactionEvent event) {
        events[0] = event;
        writeIndex = 1;
        length = 1;
    }

    @Override
    public void append(TransactionEvent event) throws InterruptedException {
        lock.lockInterruptibly();
        try {
            while(length == CAPACITY && !isClosed) {
                notFull.await();
            }
            if (isClosed)
                return;
            events[writeIndex] = event;
            writeIndex = (writeIndex + 1) % CAPACITY;
            if (event instanceof TerminateEvent) {
                isTerminated = true;
                fulfilledOrTerminated.signal();
            }
            if (writeIndex == 0 && !isFulfilled) {
                isFulfilled = true;
                if (!isTerminated)
                    isOverflowed = true;
                fulfilledOrTerminated.signal();
            }
            length ++;
            notEmpty.signal();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public TransactionEvent next() throws InterruptedException {
        lock.lockInterruptibly();
        try {
            if (!hasNext)
                return null;
            while(length == 0 && !isClosed) {
                notEmpty.await();
            }
            if (isClosed)
                return null;
            TransactionEvent event = events[readIndex];
            readIndex = (readIndex + 1) % CAPACITY;
            length --;
            if (event instanceof TerminateEvent)
                hasNext = false;
            notFull.signal();
            return event;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void reset() {
        lock.lock();
        try {
            if (isFulfilled) {
                readIndex = writeIndex;
                length = CAPACITY;
                hasNext = true;
                notEmpty.signal();
            } else {
                readIndex = 0;
                length = writeIndex - readIndex;
                hasNext = true;
                if (length > 0)
                    notEmpty.signal();
            }
        } finally {
            //See confirmed.java8.Assert
            lock.unlock();
        }
    }

    @Override
    public boolean isOverflowed() throws InterruptedException {
        lock.lock();
        try {
            while(!isFulfilled && !isTerminated) {
                fulfilledOrTerminated.await();
            }
            return isOverflowed;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public boolean isEmptyTransaction() {
        return (events[1] instanceof TerminateEvent);
    }

    public void close() {
        lock.lock();
        try {
            isClosed = true;
            for (int i = 0; i < CAPACITY; i++) {
                TransactionEvent event = events[i];
                if (event != null)
                    event.release();
                events[i] = null;
            }
            notFull.signal();
            notEmpty.signal();
        } finally {
            lock.unlock();
        }
    }

}
