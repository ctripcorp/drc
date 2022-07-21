package com.ctrip.framework.drc.applier.resource.condition;

import com.ctrip.framework.drc.core.monitor.reporter.DefaultEventMonitorHolder;
import com.ctrip.framework.drc.fetcher.resource.condition.LWM;
import com.ctrip.framework.drc.fetcher.resource.condition.LWMPassHandler;
import com.ctrip.framework.drc.fetcher.system.AbstractResource;
import com.ctrip.framework.drc.fetcher.system.InstanceResource;
import com.ctrip.xpipe.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.NavigableSet;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.AbstractQueuedLongSynchronizer;

/**
 * @Author Slight
 * Sep 19, 2019
 */
public class LWMResource extends AbstractResource implements LWM {

    private final Logger loggerEGB = LoggerFactory.getLogger("EVENT GROUP BEGIN");

    protected volatile Chart<Long, LWMPassHandler> chart = new InnerChart();
    protected volatile Bucket bucket = new InnerBucket();
    protected volatile Notifier notifier = new InnerNotifier();

    protected long lastSequenceNumber = 0;
    protected boolean first = true;

    @InstanceResource
    public Progress progress;

    @Override
    public void doDispose() {
        chart = null;
        bucket = null;
        notifier = null;
    }

    @Override
    public void doInitialize() {
        reset();
    }

    @Override
    public void acquire(long sequenceNumber) throws InterruptedException {
        assert sequenceNumber > 0;
        if (sequenceNumber <= lastSequenceNumber) {
            //sequence number zeroed (<) or applier reconnect (==)
            while(!isClean()) {
                Thread.sleep(0, 500);
            }
            reset();
        }
        if (first) {
            //pass first begin event
            first = false;
            doAcquire(sequenceNumber - 1);
            doCommit(sequenceNumber - 1);
            loggerEGB.info("first event - lwm: " + bucket.lwm() +
                    " sequence number: " + sequenceNumber);
        } else if (sequenceNumber > lastSequenceNumber + 1) {
            //compensate sequence gap
            for (long i = lastSequenceNumber + 1; i < sequenceNumber; i++) {
                doAcquire(i);
                doCommit(i);
            }
            loggerEGB.info("compensate gap [{}, {}) - lwm: {} sequence number: {}",
                    lastSequenceNumber + 1, sequenceNumber, bucket.lwm(), sequenceNumber);
        }
        lastSequenceNumber = sequenceNumber;
        doAcquire(sequenceNumber);
    }

    private void doAcquire(long sequenceNumber) {
        chart.add(sequenceNumber);
    }

    @Override
    public void commit(long sequenceNumber) throws InterruptedException {
        assert sequenceNumber > bucket.lwm() : "sequence number(" + sequenceNumber + ") must be larger than lwm(" + bucket.lwm() + ")";
        doCommit(sequenceNumber);
    }

    private void doCommit(long sequenceNumber) throws InterruptedException {
        Pair<Long, Boolean> result = bucket.add(sequenceNumber);
        if (result.getValue()) {
            long lwm = result.getKey();
            notifier.offsetIncreased(lwm);
            chart.tick(lwm);
        }
    }

    @Override
    public boolean tryPass(long lastCommitted) throws InterruptedException {
        return notifier.await(lastCommitted, 0);
    }

    @Override
    public void onCommit(long lastCommitted, LWMPassHandler handler, String... identifier) throws InterruptedException {
        LWMPassHandler once = new LWMPassHandlerOnce(handler);
        if (tryPass(lastCommitted)) {
            once.onBegin();
        } else {
            boolean success0, success1;
            success0 = chart.add(lastCommitted, once);
            success1 = tryPass(lastCommitted);
            if (success1) {
                once.onBegin();
            }
            if (!success0 && !success1) {
                for (String i0 : identifier) {
                    logger.warn("drop {}, looping to backup", i0);
                    DefaultEventMonitorHolder.getInstance().logBatchEvent("alert",
                            "lwm drop " + i0, 1, 0);
                }
                notifier.await(lastCommitted);
                once.onBegin();
            }
        }
    }

    @Override
    public long current() {
        if (bucket == null)
            return 0;
        return bucket.lwm();
    }

    protected boolean isClean() {
        return chart.size() == 0;
    }

    protected void reset() {
        chart = new InnerChart();
        bucket = new InnerBucket();
        notifier = new InnerNotifier();
        first = true;
        lastSequenceNumber = 0;
        if (progress != null)
            progress.clear();
    }

    public static class LWMPassHandlerOnce implements LWMPassHandler {

        private final AtomicBoolean executed = new AtomicBoolean(false);
        private final LWMPassHandler handler;

        public LWMPassHandlerOnce(LWMPassHandler handler) {
            this.handler = handler;
        }

        @Override
        public void onBegin() throws InterruptedException {
            if (executed.compareAndSet(false, true)) {
                handler.onBegin();
            }
        }
    }

    public interface Chart<T, H> {
        boolean remained(T i);

        void add(T i);

        boolean add(T i, H handler);

        void tick(T i) throws InterruptedException;

        int size();
    }

    public static class InnerChart implements Chart<Long, LWMPassHandler> {

        protected ConcurrentNavigableMap<Long, Queue<LWMPassHandler>> chart = new ConcurrentSkipListMap<>();
        protected static final Logger logger = LoggerFactory.getLogger(InnerChart.class);

        @Override
        public int size() {
            return chart.size();
        }

        @Override
        public boolean remained(Long i) {
            return chart.containsKey(i);
        }

        @Override
        public void add(Long i) {
            chart.putIfAbsent(i, new ConcurrentLinkedDeque<>());
        }

        @Override
        public boolean add(Long i, LWMPassHandler handler) {
            Queue<LWMPassHandler> handlers = chart.get(i);
            if (handlers != null) {
                boolean result = handlers.offer(handler);
                if (!result) {
                    logger.warn("UNLIKELY in LWM, failing offer into an unlimited queue, last committed {}", i);
                    return false;
                }
                return chart.containsKey(i);
            }
            return false;
        }

        @Override
        public void tick(Long lwm) throws InterruptedException {
            ConcurrentNavigableMap<Long, Queue<LWMPassHandler>> navigableMap = chart.headMap(lwm, true);
            if (navigableMap.isEmpty()) {
                return;
            }
            NavigableSet<Long> keySet = navigableMap.keySet();
            for (Long sn : keySet) {
                Queue<LWMPassHandler> handlers = chart.remove(sn);
                if (handlers != null) {
                    LWMPassHandler next;
                    while ((next = handlers.poll()) != null) {
                        next.onBegin();
                    }
                }
            }
        }
    }

    public interface Notifier {

        void await(long startOffset) throws InterruptedException;

        boolean await(long startOffset, long milliSeconds) throws InterruptedException;

        void offsetIncreased(long newOffset);
    }

    public static class InnerNotifier implements Notifier {

        private final Sync sync = new Sync();

        public void await(long startOffset) throws InterruptedException {
            sync.acquireSharedInterruptibly(startOffset);
        }

        @Override
        public boolean await(long startOffset, long milliSeconds) throws InterruptedException {
            return sync.tryAcquireSharedNanos(startOffset, milliSeconds * (1000*1000));
        }

        @Override
        public void offsetIncreased(long newOffset) {
            sync.releaseShared(newOffset);
        }
    }

    private static final class Sync extends AbstractQueuedLongSynchronizer {

        @Override
        protected long tryAcquireShared(long startOffset) {
            return (getState() >= startOffset) ? 1 : -1;
        }

        @Override
        protected boolean tryReleaseShared(long newOffset) {
            while(true) {
                long current = getState();
                if (current >= newOffset)
                    return false;
                if (compareAndSetState(current, newOffset))
                    return true;
            }
        }
    }

    public interface Bucket {
        Pair<Long, Boolean> add(long water);
        long lwm();
    }

    public static class InnerBucket implements Bucket {

        private volatile long lwm;

        protected ArrayList<Long> list;

        public InnerBucket() {
            list = new ArrayList<>(256);
        }

        //Valid:
        //Every time when add() is invoked(), list[0] must be filled, representing low water mark,
        // list[1] must be larger than list[0]+1, indicating a gap.
        // Take [3,5,6] as an example, list[0] is filled, 3 is the low water mark, list[1] is
        // larger than list[0]+1 (5 > (3+1)). So we say [3, 5, 6] is valid after add() is called.
        //
        //Invalid:
        //Take [3,4,6,8] as an example, list[1] which is 4 is not larger than 3+1. It is invalid.
        // As 4 is seen as the lwm, 3 should be removed from the list, and the valid state should
        // be [4,6,8].
        @Override
        public synchronized Pair<Long, Boolean> add(long water) {
            int size = list.size();
            if (size == 0) {
                list.add(0, water);
                lwm = water;
                return Pair.from(water, true);
            }
            if (water < list.get(0)) {
                return Pair.from(list.get(0), false);
            }
            int index = search(water, 0, size);
            if (list.size() == index) {
                list.ensureCapacity(index + 1);
                list.add(index, water);
            }
            if (water != list.get(index)) {
                list.ensureCapacity(index + 1);
                list.add(index, water);
            }
            long newLwm = shiftTilLwm();

            Pair<Long, Boolean> result = Pair.from(newLwm, false);
            if (newLwm > lwm) {
                lwm = newLwm;
                result.setValue(true);
            }
            return result;
        }

        @Override
        public long lwm() {
            return lwm;
        }

        //Take [0, 1, 4] as an example, head is 0 and tail is 3.
        public int search(long n, int head, int tail) {
            if (head == tail)
                return head;
            int mid = (head + tail) / 2;
            long midValue = list.get(mid);
            if (n <= midValue) {
                return search(n, head, mid);
            } else {
                return search(n, mid + 1, tail);
            }
        }

        public boolean isHeadLwm() {
            //And when isHeadLwm() is called, list.size() must be larger than 1 (>=2).
            return !(list.get(0) == list.get(1) - 1);
        }

        public long shiftTilLwm() {
            //As shiftTilLwm() is called at the bottom of add(),
            // here list.size() here must be larger than 0 (>=1)
            while ((list.size() > 1) && (!isHeadLwm())) {
                list.remove(0);
            }
            return list.get(0);
        }
    }
}
