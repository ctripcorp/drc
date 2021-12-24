package com.ctrip.framework.drc.applier.resource.condition;

import com.ctrip.xpipe.tuple.Pair;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * @Author Slight
 * Sep 24, 2019
 */
public class LWMResourceInnerBucketTest {

    private LWMResource.InnerBucket bucket;

    @Before
    public void setUp() {
        bucket = new LWMResource.InnerBucket();
        bucket.list.add((long) 10);
        bucket.list.add((long) 12);
        bucket.list.add((long) 13);
        bucket.list.add((long) 19);
        bucket.list.add((long) 20);
        bucket.list.add((long) 21);
        bucket.add(10);  // init lwm
    }

    @After
    public void tearDown() {
        bucket = null;
    }

    @Test
    public void searchWhenNIsTheLargest() {
        assertEquals(6, bucket.search(23, 0, 6));
    }


    @Test
    public void searchWhenNIsTheSmallest() {
        assertEquals(0, bucket.search(3, 0, 6));
    }

    @Test
    public void searchWhenNHasBeenInTheBucket() {
        assertEquals(1, bucket.search(12, 0, 6));
    }

    @Test
    public void search() {
        assertEquals(1, bucket.search(11, 0, 6));
        assertEquals(3, bucket.search(15, 0, 6));
    }

    @Test
    public void shiftTilLwmWithoutTriggeringRemove() {
        bucket.shiftTilLwm();
        assertArrayEquals(bucket.list.toArray(),
                new Long[]{
                        (long) 10,
                        (long) 12,
                        (long) 13,
                        (long) 19,
                        (long) 20,
                        (long) 21
                });
    }

    @Test
    public void shiftTilLwm() {
        bucket.list.add(0, (long) 5);
        bucket.list.add(1, (long) 6);
        bucket.list.add(2, (long) 7);
        bucket.list.add(3, (long) 8);

        assertArrayEquals(bucket.list.toArray(),
                new Long[]{
                        (long) 5,
                        (long) 6,
                        (long) 7,
                        (long) 8,
                        (long) 10,
                        (long) 12,
                        (long) 13,
                        (long) 19,
                        (long) 20,
                        (long) 21
                });

        assertEquals(8, bucket.shiftTilLwm());

        assertArrayEquals(bucket.list.toArray(),
                new Long[]{
                        (long) 8,
                        (long) 10,
                        (long) 12,
                        (long) 13,
                        (long) 19,
                        (long) 20,
                        (long) 21
                });
    }

    @Test
    public void shiftTilLwmWhenSizeEqualsOne() {
        bucket.list.clear();
        bucket.list.add((long) 5);
        assertArrayEquals(bucket.list.toArray(),
                new Long[]{(long) 5});
        assertEquals(5, bucket.shiftTilLwm());
    }

    @Test
    public void shiftTilLwmWhenEmpty() {
        bucket.list.clear();
        assertArrayEquals(bucket.list.toArray(),
                new Long[]{});
        try {
            bucket.shiftTilLwm();
        } catch (Exception e) {
            assertTrue(e instanceof IndexOutOfBoundsException);
        }
    }

    @Test
    public void addCurrentLwm() {
        Pair<Long, Boolean> result = bucket.add(10);
        assertEquals(10, result.getKey().longValue());
        assertEquals(false, result.getValue());
    }

    @Test
    public void addWhenListIsEmpty() {
        bucket.list.clear();
        assertArrayEquals(bucket.list.toArray(),
                new Long[]{});
        Pair<Long, Boolean> result = bucket.add(5);
        assertEquals(5, result.getKey().longValue());
        assertEquals(true, result.getValue());
    }

    @Test
    public void addWaterAndTriggerANewLwm() {
        Pair<Long, Boolean> result = bucket.add(11);
        assertEquals(13, result.getKey().longValue());
        assertEquals(true, result.getValue());
        assertArrayEquals(bucket.list.toArray(),
                new Long[]{
                        (long) 13,
                        (long) 19,
                        (long) 20,
                        (long) 21
                });
    }

    @Test
    public void addMeaninglessWater() {
        Pair<Long, Boolean> result = bucket.add(8);
        assertEquals(10, result.getKey().longValue());
        assertEquals(false, result.getValue());
        assertArrayEquals(bucket.list.toArray(),
                new Long[]{
                        (long) 10,
                        (long) 12,
                        (long) 13,
                        (long) 19,
                        (long) 20,
                        (long) 21
                });
    }

    @Test
    public void addLargest() {
        Pair<Long, Boolean> result = bucket.add(23);
        assertEquals(10, result.getKey().longValue());
        assertEquals(false, result.getValue());
        assertArrayEquals(bucket.list.toArray(),
                new Long[]{
                        (long) 10,
                        (long) 12,
                        (long) 13,
                        (long) 19,
                        (long) 20,
                        (long) 21,
                        (long) 23
                });

    }

}