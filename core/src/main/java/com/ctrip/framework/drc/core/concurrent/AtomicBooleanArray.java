package com.ctrip.framework.drc.core.concurrent;

import java.lang.invoke.VarHandle;
import java.util.concurrent.atomic.AtomicIntegerArray;

/**
 * jdk doesn't have AtomicBooleanArray, so we implement it.
 *
 * @author yongnian
 * @create 2024/12/31 16:21
 */
public class AtomicBooleanArray {
    private final AtomicIntegerArray delegate;

    /**
     * Creates a new AtomicIntegerArray of the given length, with all
     * elements initially zero.
     *
     * @param length the length of the array
     */
    public AtomicBooleanArray(int length) {
        delegate = new AtomicIntegerArray(length);
    }

    private boolean intToBoolean(int aInt) {
        return aInt == 1;
    }

    private int booleanToInt(boolean aBoolean) {
        return aBoolean ? 1 : 0;
    }


    public final int length() {
        return delegate.length();
    }

    /**
     * Returns the current value of the element at index {@code i},
     * with memory effects as specified by {@link VarHandle#getVolatile}.
     *
     * @param i the index
     * @return the current value
     */
    public final boolean get(int i) {
        return intToBoolean(delegate.get(i));
    }


    /**
     * Sets the element at index {@code i} to {@code newValue},
     * with memory effects as specified by {@link VarHandle#setVolatile}.
     *
     * @param i        the index
     * @param newValue the new value
     */
    public final void set(int i, boolean newValue) {
        delegate.set(i, booleanToInt(newValue));
    }

    /**
     * Sets the element at index {@code i} to {@code newValue},
     * with memory effects as specified by {@link VarHandle#setRelease}.
     *
     * @param i        the index
     * @param newValue the new value
     * @since 1.6
     */
    public final void lazySet(int i, boolean newValue) {
        delegate.lazySet(i, booleanToInt(newValue));
    }

    /**
     * Atomically sets the element at index {@code i} to {@code
     * newValue} and returns the old value,
     * with memory effects as specified by {@link VarHandle#getAndSet}.
     *
     * @param i        the index
     * @param newValue the new value
     * @return the previous value
     */
    public final boolean getAndSet(int i, boolean newValue) {
        return intToBoolean(delegate.getAndSet(i, booleanToInt(newValue)));
    }

    /**
     * Atomically sets the element at index {@code i} to {@code
     * newValue} if the element's current value {@code == expectedValue},
     * with memory effects as specified by {@link VarHandle#compareAndSet}.
     *
     * @param i             the index
     * @param expectedValue the expected value
     * @param newValue      the new value
     * @return {@code true} if successful. False return indicates that
     * the actual value was not equal to the expected value.
     */
    public final boolean compareAndSet(int i, boolean expectedValue, boolean newValue) {
        return delegate.compareAndSet(i, booleanToInt(expectedValue), booleanToInt(newValue));
    }

    /**
     * Possibly atomically sets the element at index {@code i} to
     * {@code newValue} if the element's current value {@code == expectedValue},
     * with memory effects as specified by {@link VarHandle#weakCompareAndSetPlain}.
     *
     * @param i             the index
     * @param expectedValue the expected value
     * @param newValue      the new value
     * @return {@code true} if successful
     * @since 9
     */
    public final boolean weakCompareAndSetPlain(int i, boolean expectedValue, boolean newValue) {
        return delegate.weakCompareAndSetPlain(i, booleanToInt(expectedValue), booleanToInt(newValue));
    }


    /**
     * Returns the String representation of the current values of array.
     *
     * @return the String representation of the current values of array
     */
    public String toString() {
        int iMax = delegate.length() - 1;
        if (iMax == -1)
            return "[]";

        StringBuilder b = new StringBuilder();
        b.append('[');
        for (int i = 0; ; i++) {
            b.append(get(i));
            if (i == iMax)
                return b.append(']').toString();
            b.append(',').append(' ');
        }
    }
}
