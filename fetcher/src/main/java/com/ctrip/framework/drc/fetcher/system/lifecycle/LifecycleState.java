package com.ctrip.framework.drc.fetcher.system.lifecycle;

/**
 * @Author Slight
 * Jun 22, 2020
 */
public interface LifecycleState {
    boolean isEmpty();

    boolean isInitializing();

    boolean isInitialized();

    boolean isStarting();

    boolean isStarted();

    boolean isStopping();

    boolean isStopped();

    boolean isPositivelyStopped();

    boolean isDisposing();

    boolean isDisposed();

    boolean isPositivelyDisposed();

    String getPhaseName();

    void setPhaseName(String name);

    void rollback(Exception e);
}
