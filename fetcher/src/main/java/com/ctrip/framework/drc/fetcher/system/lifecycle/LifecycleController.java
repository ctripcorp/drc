package com.ctrip.framework.drc.fetcher.system.lifecycle;

import com.ctrip.xpipe.api.lifecycle.Disposable;
import com.ctrip.xpipe.api.lifecycle.Initializable;
import com.ctrip.xpipe.api.lifecycle.Startable;
import com.ctrip.xpipe.api.lifecycle.Stoppable;

/**
 * @Author Slight
 * Jun 22, 2020
 */
public interface LifecycleController extends LifecycleState {

    default boolean canInitialize() {
        return canInitialize(getPhaseName());
    }

    default boolean canStart() {
        return canStart(getPhaseName());
    }

    default boolean canStop() {
        return canStop(getPhaseName());
    }

    default boolean canDispose() {
        return canDispose(getPhaseName());
    }

    default boolean canInitialize(String phaseName) {
        return phaseName == null || phaseName.equals(Disposable.PHASE_NAME_END);
    }

    default boolean canStart(String phaseName) {
        return phaseName != null  &&
                (phaseName.equals(Initializable.PHASE_NAME_END) ||
                        phaseName.equals(Stoppable.PHASE_NAME_END));
    }

    default boolean canStop(String phaseName) {
        return phaseName != null  &&
                (phaseName.equals(Startable.PHASE_NAME_END));
    }

    default boolean canDispose(String phaseName) {
        return phaseName != null  &&
                (phaseName.equals(Initializable.PHASE_NAME_END) ||
                        phaseName.equals(Stoppable.PHASE_NAME_END)
                );
    }
}
