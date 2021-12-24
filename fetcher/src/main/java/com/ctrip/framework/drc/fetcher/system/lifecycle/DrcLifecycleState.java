package com.ctrip.framework.drc.fetcher.system.lifecycle;

import com.ctrip.xpipe.api.lifecycle.Disposable;
import com.ctrip.xpipe.api.lifecycle.Initializable;
import com.ctrip.xpipe.api.lifecycle.Startable;
import com.ctrip.xpipe.api.lifecycle.Stoppable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicReference;

/**
 * @Author Slight
 * Jun 22, 2020
 */
public class DrcLifecycleState implements LifecycleState {

    protected final Logger logger = LoggerFactory.getLogger(getClass());

    protected AtomicReference<String> phaseName = new AtomicReference<>();

    protected AtomicReference<String> previousPhaseName = new AtomicReference<>();

    @Override
    public boolean isEmpty() {
        return phaseName.get() == null;
    }

    @Override
    public boolean isInitializing() {
        String phaseName = getPhaseName();
        return phaseName != null && phaseName.equals(Initializable.PHASE_NAME_BEGIN);
    }

    @Override
    public boolean isInitialized() {
        String phaseName = getPhaseName();
        return phaseName != null && phaseNameIn(phaseName,
                Initializable.PHASE_NAME_END,
                Startable.PHASE_NAME_BEGIN,
                Startable.PHASE_NAME_END,
                Stoppable.PHASE_NAME_BEGIN,
                Stoppable.PHASE_NAME_END);
    }

    @Override
    public boolean isStarting() {
        String phaseName = getPhaseName();
        return phaseName != null && phaseName.equals(Startable.PHASE_NAME_BEGIN);
    }

    @Override
    public boolean isStarted() {
        String phaseName = getPhaseName();
        return phaseName != null && phaseName.equals(Startable.PHASE_NAME_END);
    }

    @Override
    public boolean isStopping() {
        String phaseName = getPhaseName();
        return phaseName != null && phaseName.equals(Stoppable.PHASE_NAME_BEGIN);
    }

    @Override
    public boolean isStopped() {
        String phaseName = getPhaseName();
        return phaseName == null ||
                (	phaseNameIn(phaseName,
                        Initializable.PHASE_NAME_END,
                        Stoppable.PHASE_NAME_END,
                        Disposable.PHASE_NAME_BEGIN,
                        Disposable.PHASE_NAME_END));
    }

    @Override
    public boolean isPositivelyStopped() {
        String phaseName = getPhaseName();
        return phaseName != null && phaseNameIn(phaseName, Stoppable.PHASE_NAME_END, Disposable.PHASE_NAME_BEGIN, Disposable.PHASE_NAME_END);
    }

    private boolean phaseNameIn(String phaseName, String ... ins) {
        for(String in : ins){
            if(phaseName.equals(in)){
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean isDisposing() {
        String phaseName = getPhaseName();
        return phaseName != null && phaseName.equals(Disposable.PHASE_NAME_BEGIN);
    }

    @Override
    public boolean isDisposed() {
        String phaseName = getPhaseName();
        return phaseName == null  || (phaseName.equals(Disposable.PHASE_NAME_END));
    }

    @Override
    public boolean isPositivelyDisposed() {
        String phaseName = getPhaseName();
        return phaseName != null && phaseNameIn(getPhaseName(), Disposable.PHASE_NAME_END);
    }

    @Override
    public String getPhaseName() {
        return phaseName.get();
    }

    @Override
    public void setPhaseName(String name) {
        logger.info("[phase]{}, {} --> {}", toString(), phaseName, name);
        previousPhaseName.set(phaseName.get());
        phaseName.set(name);
    }

    @Override
    public void rollback(Exception e) {
        logger.info("[rollback]{},{} -> {}, reason:{}", toString(), phaseName, previousPhaseName, e.getMessage());
        phaseName.set(previousPhaseName.get());
    }
}
