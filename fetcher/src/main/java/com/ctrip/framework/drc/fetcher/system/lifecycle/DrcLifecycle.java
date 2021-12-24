package com.ctrip.framework.drc.fetcher.system.lifecycle;

import com.ctrip.xpipe.api.lifecycle.*;

public abstract class DrcLifecycle extends DrcLifecycleState implements Lifecycle, LifecycleController {

    @Override
    public void initialize() throws Exception {

        String phaseName = getPhaseName();
        if(!canInitialize(phaseName)){
            logger.error("[initialize][can not init]{}, {}", phaseName, this);
            throw new IllegalStateException("can not initialize:" + phaseName + "," + this);
        }

        try{
            setPhaseName(Initializable.PHASE_NAME_BEGIN);
            doInitialize();
            setPhaseName(Initializable.PHASE_NAME_END);
        }catch(Exception e){
            rollback(e);
            throw e;
        }
    }

    protected void doInitialize() throws Exception{

    }

    @Override
    public void start() throws Exception {

        String phaseName = getPhaseName();
        if(!canStart(phaseName)){
            logger.error("[initialize][can not start]{},{}", phaseName, this);
            throw new IllegalStateException("can not start:" + phaseName + ", " + this);
        }

        try{
            setPhaseName(Startable.PHASE_NAME_BEGIN);
            doStart();
            setPhaseName(Startable.PHASE_NAME_END);
        }catch(Exception e){
            rollback(e);
            throw e;
        }
    }
    protected void doStart() throws Exception{

    }

    @Override
    public void stop() throws Exception {

        String phaseName = getPhaseName();
        if(!canStop(phaseName)){
            logger.error("[initialize][can not stop]{}, {}" , phaseName, this);
            throw new IllegalStateException("can not stop:" + phaseName + "," + this);
        }

        try{
            setPhaseName(Stoppable.PHASE_NAME_BEGIN);
            doStop();
            setPhaseName(Stoppable.PHASE_NAME_END);
        }catch(Exception e){
            rollback(e);
            throw e;
        }
    }

    protected void doStop() throws Exception{

    }

    @Override
    public void dispose() throws Exception {

        String phaseName = getPhaseName();
        if(!canDispose(phaseName)){
            logger.error("[initialize][can not stop]{}, {}" , phaseName, this);
            throw new IllegalStateException("can not dispose:" + phaseName + "," + this);
        }
        try{
            setPhaseName(Disposable.PHASE_NAME_BEGIN);
            doDispose();
            setPhaseName(Disposable.PHASE_NAME_END);
        }catch(Exception e){
            rollback(e);
            throw e;
        }
    }

    protected void doDispose() throws Exception {

    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "-" + hashCode();
    }

    @Override
    public int getOrder() {
        return 0;
    }
}
