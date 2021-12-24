package com.ctrip.framework.drc.console.mock.helpers;

import com.ctrip.framework.drc.core.entity.Instance;

/**
 * @author shenhaibo
 * @version 1.0
 * date: 2020-08-21
 */
public class InstanceComparator extends AbstractMetaComparator<Instance, InstanceChange>{

    @SuppressWarnings("unused")
    private Instance current, future;

    public InstanceComparator(Instance current, Instance future) {
        this.current = current;
        this.future = future;
    }

    @Override
    public void compare() {

    }

    @Override
    public String idDesc() {
        return String.format("%s(%s:%d)", getClass().getSimpleName(), current.getIp(), current.getPort());
    }

    public Object getCurrent() {
        return current;
    }

    public Object getFuture() {
        return future;
    }
}