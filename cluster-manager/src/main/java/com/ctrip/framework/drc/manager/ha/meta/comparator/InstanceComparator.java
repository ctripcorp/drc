package com.ctrip.framework.drc.manager.ha.meta.comparator;

import com.ctrip.framework.drc.core.entity.Instance;
import com.ctrip.framework.drc.core.meta.comparator.AbstractMetaComparator;

/**
 * @Author limingdong
 * @create 2020/4/29
 */
public class InstanceComparator extends AbstractMetaComparator<Instance, InstanceChange> {

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

