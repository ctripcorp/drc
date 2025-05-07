package com.ctrip.framework.drc.manager.ha.meta.comparator;

import com.ctrip.framework.drc.core.entity.Applier;
import com.ctrip.framework.drc.core.meta.comparator.AbstractMetaComparator;

import java.util.Objects;

/**
 * Created by jixinwang on 2023/8/29
 */
public class ApplierPropertyComparator extends AbstractMetaComparator<Applier, ApplierChange> {

    private Applier current, future;

    public ApplierPropertyComparator(Applier current, Applier future) {
        this.current = current;
        this.future = future;
    }

    @Override
    public void compare() {
        if (!Objects.equals(current.getRouteInfo(), future.getRouteInfo())) {
            added.add(future);
        }
    }

    @Override
    public String idDesc() {
        return null;
    }

    public Object getCurrent() {
        return current;
    }

    public Object getFuture() {
        return future;
    }
}
