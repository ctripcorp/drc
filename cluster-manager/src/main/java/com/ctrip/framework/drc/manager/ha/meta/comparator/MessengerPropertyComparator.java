package com.ctrip.framework.drc.manager.ha.meta.comparator;

import com.ctrip.framework.drc.core.entity.Messenger;
import com.ctrip.framework.drc.core.meta.comparator.AbstractMetaComparator;

import java.util.Objects;

/**
 * Created by jixinwang on 2023/8/31
 */
public class MessengerPropertyComparator extends AbstractMetaComparator<Messenger, MessengerChange> {

    private Messenger current, future;

    public MessengerPropertyComparator(Messenger current, Messenger future) {
        this.current = current;
        this.future = future;
    }

    @Override
    public void compare() {
        if (!Objects.equals(current.getNameFilter(), future.getNameFilter())
                || !Objects.equals(current.getProperties(), future.getProperties())) {
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
