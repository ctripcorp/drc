package com.ctrip.framework.drc.core.meta.comparator;

import java.util.List;
import java.util.Set;

/**
 * @Author limingdong
 * @create 2020/4/21
 */
public interface MetaComparator<T, C extends Enum<C>> {

    Set<T> getAdded();

    Set<T> getRemoved();

    @SuppressWarnings("rawtypes")
    Set<MetaComparator> getMofified();

    void compare();

    List<ConfigChanged<C>> getConfigChanged();

    /**
     * add or remvoed or removed
     * @return
     */
    int totalChangedCount();

    String idDesc();

    void accept(MetaComparatorVisitor<T> visitor);
}
