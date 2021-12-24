package com.ctrip.framework.drc.core.meta.comparator;

/**
 * @Author limingdong
 * @create 2020/4/21
 */
public interface MetaComparatorVisitor<T> {

    void visitAdded(T added);

    void visitModified(@SuppressWarnings("rawtypes") MetaComparator comparator);

    void visitRemoved(T removed);
}
