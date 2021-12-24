package com.ctrip.framework.drc.console.mock.helpers;

/**
 * @author shenhaibo
 * @version 1.0
 * date: 2020-08-21
 */
public interface MetaComparatorVisitor<T> {

    void visitAdded(T added);

    void visitModified(@SuppressWarnings("rawtypes") MetaComparator comparator);

    void visitRemoved(T removed);
}
