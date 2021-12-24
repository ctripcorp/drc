package com.ctrip.framework.drc.core.monitor.execution;

import java.util.Collection;

/**
 * Created by mingdongli
 * 2019/10/9 下午5:54.
 */
public interface Execution {

    Collection<String> getStatements();

    /**
     * 追加语句
     * @param statement
     * @return
     */
    boolean appendStatements(Collection<String> statement);

    /**
     * 全部替换，先清空，再添加
     * @param statement
     * @return
     */
    boolean replaceStatements(Collection<String> statement);
}
