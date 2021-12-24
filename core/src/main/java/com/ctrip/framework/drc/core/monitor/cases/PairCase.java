package com.ctrip.framework.drc.core.monitor.cases;

import com.ctrip.framework.drc.core.monitor.operator.ReadSqlOperator;

/**
 * 测试对，<insert, select> <update, select> <delete, select>
 * Created by mingdongli
 * 2019/10/9 下午11:58.
 */
public interface PairCase<S extends ReadSqlOperator, D extends ReadSqlOperator> {

    void test(S src, D dst);
}
