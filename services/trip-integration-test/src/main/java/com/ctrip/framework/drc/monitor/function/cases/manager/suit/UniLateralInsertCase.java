package com.ctrip.framework.drc.monitor.function.cases.manager.suit;

import com.ctrip.framework.drc.core.monitor.cases.PairCase;
import com.ctrip.framework.drc.core.monitor.operator.ReadResource;
import com.ctrip.framework.drc.core.monitor.operator.ReadSqlOperator;
import com.ctrip.framework.drc.monitor.function.operator.ReadWriteSqlOperator;
import com.ctrip.framework.idgen.client.IdGeneratorFactory;
import com.ctrip.platform.dal.sharding.idgen.LongIdGenerator;

import java.util.Collections;
import java.util.List;

public class UniLateralInsertCase extends AbstractMultiWriteInTransactionPairCase implements PairCase<ReadWriteSqlOperator, ReadSqlOperator<ReadResource>> {
    public static final String DRC_SEQUENCE_NAME = "DRC_integration_test";
    LongIdGenerator generator = IdGeneratorFactory.getInstance().getOrCreateLongIdGenerator(DRC_SEQUENCE_NAME);

    @Override
    protected List<String> getStatements() {
        String sql = String.format("insert into `test`.`customer`(`id`, `name`, `gender`) values(%s, 'name1', 'male');", generator.nextId());
        return Collections.singletonList(sql);
    }
}
