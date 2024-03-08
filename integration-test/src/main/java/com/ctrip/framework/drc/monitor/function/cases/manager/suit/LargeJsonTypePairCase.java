package com.ctrip.framework.drc.monitor.function.cases.manager.suit;

import com.ctrip.framework.drc.core.monitor.cases.PairCase;
import com.ctrip.framework.drc.core.monitor.operator.ReadResource;
import com.ctrip.framework.drc.core.monitor.operator.ReadSqlOperator;
import com.ctrip.framework.drc.monitor.function.cases.InsertCase;
import com.ctrip.framework.drc.monitor.function.cases.insert.MultiStatementWriteCase;
import com.ctrip.framework.drc.monitor.function.execution.WriteExecution;
import com.ctrip.framework.drc.monitor.function.execution.insert.MultiStatementWriteExecution;
import com.ctrip.framework.drc.monitor.function.operator.ReadWriteSqlOperator;
import com.ctrip.framework.drc.monitor.utils.enums.DmlTypeEnum;
import com.google.common.collect.Lists;

import java.util.List;

public class LargeJsonTypePairCase extends AbstractMultiWriteInTransactionPairCase implements PairCase<ReadWriteSqlOperator, ReadSqlOperator<ReadResource>> {

    private static final String INSERT_FORMATTER = "insert into `drc1`.`json` (`id`,`json_data`) value (%s , %s);";
    private static final String JSON_SAMPLE = "{\"age\": 35, \"name\": \"Bob\", \"height\": 1.87, \"address\": {\"city\": \"Los Angeles\", \"state\": \"CA\"}}";


    public LargeJsonTypePairCase () {
        initResource(initInsert(),initUpdate(),initDelete(),"select * from `drc1`.`json` where id >= 100;");
    }

    private int roundCount=0;

    @Override
    protected void doWrite(ReadWriteSqlOperator src, DmlTypeEnum dmlType) {
        if (dmlType == DmlTypeEnum.DELETE) { // final dmlType
            if (++roundCount % 5 == 0) {
                roundCount = 0;
            }
        }
        if (roundCount !=0) {
            return; // skip this round,for test observe
        }
        List<String> statements = getStatements(dmlType);
        for (int i = 0; i < statements.size(); i++) {
            WriteExecution writeExecution = new MultiStatementWriteExecution(Lists.newArrayList(statements.get(i)));
            InsertCase insertCase = new MultiStatementWriteCase(writeExecution);
            boolean affected = insertCase.executeInsert(src);
            if (!affected) {
                alarm.alarm(String.format("%s execute %s error %s", getClass().getSimpleName(), dmlType.getDescription(), writeExecution.getStatements()));
            }
        }
        parkThreeSecond();

    }

    // select json_type(Json_extract(json_data, '$.time')) as t from drc1.json where id =4
    // todo small or large object/array lack of literal/(u)int in-lined-value case cover,lack of tableMapLogEvent load case

    // select json_type(Json_extract(json_data, '$.time')) as t from drc1.json where id =4
    private List<String> initInsert() {
        StringBuilder bigArray = new StringBuilder("[");
        for (int i = 0; i < 1000; i++) {
            if (i != 0) {
                bigArray.append(", ");
            }
            bigArray.append(JSON_SAMPLE);
        }
        bigArray.append(", null");
        bigArray.append(", false");
        bigArray.append(", true");
        bigArray.append(", 0");
        bigArray.append(", -32768");
        bigArray.append(", 65535");
        bigArray.append(", -2147483648");
        bigArray.append(", 4294967295");
        bigArray.append(", -9223372036854775808");
        bigArray.append(", 18446744073709551615");
        bigArray.append("]");
        String LARGE_ARRAY_TEST = String.format(INSERT_FORMATTER,100,"'" + bigArray.toString() + "'");

        StringBuilder bigJson = new StringBuilder("{\"data\": {\"users\": ");
        bigJson.append(bigArray);
        bigJson.append("}");
        bigJson.append(", \"literal0\": null");
        bigJson.append(", \"literal1\": true");
        bigJson.append(", \"literal2\": false");
        bigJson.append(", \"int\": 0");
        bigJson.append(", \"int16\": -32768");
        bigJson.append(", \"uint16\": 65535");
        bigJson.append(", \"int32\": -2147483648");
        bigJson.append(", \"uint32\": 4294967295");
        bigJson.append(", \"int64\": -9223372036854775808");
        bigJson.append(", \"uint64\": 18446744073709551615");
        bigJson.append("}");
        String LARGE_OBJ_TEST = String.format(INSERT_FORMATTER,101,"'" + bigJson + "'");


        // add all String test
        return Lists.newArrayList(
                LARGE_ARRAY_TEST,
                LARGE_OBJ_TEST
        );
    }

    private List<String> initUpdate() {

        String OPAQUE_LARGE_ARR_TEST = "update drc1.json set json_data='[\"test_json\",{\"sss\": \"eee\", \"age\": 100}]' where id =100;";
        String OPAQUE_LARGE_OBJ_TEST = "update drc1.json set json_data='{\"name\": \"eese\", \"age\": 101, \"address\": {\"city\": \"Los Angeles0000\", \"state\": \"CA00\"}}' where id =101;";

        return Lists.newArrayList(
                OPAQUE_LARGE_ARR_TEST,
                OPAQUE_LARGE_OBJ_TEST
        );
    }

    private List<String> initDelete() {
        return Lists.newArrayList(
                "DELETE FROM drc1.json WHERE id = 100;",
                "DELETE FROM drc1.json WHERE id = 101;"
        );
    }

}
