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


// https://dev.mysql.com/doc/refman/8.0/en/datetime.html

public class JsonTypePairCase extends AbstractMultiWriteInTransactionPairCase implements PairCase<ReadWriteSqlOperator, ReadSqlOperator<ReadResource>> {


    public JsonTypePairCase () {
        initResource(initInsert(),initUpdate(),initDelete(),"select * from `drc1`.`json` where id <= 18;");
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

    // example: select json_type(Json_extract(json_data, '$.time')) as t from drc1.json where id =4
    // uint16,uint32 not cover

    private List<String> initInsert() {
        String LITERAL_TEST = "INSERT INTO drc1.json (id,json_data) value (1, '[null, true ,false]' );";
        // (U)INT16/32/64
        //INSERT INTO drc1.json (id,json_data) value (2, '[0, -32768 ,65535]');
        String INT_TEST = "INSERT INTO drc1.json (id,json_data) value (2, '[0, -32768 ,65535, -2147483648, 4294967295, -9223372036854775808, 18446744073709551615]' );";
        String DOUBLE_TEST = "INSERT INTO drc1.json (id,json_data) value (3, '[0, 3.1111, -1.7976931348623157E+308, 1.7976931348623157E+308]' );";

        String OPAQUE_TIME_TEST0 = "INSERT INTO drc1.json (id,json_data) value (4, '{\"time\": \"ZERO\"}' );";
        String OPAQUE_TIME_TEST1 = "INSERT INTO drc1.json (id,json_data) value (5, '{\"time\": \"MIN\"}' );";
        String OPAQUE_TIME_TEST2 = "INSERT INTO drc1.json (id,json_data) value (6, '{\"time\": \"MAX\"}' );";

        String OPAQUE_DATE_TEST0 = "INSERT INTO drc1.json (id,json_data) value (7, '{\"date\": \"MIN\"}' );";
        String OPAQUE_DATE_TEST1 = "INSERT INTO drc1.json (id,json_data) value (8, '{\"date\": \"MAX\"}' );";

        String OPAQUE_DATETIME_TEST0 = "INSERT INTO drc1.json (id,json_data) value (9, '{\"datetime\": \"MIN\"}' );";
        String OPAQUE_DATETIME_TEST1 = "INSERT INTO drc1.json (id,json_data) value (10, '{\"datetime\": \"MAX\"}' );";

        String OPAQUE_timestamp_TEST0 = "INSERT INTO drc1.json (id,json_data) value (11, '{\"timestamp\": \"MIN\"}' );";
        String OPAQUE_timestamp_TEST1 = "INSERT INTO drc1.json (id,json_data) value (12, '{\"timestamp\": \"MIN\"}' );";

        String OPAQUE_decimal_TEST0 = "INSERT INTO drc1.json (id,json_data) value (13, '{\"decimal\": \"MIN\"}' );";
        String OPAQUE_decimal_TEST1 = "INSERT INTO drc1.json (id,json_data) value (14, '{\"decimal\": \"MAX\"}' );";
        String OPAQUE_decimal_TEST2 = "INSERT INTO drc1.json (id,json_data) value (15, '{\"decimal\": \"zero\"}' );";

        // {"en" : "test_json!@#$~   \n\r\s`", "cn" : "中文测试"}
        String STRING_TEST = "INSERT INTO drc1.json (id,json_data) value (16, '{\"en\": \"test_json!@#$~`\\\\n\", \"cn\": \"中文测试\"}' );";
        //-- {"age": 25, "name": "Alice"}  actually store in mysql
        String SMALL_ARR_TEST = "INSERT INTO drc1.json (id,json_data) value (17, '[\"test_json\",{\"name\": \"Alice\", \"age\": 25} ]' );";
        String SMALL_OBJ_TEST = "INSERT INTO drc1.json (id,json_data) value (18, '{\"name\": \"Bob\", \"age\": 35, \"address\": {\"city\": \"Los Angeles\", \"state\": \"CA\"}}' );";


        // add all String test
        return Lists.newArrayList(
                LITERAL_TEST,
                INT_TEST,
                DOUBLE_TEST,
                OPAQUE_TIME_TEST0,
                OPAQUE_TIME_TEST1,
                OPAQUE_TIME_TEST2,
                OPAQUE_DATE_TEST0,
                OPAQUE_DATE_TEST1,
                OPAQUE_DATETIME_TEST0,
                OPAQUE_DATETIME_TEST1,
                OPAQUE_timestamp_TEST0,
                OPAQUE_timestamp_TEST1,
                OPAQUE_decimal_TEST0,
                OPAQUE_decimal_TEST1,
                OPAQUE_decimal_TEST2,
                STRING_TEST,
                SMALL_ARR_TEST,
                SMALL_OBJ_TEST
        );
    }

    private List<String> initUpdate() {
        String LITERAL_TEST = "UPDATE drc1.json SET json_data = '[false,null,true]' where id =1;";
        String INT_TEST = "UPDATE drc1.json SET json_data = '[0, 0 ,0, 0, 0, 0, 0]' where id =2;";
        String DOUBLE_TEST = "UPDATE drc1.json SET json_data = '[0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]' where id =3;";

        String OPAQUE_TIME_TEST0 = "update drc1.json set json_data=JSON_REPLACE(json_data,'$.time',TIME_FORMAT('00:00:00', '%H:%i:%s.%f')) where id =4;";
        String OPAQUE_TIME_TEST1 = "update drc1.json set json_data=JSON_REPLACE(json_data,'$.time',TIME_FORMAT('-838:59:59', '%H:%i:%s.%f')) where id =5;";
        String OPAQUE_TIME_TEST2 = "update drc1.json set json_data=JSON_REPLACE(json_data,'$.time',TIME_FORMAT('838:59:59', '%H:%i:%s.%f')) where id =6;";

        String OPAQUE_DATE_TEST0 = "update drc1.json set json_data=JSON_REPLACE(json_data,'$.date',DATE('1000-01-01') ) where id =7;";
        String OPAQUE_DATE_TEST1 = "update drc1.json set json_data=JSON_REPLACE(json_data,'$.date',DATE('9999-12-31') ) where id =8;";

        String OPAQUE_DATETIME_TEST0 = "update drc1.json set json_data=JSON_REPLACE(json_data,'$.datetime',STR_TO_DATE('1000-01-01 00:00:00.000000', '%Y-%m-%d %H:%i:%s.%f') ) where id =9;";
        String OPAQUE_DATETIME_TEST1 = "update drc1.json set json_data=JSON_REPLACE(json_data,'$.datetime',STR_TO_DATE('9999-12-31 23:59:59.499999', '%Y-%m-%d %H:%i:%s.%f') ) where id =10;";

        String OPAQUE_timestamp_TEST0 = "update drc1.json set json_data=JSON_REPLACE(json_data,'$.timestamp',TIMESTAMP('1970-01-01 00:00:01.000000') ) where id =11;";
        String OPAQUE_timestamp_TEST1 = "update drc1.json set json_data=JSON_REPLACE(json_data,'$.timestamp',TIMESTAMP('2038-01-19 03:14:07.499999') ) where id =12;";

        String OPAQUE_decimal_TEST0 = "update drc1.json set json_data=JSON_REPLACE(json_data,'$.decimal', CAST(99999999999999999999999999999999999.999999999999999999999999999999 AS DECIMAL(65, 30) ) ) where id =13;";
        String OPAQUE_decimal_TEST1 = "update drc1.json set json_data=JSON_REPLACE(json_data,'$.decimal', CAST(-99999999999999999999999999999999999.999999999999999999999999999999 AS DECIMAL(65, 30) ) ) where id =14;";
        String OPAQUE_decimal_TEST2 = "update drc1.json set json_data=JSON_REPLACE(json_data,'$.decimal', CAST(0 AS DECIMAL(65, 30) )) where id =15;";


        String OPAQUE_STRING_TEST = "update drc1.json set json_data='{\"en\": \"00011ssstge\", \"cn\": \"你好！\"}' where id =16;";
        String OPAQUE_SMALL_ARR_TEST = "update drc1.json set json_data='[\"test_json\",{ \"sss\": \"eee\", \"age\": 1222 }]' where id =17;";
        String OPAQUE_SMALL_OBJ_TEST = "update drc1.json set json_data='{\"name\": \"eese\", \"age\": 350, \"address\": {\"city\": \"Los Angeles0000\", \"state\": \"CA00\"}}' where id =18;";

        return Lists.newArrayList(
                LITERAL_TEST,
                INT_TEST,
                DOUBLE_TEST,
                OPAQUE_TIME_TEST0,
                OPAQUE_TIME_TEST1,
                OPAQUE_TIME_TEST2,
                OPAQUE_DATE_TEST0,
                OPAQUE_DATE_TEST1,
                OPAQUE_DATETIME_TEST0,
                OPAQUE_DATETIME_TEST1,
                OPAQUE_timestamp_TEST0,
                OPAQUE_timestamp_TEST1,
                OPAQUE_decimal_TEST0,
                OPAQUE_decimal_TEST1,
                OPAQUE_decimal_TEST2,
                OPAQUE_STRING_TEST,
                OPAQUE_SMALL_ARR_TEST,
                OPAQUE_SMALL_OBJ_TEST
        );
    }
    
    private List<String> initDelete() {
        return Lists.newArrayList(
                "DELETE FROM drc1.json WHERE id = 1;",
                "DELETE FROM drc1.json WHERE id = 2;",
                "DELETE FROM drc1.json WHERE id = 3;",
                "DELETE FROM drc1.json WHERE id = 4;",
                "DELETE FROM drc1.json WHERE id = 5;",
                "DELETE FROM drc1.json WHERE id = 6;",
                "DELETE FROM drc1.json WHERE id = 7;",
                "DELETE FROM drc1.json WHERE id = 8;",
                "DELETE FROM drc1.json WHERE id = 9;",
                "DELETE FROM drc1.json WHERE id = 10;",
                "DELETE FROM drc1.json WHERE id = 11;",
                "DELETE FROM drc1.json WHERE id = 12;",
                "DELETE FROM drc1.json WHERE id = 13;",
                "DELETE FROM drc1.json WHERE id = 14;",
                "DELETE FROM drc1.json WHERE id = 15;",
                "DELETE FROM drc1.json WHERE id = 16;",
                "DELETE FROM drc1.json WHERE id = 17;",
                "DELETE FROM drc1.json WHERE id = 18;"
        );
    }


}
