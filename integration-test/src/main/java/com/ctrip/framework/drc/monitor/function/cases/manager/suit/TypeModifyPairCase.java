package com.ctrip.framework.drc.monitor.function.cases.manager.suit;

import com.ctrip.framework.drc.core.monitor.cases.PairCase;
import com.ctrip.framework.drc.core.monitor.operator.ReadResource;
import com.ctrip.framework.drc.core.monitor.operator.ReadSqlOperator;
import com.ctrip.framework.drc.monitor.function.operator.ReadWriteSqlOperator;
import com.google.common.collect.Lists;

import java.util.List;

/**
 * Created by jixinwang on 2020/11/4
 */
public class TypeModifyPairCase extends AbstractMultiWriteInTransactionPairCase implements PairCase<ReadWriteSqlOperator, ReadSqlOperator<ReadResource>> {

    public static final String TYPE1 = "insert into `drc4`.`table_map` (`size`, `size_big`, `decimal_max`," +
            "`varcharlt256_utf8mb4`, `varchargt256_utf8mb4`, `char240_utf8mb4`," +
            "`varcharlt256_utf8`, `varchargt256_utf8`, `char240_utf8`," +
            "`varcharlt256_cp932`, `varchargt256_cp932`, `char240_cp932`," +
            "`varcharlt256_euckr`, `varchargt256_euckr`, `char240_euckr`," +
            "`varbinarylt256`, `varbinarygt256`, `binary200`, `tinyintunsigned`, `smallintunsigned`, `mediumintunsigned`, `intunsigned`, `bigintunsigned`, `tinyint`, `smallint`, `mediumint`, `int`, `bigint`, `datachange_lasttime`) values" +
            "('small', '1', 11111111112222222222333333333344444.111111111122222222223333333333," +
            "'varcharlt256__utf8mb4_1', 'varchargt256__utf8mb4_1', 'char__utf8mb4_1'," +
            "'varcharlt256__utf8_1', 'varchargt256__utf8_1', 'char__utf8_1'," +
            "'愚かな1', '愚かな1', '愚かな1'," +
            "'присоска1', 'присоска1', 'присоска1'," +
            "'0101', '0101', '0101'," +
            "120, 1200, 12000, 300000, 1100000," +
            "15, 15, 15, 15, 15, '2020-09-07 10:10:10.666661');";

    public static final String TYPE2 = "insert into `drc4`.`table_map` (`size`, `size_big`, `decimal_max`," +
            "`varcharlt256_utf8mb4`, `varchargt256_utf8mb4`, `char240_utf8mb4`," +
            "`varcharlt256_utf8`, `varchargt256_utf8`, `char240_utf8`," +
            "`varcharlt256_cp932`, `varchargt256_cp932`, `char240_cp932`," +
            "`varcharlt256_euckr`, `varchargt256_euckr`, `char240_euckr`," +
            "`varbinarylt256`, `varbinarygt256`, `binary200`, `tinyintunsigned`, `smallintunsigned`, `mediumintunsigned`, `intunsigned`, `bigintunsigned`, `tinyint`, `smallint`, `mediumint`, `int`, `bigint`, `datachange_lasttime`) values" +
            "('large', '2', 2222222222333333333344444.22222222223333333333," +
            "'varcharlt256__utf8mb4_2', 'varchargt256__utf8mb4_2', 'char__utf8mb4_2'," +
            "'varcharlt256__utf8_2', 'varchargt256__utf8_2', 'char__utf8_2'," +
            "'愚かな2', '愚かな2', '愚かな2'," +
            "'присоска2', 'присоска2', 'присоска2'," +
            "'0101', '0101', '0101'," +
            "240, 40000, 11000000, 3000000000, 11000000000000000000," +
            "-15, -15, -15, -15, -15, '2020-09-07 10:10:10.666661');";

    private static final List<String> INSERT_STATEMENTS = Lists.newArrayList(
            TYPE1,
            TYPE2
    );

    private static final List<String> UPDATE_STATEMENTS= Lists.newArrayList(
            "update `drc4`.`table_map` set `varcharlt256_utf8mb4` = '222' where `size` = 'small';"
    );

    private static final List<String> DELETE_STATEMENTS = Lists.newArrayList(
            "delete from `drc4`.`table_map` where `size` = 'large';"
    );

    private static final String SELECT_TYPE = "select * from `drc4`.`table_map`;";

    public TypeModifyPairCase() {
        initResource(INSERT_STATEMENTS, UPDATE_STATEMENTS, DELETE_STATEMENTS, SELECT_TYPE);
    }
}
