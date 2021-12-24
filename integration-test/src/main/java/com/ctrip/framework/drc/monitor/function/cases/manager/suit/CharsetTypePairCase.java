package com.ctrip.framework.drc.monitor.function.cases.manager.suit;

import com.ctrip.framework.drc.core.monitor.operator.ReadResource;
import com.ctrip.framework.drc.core.monitor.operator.ReadSqlOperator;
import com.ctrip.framework.drc.core.monitor.cases.PairCase;
import com.ctrip.framework.drc.monitor.function.operator.ReadWriteSqlOperator;
import com.google.common.collect.Lists;

import java.util.List;

/**
 * Created by mingdongli
 * 2019/10/21 下午11:26.
 */
public class CharsetTypePairCase extends AbstractMultiWriteInTransactionPairCase implements PairCase<ReadWriteSqlOperator, ReadSqlOperator<ReadResource>> {

    public static final String V_B = "insert into `drc4`.`charset_type` values (\n" +
            "'嘿嘿嘿varchar4000', '嘿嘿嘿char1000', 'varbinary1800',\n" +
            "x'8b5fa55d1301000000530000004618000000003e040000000001000464726334000c636861727365745f74797065000c0ffe0ffefcfcfcfcfcfcfcfc10a00fcee80807fec80103020401030204ff0f12f93547',\n" +
            "x'8b5fa55d1301000000530000004618000000003e040000000001000464726334000c636861727365745f74797065000c0ffe0ffefcfcfcfcfcfcfcfc10a00fcee80807fec80103020401030204ff0f12f93547',\n" +
            "x'8b5fa55d1301000000530000004618000000003e040000000001000464726334000c636861727365745f74797065000c0ffe0ffefcfcfcfcfcfcfcfc10a00fcee80807fec80103020401030204ff0f12f93547',\n" +
            "x'8b5fa55d1301000000530000004618000000003e040000000001000464726334000c636861727365745f74797065000c0ffe0ffefcfcfcfcfcfcfcfc10a00fcee80807fec80103020401030204ff0f12f93547',\n" +
            "x'8b5fa55d1301000000530000004618000000003e040000000001000464726334000c636861727365745f74797065000c0ffe0ffefcfcfcfcfcfcfcfc10a00fcee80807fec80103020401030204ff0f12f93547',\n" +
            "'嘿嘿嘿tinytext', '嘿嘿嘿mediumtext', '嘿嘿嘿text', '嘿嘿嘿longtext',\n" +
            "x'8b5fa55d1301000000530000004618000000003e040000000001000464726334000c636861727365745f74797065000c0ffe0ffefcfcfcfcfcfcfcfc10a00fcee80807fec80103020401030204ff0f12f93547', 1, '2019-10-16 15:15:15.666616'\n" +
            ");";

    public static final String B_V = "insert into `drc4`.`charset_type` values (\n" +
            "'嘿嘿嘿varchar4000', '嘿嘿嘿char1000',\n" +
            "x'8b5fa55d1301000000530000004618000000003e040000000001000464726334000c636861727365745f74797065000c0ffe0ffefcfcfcfcfcfcfcfc10a00fcee80807fec80103020401030204ff0f12f93547',\n" +
            "'binary200',\n" +
            "x'8b5fa55d1301000000530000004618000000003e040000000001000464726334000c636861727365745f74797065000c0ffe0ffefcfcfcfcfcfcfcfc10a00fcee80807fec80103020401030204ff0f12f93547',\n" +
            "x'8b5fa55d1301000000530000004618000000003e040000000001000464726334000c636861727365745f74797065000c0ffe0ffefcfcfcfcfcfcfcfc10a00fcee80807fec80103020401030204ff0f12f93547',\n" +
            "x'8b5fa55d1301000000530000004618000000003e040000000001000464726334000c636861727365745f74797065000c0ffe0ffefcfcfcfcfcfcfcfc10a00fcee80807fec80103020401030204ff0f12f93547',\n" +
            "x'8b5fa55d1301000000530000004618000000003e040000000001000464726334000c636861727365745f74797065000c0ffe0ffefcfcfcfcfcfcfcfc10a00fcee80807fec80103020401030204ff0f12f93547',\n" +
            "'嘿嘿嘿tinytext', '嘿嘿嘿mediumtext', '嘿嘿嘿text', '嘿嘿嘿longtext',\n" +
            "x'8b5fa55d1301000000530000004618000000003e040000000001000464726334000c636861727365745f74797065000c0ffe0ffefcfcfcfcfcfcfcfc10a00fcee80807fec80103020401030204ff0f12f93547', 2, '2019-10-16 15:15:15.666617'\n" +
            ");";

    private static final List<String> INSERT_STATEMENTS = Lists.newArrayList(
            V_B,
            B_V
    );

    private static final List<String> UPDATE_STATEMENTS= Lists.newArrayList(
            "update `drc4`.`charset_type` set `char1000` = '222' where `id` = 1;",
            "update `drc4`.`charset_type` set `char1000` = '222' where `id` = 2;"
    );

    private static final List<String> DELETE_STATEMENTS = Lists.newArrayList(
            "delete from `drc4`.`charset_type` where `id` = 1;",
            "delete from `drc4`.`charset_type` where `id` = 2;"
    );

    private static final String SELECT_V_B = "select * from `drc4`.`charset_type` where `varchar4000` = '嘿嘿嘿varchar4000';";

    public CharsetTypePairCase() {
        initResource(INSERT_STATEMENTS, UPDATE_STATEMENTS, DELETE_STATEMENTS, SELECT_V_B);
    }
}
