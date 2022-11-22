package com.ctrip.framework.drc.core.driver.binlog.manager.task;

import com.ctrip.framework.drc.core.driver.binlog.constant.QueryType;
import com.ctrip.framework.drc.core.server.utils.ThreadUtils;
import org.assertj.core.util.Lists;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.ExecutorService;

import static org.junit.Assert.*;

/**
 * @Author limingdong
 * @create 2022/11/22
 */
public class CommentQueryTaskTest extends AbstractSchemaTest<String> {

    private static final String DB = "db_name";

    private static final String TABLE = "table_name";

    private static final String GTID = "b207f82e-2a7b-11ec-b128-1c34da51a830:25326877444";

    private static final String DDL = "create table `db_name`.`table_name` (" +
            "  `id` int(11) NOT NULL AUTO_INCREMENT," +
            "  PRIMARY KEY (`id`)" +
            ") ENGINE=InnoDB AUTO_INCREMENT=8";

    private ExecutorService executorService = ThreadUtils.newSingleThreadExecutor("ut");


    @Test
    public void testCommentQueryTaskCall() throws Exception {
        new DatabaseCreateTask(Lists.newArrayList(DB), inMemoryEndpoint, inMemoryDataSource).call();
        new SchemeApplyTask(inMemoryEndpoint, inMemoryDataSource,
                DB, TABLE, DDL, GTID, QueryType.CREATE, executorService, null).call();

        String gtid = abstractSchemaTask.call();
        Assert.assertEquals(GTID, gtid);
    }

    @Override
    protected AbstractSchemaTask<String> getAbstractSchemaTask() {
        return new CommentQueryTask(DB, TABLE, inMemoryEndpoint, inMemoryDataSource);
    }
}