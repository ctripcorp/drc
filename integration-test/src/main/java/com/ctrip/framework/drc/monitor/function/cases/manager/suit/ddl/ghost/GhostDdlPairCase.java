package com.ctrip.framework.drc.monitor.function.cases.manager.suit.ddl.ghost;

import com.ctrip.framework.drc.core.monitor.cases.PairCase;
import com.ctrip.framework.drc.monitor.function.cases.manager.suit.ddl.AbstractDdlPairCase;
import com.ctrip.framework.drc.monitor.function.operator.ReadWriteSqlOperator;
import com.ctrip.framework.drc.monitor.utils.enums.DmlTypeEnum;
import org.apache.tomcat.jdbc.pool.PoolProperties;
import org.springframework.util.ClassUtils;

/**
 * @Author limingdong
 * @create 2020/3/19
 */
public class GhostDdlPairCase extends AbstractDdlPairCase implements PairCase<ReadWriteSqlOperator, ReadWriteSqlOperator> {

    private static final String QUERY = "select * from `ghost1_unitest`.`t1` order by `id` limit 100;";

    private static final String LOCAL_DDL = "ddl/ghost/mac/gh-ost";

//    private static final String DELETE = "delete from `ghost1_unitest`.`t1`";

    public static final String DATABASE = "ghost1_unitest";

    public static final String TABLE = "t1";

    private static final String FIX_INSERT = "insert into `ghost1_unitest`.`t1` (`name`, `datachange_lasttime`) values ('value2333', '2010-10-16 15:15:15.666661');";

    protected static final String FIX_UPDATE = "update `ghost1_unitest`.`t1` set `datachange_lasttime` = now() where name = 'value2333';";

    protected static final String FIX_DELETE = "delete from `ghost1_unitest`.`t1` where name = 'value2333';";

    public static final String GHOST_EXECUTOR = "%s&&-user=%s&&-password=%s&&--host=%s&&-port=%s&&--database=%s&&--table=%s&&--debug&&--alter=%s&&--concurrent-rowcount&&--allow-on-master&&--initially-drop-ghost-table&&--initially-drop-old-table&&--execute";

    public static final String GHOST_CHANGE_EXECUTOR = "%s&&-user=%s&&-password=%s&&--host=%s&&-port=%s&&--database=%s&&--table=%s&&--debug&&--alter=%s&&--concurrent-rowcount&&--approve-renamed-columns&&--where-reserve=\"create_time>='2017-01-01'\"&&--allow-on-master&&--initially-drop-ghost-table&&--initially-drop-old-table&&--execute";

    public static final String GHOST_CHANGE_EXECUTOR_MAC = "%s&&-user=%s&&-password=%s&&--host=%s&&-port=%s&&--database=%s&&--table=%s&&--debug&&--alter=%s&&--concurrent-rowcount&&--approve-renamed-columns&&--allow-on-master&&--initially-drop-ghost-table&&--initially-drop-old-table&&--execute";

    public GhostDdlPairCase() {
        initResource();
    }

    @Override
    public void test(ReadWriteSqlOperator src, ReadWriteSqlOperator dst) {
        if (!goOn) {
            logger.info("[goOn] is false and return");
        }
        logger.info(">>>>>>>>>>>> START test [{}] >>>>>>>>>>>>", getClass().getSimpleName());
        boolean insertResult = true;
        boolean updateResult = true;
        boolean deleteResult = true;

        GhostExecutor.deleteSocket();

        for (int i = 0; i < ddls.size(); ++i) {
            currentSql = ddls.get(i);
            if(i >= 0 && i < 2) {
                doWrite(src);
                doWrite(dst);
            } else if (currentSql.toLowerCase().startsWith(INSERT)) {
                insertResult = insertResult && doDdlTest(src, dst, DmlTypeEnum.INSERT);
            } else if(currentSql.toLowerCase().startsWith(UPDATE)) {
                updateResult = updateResult && doDdlTest(src, dst, DmlTypeEnum.UPDATE);
            } else if(currentSql.toLowerCase().startsWith(DELETE)) {
                deleteResult = deleteResult && doDdlTest(src, dst, DmlTypeEnum.DELETE);
            } else { //ghost ddl
                exeGhost(src);
                exeGhost(dst);
            }
        }

        logger.info(">>>>>>>>>>>> END test [{}] >>>>>>>>>>>>\n", getClass().getSimpleName());
    }

    protected void exeGhost(ReadWriteSqlOperator src) {
//        deleteSocket();
        PoolProperties properties = src.getPoolProperties();
        String url = properties.getUrl();
        int first = url.indexOf("//");
        url = url.substring(first + 2);
        int last = url.indexOf("?");
        url = url.substring(0, last);
        String[] ipAndPort = url.split(":");
        String user = properties.getUsername();
        String password = properties.getPassword();


        String path = detect();

        String command;
        if (currentSql.startsWith("change")) {
            String osName = System.getProperty("os.name");
            if (osName.equals("Mac OS X")) {
                command = String.format(GHOST_CHANGE_EXECUTOR_MAC, path, user, password, ipAndPort[0], ipAndPort[1], DATABASE, TABLE, currentSql);
            } else {
                command = String.format(GHOST_CHANGE_EXECUTOR, path, user, password, ipAndPort[0], ipAndPort[1], DATABASE, TABLE, currentSql);
            }
        } else {
            command = String.format(GHOST_EXECUTOR, path, user, password, ipAndPort[0], ipAndPort[1], DATABASE, TABLE, currentSql);
        }
        try {
            logger.info("[Execute] command {} begin", command);
            GhostExecutor.execute(command);
        } catch (Exception e) {
            logger.error("[Execute] {} error", command, e);
        }
    }

    @Override
    protected String getSqlFile() {
        return "ddl/ghost/ghost-ddl.sql";
    }

    @Override
    protected String getQuerySql() {
        return QUERY;
    }

    @Override
    protected String getFixInsert() {
        return FIX_INSERT;
    }

    @Override
    protected String getFixUpdate() {
        return FIX_UPDATE;
    }

    @Override
    protected String getFixDelete() {
        return FIX_DELETE;
    }

    public static String detect() {
        String osName = System.getProperty("os.name");
        if (osName.equals("Mac OS X")) {
            String path = ClassUtils.getDefaultClassLoader().getResource(LOCAL_DDL).getPath();
            return path;
        }
        return "/usr/local/gh-ost";
    }
}
