package com.ctrip.framework.drc.performance.utils;

import com.ctrip.xpipe.config.AbstractConfigBean;

/**
 * Created by jixinwang on 2021/8/17
 */
public class ConfigService extends AbstractConfigBean {

    private static final ConfigService INSTANCE = new ConfigService();

    public static ConfigService getInstance() {
        return INSTANCE;
    }

    // qConfig key
    private static final String KEY_BINLOG_REPLAY_QPS = "binlog.replay.qps";
    private static final String KEY_JDBC_THREAD_NUMBER = "jdbc.thread.number";
    private static final String KEY_JDBC_EXECUTE_COUNT = "jdbc.execute.count";

    // qConfig default value
    private static final int DEFAULT_BINLOG_REPLAY_QPS = 10;
    private static final int DEFAULT_JDBC_THREAD_NUMBER = 100;
    private static final int DEFAULT_JDBC_EXECUTE_COUNT = 200;


    public int getBinlogReplayRound() {
        return getIntProperty(KEY_BINLOG_REPLAY_QPS, DEFAULT_BINLOG_REPLAY_QPS);
    }

    public int getJdbcThreadNumber() {
        return getIntProperty(KEY_JDBC_THREAD_NUMBER, DEFAULT_JDBC_THREAD_NUMBER);
    }

    public int getJdbcExecuteCount() {
        return getIntProperty(KEY_JDBC_EXECUTE_COUNT, DEFAULT_JDBC_EXECUTE_COUNT);
    }
}
