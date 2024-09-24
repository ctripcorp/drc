package com.ctrip.framework.drc.monitor;

import com.ctrip.framework.drc.core.driver.command.netty.endpoint.DefaultEndPoint;
import com.ctrip.framework.drc.core.monitor.reporter.DefaultEventMonitorHolder;
import com.ctrip.framework.drc.core.monitor.reporter.DefaultTransactionMonitorHolder;
import com.ctrip.framework.drc.core.server.config.SystemConfig;
import com.ctrip.framework.drc.core.server.utils.ThreadUtils;
import com.ctrip.framework.drc.monitor.config.ConfigService;
import com.ctrip.framework.drc.monitor.function.cases.manager.*;
import com.ctrip.framework.drc.monitor.function.cases.manager.suit.*;
import com.ctrip.framework.drc.monitor.function.cases.manager.suit.ddl.CompositeKeysDdlPairCase;
import com.ctrip.framework.drc.monitor.function.cases.manager.suit.ddl.GenericDdlPairCase;
import com.ctrip.framework.drc.monitor.function.cases.manager.suit.ddl.GenericSingleSideDdlPairCase;
import com.ctrip.framework.drc.monitor.function.cases.manager.suit.ddl.ghost.GhostDdlPairCase;
import com.ctrip.framework.drc.monitor.function.cases.manager.suit.ddl.ghost.GhostSingleSideDdlPairCase;
import com.ctrip.framework.drc.monitor.function.cases.truncate.DefaultTableTruncate;
import com.ctrip.framework.drc.monitor.function.cases.truncate.TableTruncate;
import com.ctrip.framework.drc.monitor.function.operator.DefaultSqlOperator;
import com.ctrip.framework.drc.monitor.function.operator.ReadWriteSqlOperator;
import com.ctrip.framework.drc.monitor.function.task.TableCompareTask;
import com.ctrip.framework.drc.monitor.performance.ConflictTestCase;
import com.ctrip.framework.drc.monitor.performance.DdlUpdateCase;
import com.ctrip.framework.drc.monitor.performance.QPSTestPairCase;
import com.ctrip.framework.drc.monitor.performance.ResultCompareCase;
import com.ctrip.framework.drc.monitor.utils.enums.TestTypeEnum;
import com.ctrip.xpipe.api.lifecycle.Destroyable;
import com.ctrip.xpipe.api.monitor.Task;
import com.ctrip.xpipe.lifecycle.AbstractLifecycle;
import org.apache.tomcat.jdbc.pool.PoolProperties;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static com.ctrip.framework.drc.core.server.config.SystemConfig.JDBC_URL_FORMAT;
import static com.ctrip.framework.drc.monitor.performance.ResultCompareCase.TRUNCATE_TABLE;

/**
 * Created by mingdongli
 * 2019/10/9 下午5:35.
 */
public class DrcMonitorModule extends AbstractLifecycle implements Destroyable {

    public static final String TRUNCATE_TABLE_BENCHMARK2 = "truncate table bbzbbzdrcbenchmarktmpdb.benchmark2;";

    private String srcIp;

    private String dstIp;

    private int srcPort;

    private int dstPort;

    private String user;

    private String password;

    protected ReadWriteSqlOperator sourceSqlOperator;

    protected ReadWriteSqlOperator reverseSourceSqlOperator;

    private PoolProperties sourcePoolProperties;

    private PoolProperties destinationPoolProperties;

    private String envType = ConfigService.getInstance().getDrcEnvType();

    private PairCaseManager pairCaseManager = new DefaultPairCaseManager();
    private PairCaseManager scannerPairCaseManager = new DefaultPairCaseManager();

    private PairCaseManager benchmarkCaseManager = new BenchmarkPairCaseManager();

    private PairCaseManager ddlPairCaseManager = new DdlPairCaseManager();

    protected PairCaseManager unilateralPairCaseManager = new UnilateralPairCaseManager();

    private ScheduledExecutorService scheduledExecutorService = ThreadUtils.newSingleThreadScheduledExecutor("default-scheduledExecutorService");

    private ScheduledExecutorService benchmarkScheduledExecutorService = ThreadUtils.newSingleThreadScheduledExecutor("benchmark-scheduledExecutorService");

    protected ScheduledExecutorService dalScheduledExecutorService = ThreadUtils.newSingleThreadScheduledExecutor("dal-scheduledExecutorService");

    private ScheduledExecutorService unilateralScheduledExecutorService = ThreadUtils.newSingleThreadScheduledExecutor("unilateral-scheduledExecutorService");
    private ScheduledExecutorService compareTableScheduledExecutorService = ThreadUtils.newSingleThreadScheduledExecutor("table-compare-scheduledExecutorService");
    private ScheduledExecutorService oneCompareScheduledExecutorService = ThreadUtils.newSingleThreadScheduledExecutor("multi-write-one-compare-scheduledExecutorService");

    private boolean writeOneTransaction = true;   // build replication automatically

    public DrcMonitorModule(int srcPort, int dstPort) {
        this(srcPort, dstPort, SystemConfig.MYSQL_PASSWORD);
    }

    public DrcMonitorModule(int srcPort, int dstPort, String password) {
        this.srcPort = srcPort;
        this.dstPort = dstPort;
        this.password = password;
    }

    public DrcMonitorModule(String srcIp, int srcPort, String dstIp, int dstPort, String user, String password) {
        this.srcIp = srcIp;
        this.dstIp = dstIp;
        this.srcPort = srcPort;
        this.dstPort = dstPort;
        this.user = user;
        this.password = password;
    }

    @Override
    protected void doInitialize() throws Exception {
        if (null == user) {
            user = SystemConfig.MYSQL_USER_NAME;
        }
        if (null == srcIp) {
            srcIp = SystemConfig.LOCAL_SERVER_ADDRESS;
        }
        if (null == dstIp) {
            dstIp = SystemConfig.LOCAL_SERVER_ADDRESS;
        }

        sourcePoolProperties = new PoolProperties();
        String srcUrl = String.format(JDBC_URL_FORMAT, srcIp, srcPort);
        sourcePoolProperties.setUrl(srcUrl);
        initPoolProperties(sourcePoolProperties);

        destinationPoolProperties = new PoolProperties();
        String dstUrl = String.format(JDBC_URL_FORMAT, dstIp, dstPort);
        destinationPoolProperties.setUrl(dstUrl);
        initPoolProperties(destinationPoolProperties);

        sourceSqlOperator = new DefaultSqlOperator(new DefaultEndPoint(srcIp, srcPort, user, password), sourcePoolProperties);
        sourceSqlOperator.initialize();

        reverseSourceSqlOperator = new DefaultSqlOperator(new DefaultEndPoint(dstIp, dstPort, user, password), destinationPoolProperties);
        reverseSourceSqlOperator.initialize();

        addPairCase();
    }

    private void initPoolProperties(PoolProperties properties) {
        properties.setDriverClassName("com.mysql.jdbc.Driver");
        if (user != null) {
            properties.setUsername(user);
        } else {
            properties.setUsername(SystemConfig.MYSQL_USER_NAME);
        }

        properties.setPassword(password);
        properties.setInitialSize(2);
        properties.setMaxActive(100);
        properties.setMaxIdle(10);
        properties.setMinIdle(1);
        properties.setRemoveAbandoned(true);
        properties.setRemoveAbandonedTimeout(60);
        properties.setValidationQuery("SELECT 1");
        properties.setValidationQueryTimeout(1);
        properties.setTestOnBorrow(true);
        properties.setValidationInterval(30000);
        // 建立连接超时时间10s，等待请求返回超时时间2s,docker启动需要时间
        properties.setConnectionProperties("connectTimeout=1000;socketTimeout=20000");
    }

    @Override
    protected void doStart() throws Exception {
        sourceSqlOperator.start();

        reverseSourceSqlOperator.start();

        scheduledExecutorService.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                try {
                    //dml
                    final boolean finalAutoWrite = ConfigService.getInstance().getAutoWriteSwitch();
                    if (finalAutoWrite) {
                        logger.info(">>>>>>>>>>>> start test dml >>>>>>>>>>>>");
                        probe();
                        logger.info(">>>>>>>>>>>> end test dml >>>>>>>>>>>>");
                    }

                    //ddl
                    final boolean finalAutoDdl = ConfigService.getInstance().getAutoDdlSwitch();
                    if (finalAutoDdl) {
                        logger.info(">>>>>>>>>>>> start test ddl >>>>>>>>>>>>");
                        ddl();
                        logger.info(">>>>>>>>>>>> end test ddl >>>>>>>>>>>>");
                    }
                } catch (Throwable e) {
                    DefaultEventMonitorHolder.getInstance().logError(e);
                }
            }
        }, 10, 120, TimeUnit.SECONDS);

        benchmarkScheduledExecutorService.scheduleWithFixedDelay(new Runnable() {
            private long round = 201;

            @Override
            public void run() {
                try {
                    DefaultTransactionMonitorHolder.getInstance().logTransaction("Monitor", "Benchmark", new Task() {
                        @Override
                        public void go() throws Exception {
                            final boolean finalAutoBenchmark = ConfigService.getInstance().getAutoBenchmarkSwitch();
                            if (finalAutoBenchmark) {
                                benchmark(round);
                            }
                        }
                    });
                } catch (Throwable t) {
                    logger.error("benchmark error", t);
                }
                round++;
            }
        }, 10, 100, TimeUnit.MILLISECONDS);

        unilateralScheduledExecutorService.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                try {
                    DefaultTransactionMonitorHolder.getInstance().logTransaction("Monitor", "Uni", new Task() {
                        @Override
                        public void go() throws Exception {
                            if (ConfigService.getInstance().getAutoScannerSwitch()) {
                                uni();
                            }
                        }
                    });
                } catch (Throwable t) {
                    logger.error("uni error", t);
                }
            }
        }, 10, 1, TimeUnit.SECONDS);


        TableCompareTask tableCompareTask = new TableCompareTask();
        compareTableScheduledExecutorService.scheduleWithFixedDelay(() -> {
            if (ConfigService.getInstance().getAutoTableCompareSwitch()) {
                tableCompareTask.compare();
            }
        }, 10, 100, TimeUnit.SECONDS);


        oneCompareScheduledExecutorService.scheduleWithFixedDelay(() -> {
            if (ConfigService.getInstance().getAutoScannerSwitch()) {
                scannerTest();
            }
        }, 10, 10, TimeUnit.SECONDS);


        if (writeOneTransaction) {
            new MultiTableWriteInTransactionPairCase().test(sourceSqlOperator, reverseSourceSqlOperator); //write one transaction
        }

    }

    @Override
    protected void doStop() throws Exception {
        scheduledExecutorService.shutdownNow();
        sourceSqlOperator.stop();
    }

    protected void addPairCase() {
        pairCaseManager.addPairCase(new TransactionTableConflictPairCase());
        pairCaseManager.addPairCase(new MultiTableWriteInTransactionPairCase());
        pairCaseManager.addPairCase(new PromiscuousWritePairCase());
        pairCaseManager.addPairCase(new MultiTypeNumberPairCase());
        pairCaseManager.addPairCase(new MultiTypeNumberUnsignedPairCase());
        pairCaseManager.addPairCase(new FloatTypePairCase());
        pairCaseManager.addPairCase(new CharsetTypePairCase());
        pairCaseManager.addPairCase(new TimeTypePairCase());
        pairCaseManager.addPairCase(new TypeModifyPairCase());
        pairCaseManager.addPairCase(new RowsFilterCase());
        pairCaseManager.addPairCase(new GrandTransactionPairCase(TestTypeEnum.FUNCTION));
        pairCaseManager.addPairCase(new GrandEventPairCase(TestTypeEnum.FUNCTION));
        pairCaseManager.addPairCase(new CombinedPrimaryKeyPairCase());
        pairCaseManager.addPairCase(new JsonTypePairCase());
        pairCaseManager.addPairCase(new LargeJsonTypePairCase());
        if (ConfigService.getInstance().getBinlogMinimalRowImageSwitch()) {
            pairCaseManager.addPairCase(new BinlogMinimalRowImage());
        }
        if (ConfigService.getInstance().getBinlogNoBlobRowImageSwitch()) {
            pairCaseManager.addPairCase(new BinlogNoBlobRowImage());
        }

        //benchmark
        if (ConfigService.getInstance().getDrcMonitorQpsSwitch()) {
            benchmarkCaseManager.addPairCase(new QPSTestPairCase());
        }
        if (ConfigService.getInstance().getGrandTransactionSwitch()) {
            benchmarkCaseManager.addPairCase(new GrandTransactionPairCase());
        }
        if (ConfigService.getInstance().getGrandEventSwitch()) {
            benchmarkCaseManager.addPairCase(new GrandEventPairCase());
        }
        if (ConfigService.getInstance().getResultCompareSwitch()) {
            benchmarkCaseManager.addPairCase(new ResultCompareCase());
        }
        if (ConfigService.getInstance().getDrcDdlQpsSwitch()) {
            benchmarkCaseManager.addPairCase(new DdlUpdateCase());
        }
        if (ConfigService.getInstance().getConflictBenchmarkSwitch()) {
            benchmarkCaseManager.addPairCase(new ConflictTestCase());
        }

        //ddl
        if (ConfigService.getInstance().getGenericDdlSwitch()) {
            ddlPairCaseManager.addPairCase(new GenericDdlPairCase());
        }
        if (ConfigService.getInstance().getGenericSingleSideDdlSwitch()) {
            ddlPairCaseManager.addPairCase(new GenericSingleSideDdlPairCase());
        }
        if (ConfigService.getInstance().getGhostDdlSwitch()) {
            ddlPairCaseManager.addPairCase(new GhostDdlPairCase());
        }
        if (ConfigService.getInstance().getGhostSingleSideDdlSwitch()) {
            ddlPairCaseManager.addPairCase(new GhostSingleSideDdlPairCase());
        }
        if (ConfigService.getInstance().getCompositeKeysDdlSwitch()) {
            ddlPairCaseManager.addPairCase(new CompositeKeysDdlPairCase());
        }

        scannerPairCaseManager.addPairCase(new MultiShardDbWriteOneComparePairCase());
    }

    protected void probe() {
        DataCleanUpPairCase dataCleanUpPairCase = new DataCleanUpPairCase();
        dataCleanUpPairCase.doWrite(sourceSqlOperator, reverseSourceSqlOperator);

        pairCaseManager.test(sourceSqlOperator, reverseSourceSqlOperator);
        pairCaseManager.test(reverseSourceSqlOperator, sourceSqlOperator);
    }

    protected void benchmark(long round) {
        if ("fat".equalsIgnoreCase(envType) && round % 200 == 0) {
            logger.info(">>>>>>>>>>>> START truncate [{}] >>>>>>>>>>>>", "bbzbbzdrcbenchmarktmpdb.benchmark1," + "bbzbbzdrcbenchmarktmpdb.benchmark2");
            TableTruncate tableTruncate = new DefaultTableTruncate();
            tableTruncate.truncateTable(sourceSqlOperator, TRUNCATE_TABLE);
            tableTruncate.truncateTable(reverseSourceSqlOperator, TRUNCATE_TABLE);

            tableTruncate.truncateTable(sourceSqlOperator, TRUNCATE_TABLE_BENCHMARK2);
            tableTruncate.truncateTable(reverseSourceSqlOperator, TRUNCATE_TABLE_BENCHMARK2);

            tableTruncate.truncateTable(sourceSqlOperator, "truncate table bbzbbzdrcbenchmarktmpdb.conflictBenchmark;");
            tableTruncate.truncateTable(reverseSourceSqlOperator, "truncate table bbzbbzdrcbenchmarktmpdb.conflictBenchmark;");

            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
            }
        }
        benchmarkCaseManager.test(sourceSqlOperator, reverseSourceSqlOperator);
    }

    protected void ddl() {
        ddlPairCaseManager.test(sourceSqlOperator, reverseSourceSqlOperator);
    }

    protected void uni() {
        unilateralPairCaseManager.test(sourceSqlOperator, reverseSourceSqlOperator);
    }

    protected void scannerTest() {
        logger.info("scannerTest start");
        scannerPairCaseManager.test(sourceSqlOperator, reverseSourceSqlOperator);
    }

    @Override
    public void destroy() {
        scheduledExecutorService.shutdown();
    }

}
