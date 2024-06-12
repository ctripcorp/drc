package com.ctrip.framework.drc.monitor.function.cases.manager.suit;

import com.ctrip.framework.drc.core.http.ApiResult;
import com.ctrip.framework.drc.core.meta.DBInfo;
import com.ctrip.framework.drc.core.meta.InstanceInfo;
import com.ctrip.framework.drc.core.monitor.cases.PairCase;
import com.ctrip.framework.drc.core.monitor.execution.Execution;
import com.ctrip.framework.drc.core.monitor.operator.ReadResource;
import com.ctrip.framework.drc.core.monitor.operator.ReadSqlOperator;
import com.ctrip.framework.drc.core.monitor.reporter.DefaultEventMonitorHolder;
import com.ctrip.framework.drc.core.monitor.reporter.DefaultTransactionMonitorHolder;
import com.ctrip.framework.drc.core.server.config.RegistryKey;
import com.ctrip.framework.drc.core.server.config.applier.dto.ApplierConfigDto;
import com.ctrip.framework.drc.core.server.utils.ThreadUtils;
import com.ctrip.framework.drc.monitor.config.ConfigService;
import com.ctrip.framework.drc.monitor.function.cases.InsertCase;
import com.ctrip.framework.drc.monitor.function.cases.insert.MultiStatementWriteCase;
import com.ctrip.framework.drc.monitor.function.cases.truncate.DefaultTableTruncate;
import com.ctrip.framework.drc.monitor.function.cases.truncate.TableTruncate;
import com.ctrip.framework.drc.monitor.function.execution.WriteExecution;
import com.ctrip.framework.drc.monitor.function.execution.insert.MultiStatementWriteExecution;
import com.ctrip.framework.drc.monitor.function.execution.select.SingleSelectExecution;
import com.ctrip.framework.drc.monitor.function.operator.ReadWriteSqlOperator;
import com.ctrip.framework.drc.monitor.utils.enums.StatusEnum;
import com.ctrip.xpipe.retry.RestOperationsRetryPolicyFactory;
import com.ctrip.xpipe.spring.RestTemplateFactory;
import com.dianping.cat.Cat;
import com.dianping.cat.message.Transaction;
import com.google.common.collect.Lists;
import org.springframework.http.*;
import org.springframework.web.client.RestOperations;
import org.unidal.tuple.Pair;

import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static com.ctrip.framework.drc.core.server.config.SystemConfig.CONNECTION_TIMEOUT;


public class MultiShardDbWriteOneComparePairCase extends AbstractMultiWriteInTransactionPairCase implements PairCase<ReadWriteSqlOperator, ReadSqlOperator<ReadResource>> {

    /**
     * CREATE TABLE drc_shard_1.table1
     * (
     * `id`                  bigint(20)   NOT NULL AUTO_INCREMENT,
     * `col_str`             varchar(200) NOT NULL,
     * `datachange_lasttime` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
     * PRIMARY KEY (`id`)
     * ) ENGINE = InnoDB
     * default character set = "utf8mb3";
     */
    public static final String TEST_SHARD_DB_PREFIX = "drc_shard_";
    public static final String TABLE_NAME = "table1";
    public int dbShardNum = 50;
    public static final String APPLIER_REGISTRY_KEY_PRE = "zyn_test_2_dalcluster.zyn_test_2.zyn_test_1";
    private final TableTruncate tableTruncate = new DefaultTableTruncate();
    protected ScheduledExecutorService scheduledExecutorService = ThreadUtils.newSingleThreadScheduledExecutor(getClass().getSimpleName());
    protected ExecutorService writeDbService = ThreadUtils.newFixedThreadPool(10, "WriteDb " + getClass().getSimpleName());
    protected RestOperations restTemplate = RestTemplateFactory.createCommonsHttpRestTemplate(4, 40, CONNECTION_TIMEOUT, 5000, 0, new RestOperationsRetryPolicyFactory(2)); //retry by Throwable
    private final Map<String, String> registryKeyMap = new HashMap<>();
    private final Map<String, String> ipMap = new HashMap<>();


    @Override
    public void test(ReadWriteSqlOperator src, ReadSqlOperator<ReadResource> dst) {
        try {
            // clear src/dst
            init(src, (ReadWriteSqlOperator) dst);

            int writeSeconds = 60;
            int parkSeconds = 5;
            logger.info(">>>>>>>>>>>> START test [{}] >>>>>>>>>>>>", getClass().getSimpleName());

            // write xx seconds
            if (dst == null) {
                logger.error("[{}] not ReadWriteSqlOperator, fail", getClass().getSimpleName());
                return;
            }
            logger.info("[{}] start write for {} seconds", getClass().getSimpleName(), writeSeconds);
            long startTime = System.currentTimeMillis();
            long writeMillis = TimeUnit.SECONDS.toMillis(writeSeconds);
            this.scheduleCloseTask((int) writeMillis);
            do {
                doWriteMultiDb(src);
            } while (System.currentTimeMillis() - startTime <= writeMillis);

            // write ends, park
            logger.info("[{}] start wait for {} seconds", getClass().getSimpleName(), parkSeconds);
            park(parkSeconds);

            // compare all src/dst
            logger.info("[{}] start check result", getClass().getSimpleName());
            checkResult(src, dst);
        } catch (Exception e) {
            logger.error("case exception", e);
        } finally {
            // clear src/dst
            logger.info(">>>>>>>>>>>> END test [{}] >>>>>>>>>>>>\n", getClass().getSimpleName());
        }
    }

    private final String DEFAULT_IP = "10.118.1.127";

    private void init(ReadWriteSqlOperator src, ReadWriteSqlOperator dst) throws Exception {
        id.set(1);
        dbShardNum = ConfigService.getInstance().getScannerTestShardNum();
        selectList.clear();
        this.clearData(src, dst);
        for (int i = 1; i <= getDbShardNum(); i++) {
            String dbName = getDbName(i);
            ipMap.put(dbName, DEFAULT_IP);
            registryKeyMap.put(dbName, String.format(APPLIER_REGISTRY_KEY_PRE + ".%s", dbName));
        }
    }

    private int getDbShardNum() {
        return dbShardNum;
    }


    private void scheduleCloseTask(int writeMillis) {
        Random random = new Random();
        for (int i = 1; i <= getDbShardNum(); i++) {
            String dbName = getDbName(i);
            String applierRegistryKey = registryKeyMap.get(dbName);
            String ip = ipMap.get(dbName);

            long randomTime = 1000L + random.nextInt(writeMillis);
            scheduledExecutorService.schedule(() -> restartApplier(ip, applierRegistryKey), randomTime, TimeUnit.MILLISECONDS);
        }
    }


    private ApiResult restartApplier(String ip, String applierRegistryKey) throws Exception {
        DefaultTransactionMonitorHolder.getInstance().logTransaction("intergration-test.applier.restart", applierRegistryKey, () -> {
            ResponseEntity<ApiResult> response = null;
            try {
                HttpHeaders headers = new HttpHeaders();
                headers.setContentType(MediaType.APPLICATION_JSON);
                headers.setAccept(Lists.newArrayList(MediaType.APPLICATION_JSON));
                HttpEntity<Object> entity = new HttpEntity<>(getBody(applierRegistryKey), headers);
                response = restTemplate.exchange(getUrl(ip), HttpMethod.POST, entity, ApiResult.class);
                Boolean success = (Boolean) response.getBody().getData();
                if (!success) {
                    throw new Exception("applier restart fail: " + applierRegistryKey);
                }
            } catch (Throwable e) {
                logger.error("applier restart fail, resp: {}", response, e);
                throw e;

            }
        });
        return ApiResult.getSuccessInstance(null);

    }

    protected ApplierConfigDto getBody(String applierRegistryKey) {
        ApplierConfigDto applierConfigDto = new ApplierConfigDto();
        applierConfigDto.setApplyMode(3);
        applierConfigDto.setIncludedDbs(RegistryKey.getTargetDB(applierRegistryKey));
        RegistryKey key = RegistryKey.from(applierRegistryKey);
        applierConfigDto.setCluster(key.getClusterName());
        DBInfo target = new DBInfo();
        target.setMhaName(key.getMhaName());
        applierConfigDto.setTarget(target);
        InstanceInfo replicator = new InstanceInfo();
        replicator.setMhaName(RegistryKey.getTargetMha(applierRegistryKey));
        applierConfigDto.setReplicator(replicator);
        return applierConfigDto;
    }

    private String getUrl(String ipAndPort) {
        return String.format("http://%s/%s", ipAndPort + ":" + 8080, "appliers/restart");
    }

    private void clearData(ReadWriteSqlOperator src, ReadWriteSqlOperator dst) throws Exception {
        List<Future<Boolean>> futures = Lists.newArrayList();
        for (int i = 1; i <= getDbShardNum(); i++) {
            String s = String.format("truncate table %s.%s", getDbName(i), TABLE_NAME);

            futures.add(writeDbService.submit(() -> tableTruncate.truncateTable(src, s)));
            futures.add(writeDbService.submit(() -> tableTruncate.truncateTable(dst, s)));
        }
        for (Future<Boolean> future : futures) {
            Boolean done = future.get(30, TimeUnit.SECONDS);
            if (!done) {
                throw new Exception("truncate table fail");
            }
        }
    }

    protected void doWriteMultiDb(ReadWriteSqlOperator src) throws Exception {
        List<Future<Boolean>> futures = Lists.newArrayList();
        for (int i = 1; i <= getDbShardNum(); i++) {
            int finalI = i;
            Future<Boolean> submit = writeDbService.submit(() -> writeDb(src, finalI));
            futures.add(submit);
        }
        for (Future<Boolean> future : futures) {
            future.get(30, TimeUnit.SECONDS);
        }
        parkMills(500);
    }

    private boolean writeDb(ReadWriteSqlOperator src, int i) {
        List<String> statements = getStatements(i);
        WriteExecution writeExecution = new MultiStatementWriteExecution(statements);
        InsertCase insertCase = new MultiStatementWriteCase(writeExecution);
        boolean affected = insertCase.executeInsert(src);
        if (!affected) {
            alarm.alarm(String.format("%s execute write error %s", getClass().getName(), writeExecution.getStatements()));
        }
        return affected;
    }

    private ExecutorService taskExecutorService = ThreadUtils.newCachedThreadPool("sql");


    private static ThreadLocal<SimpleDateFormat> dateFormatThreadLocal = ThreadLocal.withInitial(() -> new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS"));

    private void checkResult(ReadWriteSqlOperator src, ReadSqlOperator<ReadResource> dst) {
        List<Pair<Date, List<String>>> allList = getSelectList();
        for (Pair<Date, List<String>> pair : allList) {
            Date writeTime = pair.getKey();
            String writeTimeTag = dateFormatThreadLocal.get().format(writeTime);
            List<String> selectList = pair.getValue();
            Transaction t = Cat.newTransaction(integrationTestInstanceName + ".test.case", getClass().getSimpleName());
            try {
                for (int i = 0; i < selectList.size(); i++) {
                    Execution selectExecution = new SingleSelectExecution(selectList.get(i));
                    boolean result = diff(selectExecution, src, dst);
                    if (!result) {
                        t.addData(writeTimeTag, StatusEnum.FAIL.getDescription());
                        t.setStatus(new Exception("Not all cases pass"));
                        break;
                    }
                }
                t.addData(writeTimeTag, StatusEnum.SUCCESS.getDescription());
            } catch (Exception e) {
                DefaultEventMonitorHolder.getInstance().logError(e);
                t.setStatus(e);
            } finally {
                t.complete();
            }
        }
    }

    private final List<Pair<Date, List<String>>> selectList = Lists.newArrayList();

    private List<Pair<Date, List<String>>> getSelectList() {
        return selectList;
    }

    @Override
    protected void parkOneSecond() {
        park(1);
    }

    protected void park(int seconds) {
        try {
            Thread.sleep(TimeUnit.SECONDS.toMillis(seconds));
        } catch (InterruptedException e) {
            logger.error("sleep error", e);
        }
    }

    protected void parkMills(int mills) {
        try {
            Thread.sleep(mills);
        } catch (InterruptedException e) {
            logger.error("sleep error", e);
        }
    }

    public MultiShardDbWriteOneComparePairCase() {
        initResource(initInsert(), initUpdate(), initDelete(), "select * from `drc1`.`json` where id <= 18;");
    }

    private AtomicInteger id = new AtomicInteger(1);

    protected List<String> getStatements(int shardNum) {
        // each period: 3 insert, 1 update, 1 delete
        int id1 = id.getAndIncrement();
        int id2 = id.getAndIncrement();
        int id3 = id.getAndIncrement();
        this.addSelect(shardNum, Lists.newArrayList(id1, id2, id3));
        return Lists.newArrayList(
                getInsertStatement(shardNum, id1), getInsertStatement(shardNum, id2), getInsertStatement(shardNum, id3),
                getUpdateStatement(shardNum, id2),
                getDeleteStatement(shardNum, id3)
        );
    }

    private void addSelect(int shardNum, List<Integer> integers) {
        List<String> sql = Lists.newArrayList();
        for (Integer integer : integers) {
            sql.add(getSelectStatement(shardNum, integer));
        }

        selectList.add(Pair.from(new Date(), sql));
    }

    protected String getSelectStatement(int shardNum, Integer id) {
        return String.format("SELECT id,col_str,datachange_lasttime FROM %s.%s WHERE id = %d;", getDbName(shardNum), TABLE_NAME, id);
    }

    protected String getDeleteStatement(int shardNum, int id3) {
        return String.format("DELETE FROM %s.%s WHERE id = %d;", getDbName(shardNum), TABLE_NAME, id3);
    }

    protected String getUpdateStatement(int shardNum, int id2) {
        return String.format("UPDATE %s.%s t SET t.col_str = '%s' WHERE t.id = %d;", getDbName(shardNum), TABLE_NAME, UUID.randomUUID(), id2);
    }

    protected String getInsertStatement(int shardNum, int id1) {
        return String.format("INSERT INTO %s.%s (id, col_str, datachange_lasttime) VALUES (%d, '%s', DEFAULT);", getDbName(shardNum), TABLE_NAME, id1, UUID.randomUUID());
    }

    private static String getDbName(int shardNum) {
        return TEST_SHARD_DB_PREFIX + shardNum;
    }

    private List<String> initInsert() {
        return Lists.newArrayList();
    }

    private List<String> initUpdate() {
        return Lists.newArrayList();
    }

    private List<String> initDelete() {
        return Lists.newArrayList();
    }


}
