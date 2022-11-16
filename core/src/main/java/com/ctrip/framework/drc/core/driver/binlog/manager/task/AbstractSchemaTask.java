package com.ctrip.framework.drc.core.driver.binlog.manager.task;

import com.ctrip.framework.drc.core.monitor.datasource.DataSourceManager;
import com.ctrip.framework.drc.core.monitor.reporter.DefaultEventMonitorHolder;
import com.ctrip.framework.drc.core.server.utils.ThreadUtils;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.*;
import org.apache.tomcat.jdbc.pool.DataSource;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.ctrip.framework.drc.core.driver.binlog.manager.task.BatchTask.MAX_BATCH_SIZE;
import static com.ctrip.framework.drc.core.monitor.datasource.DataSourceManager.MAX_ACTIVE;
import static com.ctrip.framework.drc.core.server.config.SystemConfig.DDL_LOGGER;

/**
 * @Author limingdong
 * @create 2021/4/7
 */
public abstract class AbstractSchemaTask<V> implements NamedCallable<V> {

    private ListeningExecutorService executorService;

    protected Endpoint inMemoryEndpoint;

    protected DataSource inMemoryDataSource;

    private String identity;

    public AbstractSchemaTask(Endpoint inMemoryEndpoint, DataSource inMemoryDataSource) {
        this.inMemoryEndpoint = inMemoryEndpoint;
        this.inMemoryDataSource = inMemoryDataSource;
        this.identity = getClass().getSimpleName() + "-" + inMemoryEndpoint.getSocketAddress();
        executorService = MoreExecutors.listeningDecorator(ThreadUtils.newCachedThreadPool(identity));
    }

    @Override
    public void afterException(Throwable t) {
        NamedCallable.super.afterException(t);
        DataSourceManager.getInstance().clearDataSource(inMemoryEndpoint);
        inMemoryDataSource = DataSourceManager.getInstance().getDataSource(inMemoryEndpoint);
        DDL_LOGGER.warn("[Clear] datasource and recreate for {}", inMemoryEndpoint);
    }

    protected boolean doCreate(Collection<String> sqlCollection, Class<? extends BatchTask> clazz, boolean sync) throws Exception {
        return doCreate(sqlCollection, clazz, sync, MAX_ACTIVE);
    }

    protected boolean doCreate(Collection<String> sqlCollection, Class<? extends BatchTask> clazz, boolean sync, int concurrency) throws Exception {
        List<BatchTask> tasks = getBatchTasks(sqlCollection, clazz);
        return sync ? sync(tasks) : async(tasks, concurrency);
    }

    protected List<BatchTask> getBatchTasks(Collection<String> sqlCollection, Class<? extends BatchTask> clazz) throws InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {
        List<BatchTask> tasks = Lists.newArrayList();

        List<String> sqls = Lists.newArrayList();
        int sqlSize = 0;
        for (String sql : sqlCollection) {
            sqls.add(sql);
            sqlSize++;

            if (sqlSize >= MAX_BATCH_SIZE) {
                tasks.add(getBatchTask(clazz, sqls));
                sqls = Lists.newArrayList();
                sqlSize = 0;
            }
        }

        if (sqlSize > 0) {
            tasks.add(getBatchTask(clazz, sqls));
        }

        return tasks;
    }

    private boolean sync(List<BatchTask> tasks) throws Exception {
        boolean res = true;
        for (BatchTask retryTask : tasks) {
            res = retryTask.call();
            if (!res) {
                return res;
            }
        }
        return res;
    }

    private boolean async(List<BatchTask> tasks, int concurrency) {
        if (tasks.size() <= concurrency) {
            return oneBatch(tasks);
        } else {
            boolean res = true;
            int loopSize = tasks.size() / concurrency + 1;
            for (int i = 0; i < loopSize; ++i) {
                if (i == (loopSize - 1)) {
                    res = oneBatch(tasks.subList(i * concurrency, tasks.size()));
                } else {
                    res = oneBatch(tasks.subList(i * concurrency, (i + 1) * concurrency));
                }
                if (!res) {
                    return res;
                }
            }
            return res;
        }
    }

    protected boolean oneBatch(List<BatchTask> tasks) {
        long now = System.currentTimeMillis();
        AtomicBoolean res = new AtomicBoolean(true);
        CountDownLatch countDownLatch = new CountDownLatch(1);
        List<ListenableFuture<Boolean>> creates = Lists.newArrayList();

        for (BatchTask batchTask : tasks) {
            ListenableFuture<Boolean> createFuture = executorService.submit(batchTask);
            creates.add(createFuture);
        }

        ListenableFuture<List<Boolean>> successfulCreates = Futures.successfulAsList(creates);
        Futures.addCallback(successfulCreates, new FutureCallback<>() {
            @Override
            public void onSuccess(List<Boolean> result) {
                for (int i = 0; i < result.size(); ++i) {
                    Boolean createRes = result.get(i);
                    if (createRes == null || !createRes) {
                        res.set(false);
                        DDL_LOGGER.error("[BatchTask] fail and set res false for {}", identity);
                        break;
                    }
                }
                countDownLatch.countDown();
            }

            @Override
            public void onFailure(Throwable t) {
                res.set(false);
                DDL_LOGGER.error("[BatchTask] fail and set res false for {}", identity);
                countDownLatch.countDown();
            }
        });

        try {
            boolean queryResult = countDownLatch.await(60 * 3 , TimeUnit.SECONDS);
            long elapse = System.currentTimeMillis() - now;
            if (queryResult) {
                DDL_LOGGER.info("[BatchTask] success with {} thread and cost {}", tasks.size(), elapse);
            } else {
                res.set(false);
                DDL_LOGGER.error("[BatchTask] timeout for countDownLatch querying doCreate with {} thread and cost {}", tasks.size(), elapse);
                DefaultEventMonitorHolder.getInstance().logEvent("DRC.replicator.batch.task.timeout", identity);
            }
        } catch (InterruptedException e) {
            DDL_LOGGER.error("doCreate error in countDownLatch wait", e);
        }

        return res.get();
    }

    protected BatchTask getBatchTask(Class<? extends BatchTask> clazz, List<String> sqls) throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException {
        Constructor constructor =  clazz.getConstructor(new Class[]{List.class, Endpoint.class, DataSource.class});
        return (BatchTask) constructor.newInstance(sqls, inMemoryEndpoint, inMemoryDataSource);
    }

}
