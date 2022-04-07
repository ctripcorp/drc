package com.ctrip.framework.drc.core.driver.binlog.manager.task;

import com.ctrip.framework.drc.core.monitor.datasource.DataSourceManager;
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

/**
 * @Author limingdong
 * @create 2021/4/7
 */
public abstract class AbstractSchemaTask implements NamedCallable<Boolean> {

    private ListeningExecutorService executorService = MoreExecutors.listeningDecorator(ThreadUtils.newCachedThreadPool("Schema-Create-Task"));

    protected Endpoint inMemoryEndpoint;

    protected DataSource inMemoryDataSource;

    public AbstractSchemaTask(Endpoint inMemoryEndpoint, DataSource inMemoryDataSource) {
        this.inMemoryEndpoint = inMemoryEndpoint;
        this.inMemoryDataSource = inMemoryDataSource;
    }

    @Override
    public void afterException(Throwable t) {
        NamedCallable.super.afterException(t);
        DataSourceManager.getInstance().clearDataSource(inMemoryEndpoint);
        inMemoryDataSource = DataSourceManager.getInstance().getDataSource(inMemoryEndpoint);
        NamedCallable.DDL_LOGGER.warn("[Clear] datasource and recreate for {}", inMemoryEndpoint);
    }

    protected boolean doCreate(Collection<String> sqlCollection, Class<? extends BatchTask> clazz, boolean sync) throws Exception {
        List<BatchTask> tasks = getBatchTasks(sqlCollection, clazz);
        return sync ? sync(tasks) : async(tasks);
    }

    private List<BatchTask> getBatchTasks(Collection<String> sqlCollection, Class<? extends BatchTask> clazz) throws InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {
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

    private boolean async(List<BatchTask> tasks) {
        long now = System.currentTimeMillis();
        AtomicBoolean res = new AtomicBoolean(true);
        CountDownLatch countDownLatch = new CountDownLatch(1);
        List<ListenableFuture<Boolean>> queries = Lists.newArrayList();

        for (BatchTask retryTask : tasks) {
            ListenableFuture<Boolean> masterFuture = executorService.submit(retryTask);
            queries.add(masterFuture);
        }

        ListenableFuture<List<Boolean>> successfulQueries = Futures.successfulAsList(queries);
        Futures.addCallback(successfulQueries, new FutureCallback<>() {
            @Override
            public void onSuccess(List<Boolean> result) {
                for (int i = 0; i < result.size(); ++i) {
                    Boolean createRes = result.get(i);
                    if (createRes == null || !createRes) {
                        res.set(false);
                        return;
                    }
                }
                countDownLatch.countDown();
            }

            @Override
            public void onFailure(Throwable t) {
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
