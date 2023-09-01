package com.ctrip.framework.drc.manager.ha.rest;

import com.ctrip.framework.drc.manager.ha.cluster.ClusterManager;
import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.google.common.util.concurrent.MoreExecutors;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Executor;

/**
 * Created by jixinwang on 2023/8/29
 */
public class MultiMetaServer implements InvocationHandler {

    public static ClusterManager newProxy(ClusterManager dstServer, List<ClusterManager> otherServers) {

        return (ClusterManager) Proxy.newProxyInstance(MultiMetaServer.class.getClassLoader(),
                new Class[] { ClusterManager.class }, new MultiMetaServer(dstServer, otherServers));

    }

    private ClusterManager dstServer;
    private List<ClusterManager> otherServers;
    private Executor executors;

    public MultiMetaServer(ClusterManager dstServer, List<ClusterManager> otherServers) {
        this(dstServer, otherServers, MoreExecutors.directExecutor());
    }

    public MultiMetaServer(ClusterManager dstServer, List<ClusterManager> otherServers, Executor executors) {
        this.dstServer = dstServer;
        this.otherServers = otherServers;
        this.executors = executors;
    }

    @Override
    public Object invoke(Object proxy, final Method method, final Object[] rawArgs) throws Throwable {

        for (final ClusterManager metaServer : otherServers) {

            final Object[] args = copy(rawArgs);

            executors.execute(new AbstractExceptionLogTask() {

                @Override
                protected void doRun() throws Exception {
                    method.invoke(metaServer, args);
                }
            });
        }
        final Object[] args = copy(rawArgs);
        return method.invoke(dstServer, args);
    }

    private Object[] copy(Object[] rawArgs) {

        Object[] array = Arrays.copyOf(rawArgs, rawArgs.length);
        for (int i = 0; i < array.length; i++) {

            if (array[i] instanceof ForwardInfo) {
                ForwardInfo rawForwardInfo = (ForwardInfo) array[i];
                array[i] = rawForwardInfo.clone();
            }
        }
        return array;
    }
}
