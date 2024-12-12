package com.ctrip.framework.drc.core.concurrent;

import com.ctrip.framework.drc.core.server.utils.ThreadUtils;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

/**
 * @author yongnian
 * @create 2024/11/4 20:29
 */
public class AllSuccessFutureTest {
    @Mock
    List<Future<Boolean>> dcFutures;
    @InjectMocks
    AllSuccessFuture allSuccessFuture;

    @Test
    public void testTrue() throws ExecutionException, InterruptedException, TimeoutException {
        ExecutorService executorService = ThreadUtils.newCachedThreadPool("test");
        List<Future<Boolean>> list = new ArrayList<>();
        list.add(executorService.submit(getBooleanCallable(true)));
        list.add(executorService.submit(getBooleanCallable(true)));
        list.add(executorService.submit(getBooleanCallable(true)));
        AllSuccessFuture allSuccessFuture1 = new AllSuccessFuture(list);
        Assert.assertTrue(allSuccessFuture1.get(1, TimeUnit.SECONDS));
    }

    @Test
    public void testFalse() throws ExecutionException, InterruptedException, TimeoutException {
        ExecutorService executorService = ThreadUtils.newCachedThreadPool("test");
        List<Future<Boolean>> list = new ArrayList<>();
        list.add(executorService.submit(getBooleanCallable(true)));
        list.add(executorService.submit(getBooleanCallable(false)));
        list.add(executorService.submit(getBooleanCallable(true)));
        AllSuccessFuture allSuccessFuture1 = new AllSuccessFuture(list);
        Assert.assertFalse(allSuccessFuture1.get(1, TimeUnit.SECONDS));
    }

    private static Callable<Boolean> getBooleanCallable(boolean result) {
        return () -> {
            try {
                Thread.sleep(100);
                return result;
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            return true;
        };
    }
}

//Generated with love by TestMe :) Please raise issues & feature requests at: https://weirddev.com/forum#!/testme