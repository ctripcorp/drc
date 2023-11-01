package com.ctrip.framework.drc.console.vo;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.junit.Test;

import static org.junit.Assert.*;

public class MqConfigVoTest {

    @Test
    public void testFrom() throws InterruptedException {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        SettableFuture<Integer> future0 = SettableFuture.create();
        // 使用其他线程去 set 对应的结果。
        System.out.println(Thread.currentThread().getName() + " submit");
        executor.submit(() -> {
            // 增加线程 sleep 的逻辑。
//            try {
                System.out.println(Thread.currentThread().getName() + " in Running");
//                Thread.sleep(1000);
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }
            future0.set(1);
        });

        Futures.addCallback(future0, new FutureCallback<>() {
            @Override
            public void onSuccess(Integer result) {
                // 这一行会被哪个线程执行？主线程？还是上面的线程池？
                System.out.println(Thread.currentThread().getName() + " call back result=" + result);
            }

            @Override
            public void onFailure(Throwable t) {
            }
        }, MoreExecutors.directExecutor());
        
        Thread.sleep(10000);
    }
}