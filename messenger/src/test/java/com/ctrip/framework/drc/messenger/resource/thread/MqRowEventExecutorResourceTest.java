package com.ctrip.framework.drc.messenger.resource.thread;

import com.ctrip.xpipe.concurrent.NamedThreadFactory;
import com.google.common.collect.Maps;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Created by shiruixin
 * 2025/2/10 17:38
 */
public class MqRowEventExecutorResourceTest {
    MqRowEventExecutorResource executorResource;
    Map<String, AtomicInteger> activeThreadsMap = Maps.newConcurrentMap();
    AtomicReference<AtomicInteger> max = new AtomicReference<>(new AtomicInteger(0));
    @Before
    public void setUp() throws Exception {
        executorResource = new MqRowEventExecutorResource();
        executorResource.registryKey = "registryKey";
        executorResource.doInitialize();
    }



    @Test
    public void test() {


        ExecutorService service = new ThreadPoolExecutor(
                10, 50, 30, TimeUnit.SECONDS,
                new ArrayBlockingQueue<>(10), new NamedThreadFactory("test"),new ThreadPoolExecutor.CallerRunsPolicy()
        );



        for (int i = 0; i < 50; i++) {
            new Thread(() -> {
                for (int j = 0; j < 100000; j++){
                    service.submit(() -> {
                        for (int z = 0; z < 200; z++){
                            f();
                        }
                    });
                }
            },"Main-"+i).start();

        }

        System.out.println(max.get().get());

        service.shutdown();

    }


    public void f() {
        String name = Thread.currentThread().getName();
        System.out.println(name);
        AtomicInteger atomicInteger = activeThreadsMap.computeIfAbsent("registryKey", (key) -> new AtomicInteger(0));
        atomicInteger.getAndIncrement();
        if (atomicInteger.get() > max.get().get()){
            max.set(atomicInteger);
        }
//        System.out.println(atomicInteger.get());

        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        atomicInteger.getAndDecrement();
    }
}