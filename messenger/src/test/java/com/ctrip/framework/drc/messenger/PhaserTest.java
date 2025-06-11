package com.ctrip.framework.drc.messenger;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;
import java.util.concurrent.*;

/**
 * Created by dengquanliang
 * 2025/6/11 14:24
 */
public class PhaserTest {

    protected static final Logger logger = LoggerFactory.getLogger(PhaserTest.class);


    @Test
    public void test() throws InterruptedException {
        long start = System.currentTimeMillis();
        Phaser phaser = new Phaser();
        phaser.register();
        for (int i = 0; i < 10; i++) {
            new TestTask(phaser).start();
        }

//        new Thread(new Runnable() {
//            @Override
//            public void run() {
//                try {
//                    TimeUnit.SECONDS.sleep(5);
//                } catch (InterruptedException e) {
//                    throw new RuntimeException(e);
//                }
//
//                phaser.forceTermination();
//                System.out.println("phaser terminated");
//            }
//        }).start();

        phaser.arriveAndAwaitAdvance();

        System.out.println("all work finished in " + (System.currentTimeMillis() - start) +  "ms");

    }

    static class TestTask extends Thread {
        private Phaser phaser;

        public TestTask(Phaser phaser) {
            this.phaser = phaser;
            this.phaser.register();
        }

        @Override
        public void run() {
            try {
                System.out.println("The thread [" + getName() + "] is working");
                TimeUnit.SECONDS.sleep(1);
//                TimeUnit.SECONDS.sleep(new Random().nextInt(5));
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            System.out.println("The thread [" + getName() + "] work finished");
            phaser.arriveAndDeregister();

        }

    }
}
