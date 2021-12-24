package com.ctrip.framework.drc.fetcher.system;

import com.ctrip.framework.drc.fetcher.activity.event.AcquireActivity;
import com.ctrip.framework.drc.fetcher.activity.event.ReleaseActivity;
import com.ctrip.framework.drc.fetcher.resource.thread.ExecutorResource;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

/**
 * @Author Slight
 * May 14, 2020
 */
public class AbstractLinkTest {

    public static class TestSource extends AbstractLoopActivity implements TaskSource<Integer> {

        TaskActivity<Integer, ?> latter;

        @Override
        public void loop() throws InterruptedException {
            for (int i = 0; i < 5; i++) {
                for (int j = 0; j < 10; j++) {
                    latter.waitSubmit(j);
                }
            }
        }

        @Override
        public <U> TaskActivity<Integer, U> link(TaskActivity<Integer, U> latter) {
            this.latter = latter;
            return latter;
        }
    }

    public static class A1 extends TaskQueueActivity<Integer, String> {

        @Override
        public Integer doTask(Integer task) throws InterruptedException {
            return hand(task + "");
        }

        @Override
        protected void select(String task) {
            if (task.equals("0")) {
                super.select(task);
            }
        }
    }


    public static class A2 extends TaskQueueActivity<String, Boolean> {

        @Override
        public String doTask(String task) throws InterruptedException {
            System.out.println(task + "-" + this.hashCode());
            return finish(task);
        }
    }

    class Link extends AbstractLink {

        public Link() throws Exception {
            resources.put("Executor", new ExecutorResource());

            source(TestSource.class)
                    .link(A1.class)
                    .link(A2.class, 10);
            check();
        }
    }


    public static class A3 extends A1 implements AcquireActivity {

        @Override
        public boolean tryAcquire(long timeout, TimeUnit unit) throws InterruptedException {
            return false;
        }
    }

    public static class A4 extends A2 implements ReleaseActivity {

        @Override
        public void release() {

        }
    }

    class Link1 extends AbstractLink {

        public Link1() throws Exception {
            resources.put("Executor", new ExecutorResource());
            source(TestSource.class)
                    .link(A3.class)
                    .link(A4.class);
            check();
        }
    }

    @Test(expected = IllegalStateException.class)
    public void testException() throws Exception {
        new Link();
    }

    @Test
    public void testOk() throws Exception {
        Link1 link1 = new Link1();
        link1.initialize();
        link1.start();
        link1.stop();
        link1.dispose();
    }
}