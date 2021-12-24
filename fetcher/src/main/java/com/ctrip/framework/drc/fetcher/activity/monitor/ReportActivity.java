package com.ctrip.framework.drc.fetcher.activity.monitor;

import com.ctrip.framework.drc.fetcher.system.TaskQueueActivity;
import com.ctrip.xpipe.spring.RestTemplateFactory;
import org.springframework.web.client.RestOperations;

import java.util.ArrayList;
import java.util.List;

public abstract class ReportActivity<T, U>  extends TaskQueueActivity<T, U> {

    private List<T> taskList = new ArrayList<T>();

    private long lastTime = 0L;

    protected RestOperations restTemplate;

    @Override
    public int queueSize() {
        return 100;
    }

    @Override
    public T doTask(T task) {

        long currentTime = System.currentTimeMillis();

        taskList.add(task);
        if (!hasNext() || currentTime - lastTime > 1000 || taskList.size() > 50) {
            try {
                if (restTemplate == null) {
                    restTemplate = RestTemplateFactory.createCommonsHttpRestTemplate();
                }
                doReport(taskList);
            } catch (Exception e) {
                logger.error("[{}] report error: ", getClass().getName(), e);
            }

            taskList.clear();
        }
        lastTime = currentTime;
        return finish(task);
    }

    public abstract boolean report(T task);

    @Override
    public String namespace() {
        return "report";
    }

    public abstract void doReport(List<T> taskList);
}
