package com.ctrip.framework.drc.core.monitor.reporter;

import com.ctrip.xpipe.api.monitor.Task;

import java.util.concurrent.Callable;

/**
 * @Author limingdong
 * @create 2021/12/1
 */
public class BlankTransactionMonitor implements TransactionMonitor {

    @Override
    public int getOrder() {
        return 1;
    }

    @Override
    public void logTransaction(String type, String name, Task task) throws Exception {
        task.go();
    }

    @Override
    public void logTransactionSwallowException(String type, String name, Task task) {
        try{
            task.go();
        }catch(Throwable th){
        }
    }

    @Override
    public <V> V logTransaction(String type, String name, Callable<V> task) throws Exception {
        try {
            V result = task.call();
            return result;
        } catch(Exception th){
            throw th;
        }
    }

    @Override
    public <V> V logTransactionSwallowException(String type, String name, Callable<V> task) {
        try {
            V result = task.call();
            return result;
        } catch(Throwable th){
        }
        return null;
    }
}
