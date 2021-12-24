package com.ctrip.framework.drc.fetcher.system;

/**
 * @Author Slight
 * May 13, 2020
 */
public interface TaskSource<T> extends Activity {

    <U> TaskActivity<T, U> link(TaskActivity<T, U> latter);
}
