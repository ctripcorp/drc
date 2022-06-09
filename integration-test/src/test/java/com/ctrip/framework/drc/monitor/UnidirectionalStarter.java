package com.ctrip.framework.drc.monitor;

import com.ctrip.framework.drc.monitor.module.AbstractTestStarter;
import org.junit.Before;
import org.junit.Test;

/**
 *  run doTest to start integrity test WITHOUT any interference
 *
 *  start src and dst mysql instance by port 3306 and 3307 if they are not in use, or pick up the first free port starting from 3306
 *  with docker image 'mysql:5.7'
 *
 *  my.cnf is placed in src/my.cnf and dst/my.cnf respectively
 *
 *  docker ps -a | grep mysql | awk '{print $1}' | grep -v CONTAINER | xargs docker rm -f
 *  to stop all containers
 *  docker volume prune
 * Created by mingdongli
 * 2019/10/14 下午11:31.
 */
public class UnidirectionalStarter extends AbstractTestStarter {

    @Override
    @Before
    public void setUp() {
        super.setUp();
//        unidirectionalReplicateModule.setImage("mysql:5.7.22");
    }

    @Test
    public void doTest() throws Exception {
        unidirectionalReplicateModule.startMySQLModule();
        unidirectionalReplicateModule.startRAModule();
        unidirectionalReplicateModule.startMonitorModule();
        Thread.currentThread().join();
    }
}
