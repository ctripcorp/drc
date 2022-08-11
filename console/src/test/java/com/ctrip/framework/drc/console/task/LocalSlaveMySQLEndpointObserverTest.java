package com.ctrip.framework.drc.console.task;

import com.ctrip.framework.drc.console.enums.ActionEnum;
import com.ctrip.framework.drc.console.mock.LocalSlaveMySQLEndpointObservable;
import com.ctrip.framework.drc.console.mock.LocalSlaveMySQLEndpointObserver;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.unidal.tuple.Triple;

import static com.ctrip.framework.drc.console.utils.UTConstants.*;

/**
 * @Author: hbshen
 * @Date: 2021/4/28
 */
public class LocalSlaveMySQLEndpointObserverTest{

    private LocalSlaveMySQLEndpointObserver observer;

    @Before
    public void setUp() {
        observer = new LocalSlaveMySQLEndpointObserver();
        observer.localDcName = DC1;
        observer.onlyCarePart = true;
    }

    @Test
    public void testObserve() {
        Assert.assertEquals(0, observer.slaveMySQLEndpointMap.size());

        observer.update(new Triple<>(META_KEY3, MYSQL_ENDPOINT2_MHA1DC1, ActionEnum.ADD), new LocalSlaveMySQLEndpointObservable());
        Assert.assertEquals(0, observer.slaveMySQLEndpointMap.size());

        observer.update(new Triple<>(META_KEY1, MYSQL_ENDPOINT2_MHA1DC1, ActionEnum.ADD), new LocalSlaveMySQLEndpointObservable());
        Assert.assertEquals(1, observer.slaveMySQLEndpointMap.size());
        Assert.assertEquals(MYSQL_ENDPOINT2_MHA1DC1, observer.slaveMySQLEndpointMap.get(META_KEY1));

        observer.update(new Triple<>(META_KEY1, MYSQL_ENDPOINT2_MHA2DC1, ActionEnum.UPDATE), new LocalSlaveMySQLEndpointObservable());
        observer.update(new Triple<>(META_KEY2, MYSQL_ENDPOINT2_MHA1DC1, ActionEnum.UPDATE), new LocalSlaveMySQLEndpointObservable());
        Assert.assertEquals(2, observer.slaveMySQLEndpointMap.size());
        Assert.assertEquals(MYSQL_ENDPOINT2_MHA2DC1, observer.slaveMySQLEndpointMap.get(META_KEY1));
        Assert.assertEquals(MYSQL_ENDPOINT2_MHA1DC1, observer.slaveMySQLEndpointMap.get(META_KEY2));

        observer.update(new Triple<>(META_KEY1, MYSQL_ENDPOINT2_MHA2DC1, ActionEnum.DELETE), new LocalSlaveMySQLEndpointObservable());
        observer.update(new Triple<>(META_KEY2, MYSQL_ENDPOINT2_MHA2DC1, ActionEnum.DELETE), new LocalSlaveMySQLEndpointObservable());
        Assert.assertEquals(0, observer.slaveMySQLEndpointMap.size());

    }
}
