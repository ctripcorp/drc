package com.ctrip.framework.drc.console.monitor;

import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.internal.verification.VerificationModeFactory;
import org.mockito.stubbing.OngoingStubbing;
import org.mockito.stubbing.Stubber;
import org.mockito.verification.VerificationMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by mingdongli
 * 2019/10/27 下午1:50.
 */
public abstract class MockTest {

    protected Logger logger = LoggerFactory.getLogger(this.getClass());

    public void setUp() throws Exception {
        logger.info("initMocks");
        MockitoAnnotations.initMocks(this);
    }

    protected Stubber doNothing() {
        return Mockito.doNothing();
    }

    protected <T> T verify(T t) {
        return Mockito.verify(t);
    }

    protected <T> T verify(T t, VerificationMode verificationMode) {
        return Mockito.verify(t, verificationMode);
    }

    protected VerificationMode never() {
        return Mockito.never();
    }


    public static VerificationMode times(int wantedNumberOfInvocations) {
        return Mockito.times(wantedNumberOfInvocations);
    }

    public static VerificationMode atMost(int maxNumberOfInvocations) {
        return Mockito.atMost(maxNumberOfInvocations);
    }

    protected <T> T mock(Class<T> t) {
        return Mockito.mock(t);
    }

    protected <T> OngoingStubbing<T> when(T t) {
        return Mockito.when(t);
    }


    public static <T> T any(Class<T> clazz) {
        return Mockito.any(clazz);
    }

    protected <T> T anyObject() {
        return Mockito.anyObject();
    }


    protected int anyInt() {
        return Mockito.anyInt();
    }

    protected String anyString() {
        return Mockito.anyString();
    }
}
