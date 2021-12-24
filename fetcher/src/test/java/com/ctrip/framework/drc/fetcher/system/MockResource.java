package com.ctrip.framework.drc.fetcher.system;

/**
 * @Author Slight
 * Sep 18, 2019
 */
public class MockResource implements Resource {

    public int disposeCalled = 0;
    public int initializeCalled = 0;

    @Override
    public void dispose() {
        disposeCalled++;
    }

    @Override
    public void initialize() {
        initializeCalled++;
    }

    @Override
    public void setSystem(AbstractSystem system) {

    }

    @Override
    public AbstractSystem getSystem() {
        return null;
    }

    @Override
    public void load() throws Exception {}

    @Override
    public Unit derive(Class clazz) throws Exception {
        return null;
    }

    @Override
    public String namespace() {
        return null;
    }
}
