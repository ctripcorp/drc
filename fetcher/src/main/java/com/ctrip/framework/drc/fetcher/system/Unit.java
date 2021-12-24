package com.ctrip.framework.drc.fetcher.system;

public interface Unit {

    void setSystem(AbstractSystem system);

    AbstractSystem getSystem();

    void load() throws Exception;

    Unit derive(Class clazz) throws Exception;

    String namespace();
}
