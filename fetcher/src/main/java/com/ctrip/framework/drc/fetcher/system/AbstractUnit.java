package com.ctrip.framework.drc.fetcher.system;

import com.ctrip.framework.drc.fetcher.system.lifecycle.DrcLifecycle;

import java.lang.reflect.Field;
import java.util.Map;

/**
 * @Author Slight
 * Sep 18, 2019
 */
public abstract class AbstractUnit extends DrcLifecycle implements Unit {

    protected AbstractSystem system;

    @Override
    public void setSystem(AbstractSystem system) {
        this.system = system;
    }

    @Override
    public AbstractSystem getSystem() {
        return system;
    }

    protected String getSystemName() {
        try {
            return system.getName();
        } catch (Throwable t) {
            return "UNSET";
        }
    }

    protected Resource getResource(String name) {
        return system.resources.get(name);
    }

    protected Activity getActivity(String name) { return system.activities.get(name); }

    protected Map<String, Resource> getResources() {
        return system.resources;
    }

    @SuppressWarnings("unchecked")
    @Override
    public Unit derive(Class clazz) throws Exception {
        Unit unit = (Unit) clazz.getConstructor().newInstance();
        unit.setSystem(getSystem());
        unit.load();
        for (Field field : clazz.getFields()) {
            if (field.isAnnotationPresent(Derived.class)) {
                field.set(unit, this.getClass().getField(field.getName()).get(this));
            }
        }
        return unit;
    }

    @Override
    public void load() throws Exception {
        for (Field field : this.getClass().getFields()) {
            //inject resource
            if (field.isAnnotationPresent(InstanceResource.class)) {
                field.set(this, getResource(field.getType().getSimpleName()));
            }

            //inject activity
            if (field.isAnnotationPresent(InstanceActivity.class)) {
                field.set(this, getActivity(field.getType().getSimpleName()));
            }

            //inject config
            if (field.isAnnotationPresent(InstanceConfig.class)) {
                InstanceConfig instanceConfig = field.getAnnotation(InstanceConfig.class);
                loadConfig(this, field, instanceConfig.path());
            }
        }
    }

    protected void loadConfig(Object object, Field field, String path) throws Exception {
        system.loadConfig(object, field, path);
    }

    public String namespace() {
        return null;
    }
}
