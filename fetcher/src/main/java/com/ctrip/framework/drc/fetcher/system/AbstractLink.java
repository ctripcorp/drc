package com.ctrip.framework.drc.fetcher.system;

import com.ctrip.framework.drc.fetcher.activity.event.AcquireActivity;
import com.ctrip.framework.drc.fetcher.activity.event.ReleaseActivity;

import java.util.ArrayList;
import java.util.List;

/**
 * @Author Slight
 * May 13, 2020
 */
public class AbstractLink extends AbstractSystem {

    public static class LinkBuilder<T> {

        public List<? extends TaskSource<T>> currents;
        public AbstractSystem system;

        public LinkBuilder(TaskSource<T> current, AbstractSystem system) {
            List<TaskSource<T>> currents = new ArrayList<>();
            currents.add(current);
            this.currents = currents;
            this.system = system;
        }

        public LinkBuilder(List<? extends TaskSource<T>> currents, AbstractSystem system) {
            this.currents = currents;
            this.system = system;
        }

        @SuppressWarnings("unchecked")
        public <U> LinkBuilder<U> link(Class latterClass, int parallel) throws Exception {
            List<TaskActivity<T, U>> alters = new ArrayList<>();
            for (int i = 0; i < parallel; i++) {
                TaskActivity<T, U> latter = (TaskActivity<T, U>) latterClass.getConstructor().newInstance();
                latter.setSystem(system);
                system.activities.put(latterClass.getSimpleName() + "-" + latter.hashCode(), latter);
                alters.add(latter);
            }
            for (int i = 0; i < currents.size(); i++) {
                for (int j = 0; j < alters.size(); j++) {
                    currents.get(i).link(alters.get(j));
                }
            }
            return new LinkBuilder<>(alters, system);
        }

        @SuppressWarnings("unchecked")
        public <U> LinkBuilder<U> link(Class latterClass) throws Exception {
            TaskActivity<T, U> latter = (TaskActivity<T, U>) latterClass.getConstructor().newInstance();
            latter.setSystem(system);
            system.activities.put(latterClass.getSimpleName(), latter);
            for (int i = 0; i < currents.size(); i++) {
                currents.get(i).link(latter);
            }
            return new LinkBuilder<>(latter, system);
        }

        @SuppressWarnings("unchecked")
        public LinkBuilder<T> with(Class unitClass) throws Exception {
            AbstractUnit unit = (AbstractUnit) unitClass.getConstructor().newInstance();
            unit.setSystem(system);
            if (unit instanceof Activity) {
                system.activities.put(unitClass.getSimpleName(), (Activity) unit);
            }
            if (unit instanceof Resource) {
                String name = unitClass.getSimpleName();
                system.resources.put(
                        name.substring(
                                0,
                                name.lastIndexOf("Resource")
                        ),
                        (Resource) unit
                );
            }
            return this;
        }

    }

    @SuppressWarnings("unchecked")
    protected <T> LinkBuilder<T> source(Class sourceClass) throws Exception {
        TaskSource<T> source = (TaskSource<T>) sourceClass.getConstructor().newInstance();
        source.setSystem(this);
        this.activities.put(sourceClass.getSimpleName(), source);
        return new LinkBuilder<>(source, this);
    }

    protected void check() {
        boolean hasFirstActivity = false;
        boolean hasLastActivity = false;

        for (Activity activity : activities.values()) {
            if (activity instanceof AcquireActivity) {
                hasFirstActivity = true;
            }
            if (activity instanceof ReleaseActivity) {
                hasLastActivity = true;
            }
        }

        String msg = String.format("[hasFirstActivity]:%b, [hasLastActivity]:%b", hasFirstActivity, hasLastActivity);
        if (!(hasFirstActivity && hasLastActivity)) {
            throw new IllegalStateException(msg);
        }

    }
}
