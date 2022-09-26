package com.ctrip.framework.drc.core.server.common.enums;

import com.ctrip.framework.drc.core.server.common.filter.row.*;

import static com.ctrip.framework.drc.core.server.common.filter.row.RuleFactory.ROWS_FILTER_RULE;

/**
 * @Author limingdong
 * @create 2022/4/22
 */
public enum RowsFilterType {

    None("none") {
        public Class<? extends RowsFilterRule> filterRuleClass() {
            return NoopRowsFilterRule.class;
        }
    },

    AviatorRegex("aviator_regex") {
        @Override
        public Class<? extends RowsFilterRule> filterRuleClass() {
            return AviatorRegexRowsFilterRule.class;
        }
    },

    JavaRegex("java_regex") {
        @Override
        public Class<? extends RowsFilterRule> filterRuleClass() {
            return JavaRegexRowsFilterRule.class;
        }
    },

    TripUdl("trip_udl") {
        @Override
        public Class<? extends RowsFilterRule> filterRuleClass() {
            return UserRowsFilterRule.class;
        }
    },

    TripUid("trip_uid") {
        @Override
        public Class<? extends RowsFilterRule> filterRuleClass() {
            return UserRowsFilterRule.class;
        }
    },

    Custom("custom") {
        @Override
        public Class<? extends RowsFilterRule> filterRuleClass() throws ClassNotFoundException {
            String clazz = System.getProperty(ROWS_FILTER_RULE);
            return  (Class<RowsFilterRule>) Class.forName(clazz);
        }
    };

    private String name;

    RowsFilterType(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public static RowsFilterType getType(String name) {
        for (RowsFilterType filterType : values()) {
            if (filterType.getName().equalsIgnoreCase(name)) {
                return filterType;
            }
        }

        throw new UnsupportedOperationException("not support for name " + name);
    }

    public abstract Class<? extends RowsFilterRule> filterRuleClass() throws ClassNotFoundException;
}
