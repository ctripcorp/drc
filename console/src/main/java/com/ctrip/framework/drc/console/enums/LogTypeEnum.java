package com.ctrip.framework.drc.console.enums;

import org.slf4j.Logger;

/**
 * @author shenhaibo
 * @version 1.0
 * date: 2020-07-27
 */
public enum LogTypeEnum {

    INFO("info") {
        @Override
        public void log(Logger logger, String msg, Throwable t) {
            logger.info(msg);
        }
    },
    WARN("warn") {
        @Override
        public void log(Logger logger, String msg, Throwable t) {
            logger.warn(msg);
        }
    },
    ERROR("error") {
        @Override
        public void log(Logger logger, String msg, Throwable t) {
            logger.error(msg, t);
        }
    };

    private String type;

    LogTypeEnum(String type) {
        this.type = type;
    }

    @Override
    public String toString() {
        return type;
    }

    public abstract void log(Logger logger, String msg, Throwable t);
}
