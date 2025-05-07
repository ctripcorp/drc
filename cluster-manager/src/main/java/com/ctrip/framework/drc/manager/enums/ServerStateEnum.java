package com.ctrip.framework.drc.manager.enums;

import com.google.common.collect.Lists;

import java.util.Collection;
import java.util.EnumMap;
import java.util.Map;

/**
 * @author yongnian
 * @create 2024/11/1 16:35
 */
public enum ServerStateEnum {
    NORMAL(0, true),
    LOST(1, false),
    RESTARTING(2, false);


    private final int code;
    private final boolean alive;

    ServerStateEnum(int code, boolean alive) {
        this.code = code;
        this.alive = alive;
    }

    public boolean notAlive() {
        return !alive;
    }

    public int getCode() {
        return code;
    }

    public static ServerStateEnum parseCode(int code) {
        for (ServerStateEnum value : values()) {
            if (value.code == code) {
                return value;
            }
        }
        return null;
    }

    private static final Map<ServerStateEnum, Collection<ServerStateEnum>> FSM = new EnumMap<>(ServerStateEnum.class);

    static {
        FSM.put(NORMAL, Lists.newArrayList(LOST));
        FSM.put(LOST, Lists.newArrayList(RESTARTING));
        FSM.put(RESTARTING, Lists.newArrayList(NORMAL, LOST));
    }

    public final synchronized boolean pushTo(ServerStateEnum next) {
        return FSM.get(this).contains(next);
    }
}
