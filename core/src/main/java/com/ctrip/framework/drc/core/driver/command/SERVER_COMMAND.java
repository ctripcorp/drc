package com.ctrip.framework.drc.core.driver.command;

import com.ctrip.framework.drc.core.driver.command.packet.applier.ApplierDumpCommandPacket;
import com.ctrip.framework.drc.core.driver.command.packet.client.*;
import com.ctrip.framework.drc.core.driver.command.packet.monitor.DelayMonitorCommandPacket;

/**
 * @author wenchao.meng
 * <p>
 * Sep 01, 2019
 */
public enum SERVER_COMMAND {

    COM_BINLOG_DUMP_GTID((byte)0x1e, BinlogDumpGtidCommandPacket.class),

    COM_QUERY((byte)0x03, QueryCommandPacket.class),

    COM_REGISTER_SLAVE((byte)0x15, RegisterSlaveCommandPacket.class),

    COM_APPLIER_BINLOG_DUMP_GTID((byte)0xf0, ApplierDumpCommandPacket.class),

    COM_HEARTBEAT((byte)0x0e, HeartBeatPacket.class),

    COM_DELAY_MONITOR((byte)0xf1, DelayMonitorCommandPacket.class),

    COM_HEARTBEAT_RESPONSE((byte)0xf2, HeartBeatResponsePacket.class);

    private byte code;

    private Class<?> clazz;

    SERVER_COMMAND(byte code, Class<?> clazz) {
        this.code = code;
        this.clazz = clazz;
    }

    public byte getCode() {
        return code;
    }

    public Class<?> getClazz() {
        return clazz;
    }

    public static SERVER_COMMAND getType(int code) {
        for (SERVER_COMMAND command : values()) {
            if (code == command.getCode()) {
                return command;
            }
        }
        return null;
    }
}
