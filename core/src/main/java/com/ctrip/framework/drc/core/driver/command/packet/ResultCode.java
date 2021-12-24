package com.ctrip.framework.drc.core.driver.command.packet;

import com.ctrip.framework.drc.core.driver.IoCache;
import com.ctrip.framework.drc.core.driver.binlog.LogEvent;
import com.ctrip.framework.drc.core.driver.binlog.impl.DrcErrorLogEvent;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import org.slf4j.Logger;

import java.util.Collection;

/**
 * Created by mingdongli
 * 2019/10/22 下午12:05.
 */
public enum ResultCode {

    HANDLE_SUCCESS(0, "handle success"),

    HANDLE_FAIL(1, "handle fail"),

    APPLIER_GTID_ERROR(2, "invalid gtid"),

    REPLICATOR_NOT_READY(3, "not ready to serve for finding first file contained gtidset"),

    REPLICATOR_SEND_BINLOG_ERROR(4, "send binlog error"),

    PORT_ALREADY_EXIST(5, "port already in use"),

    SERVER_ALREADY_EXIST(6, "server already exists"),

    UNKNOWN_ERROR(100, "unknown error");

    private int code;

    private String message;

    ResultCode(int code, String message) {
        this.code = code;
        this.message = message;
    }

    public int getCode() {
        return code;
    }

    public static ResultCode getResultCode(int error) {
        for (ResultCode resultCode : values()) {
            if (resultCode.getCode() == error) {
                return resultCode;
            }
        }

        return UNKNOWN_ERROR;
    }

    public static ResultCode getUnknownError(String error) {
        UNKNOWN_ERROR.setMessage(error);
        return UNKNOWN_ERROR;
    }

    public void setCode(int code) {
        this.code = code;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public void sendResultCode(Channel channel, Logger logger) {
        DrcErrorLogEvent errorLogEvent = new DrcErrorLogEvent(this.getCode(), this.getMessage());
        try {
            errorLogEvent.write(new IoCache() {
                @Override
                public void write(byte[] data) {
                }

                @Override
                public void write(Collection<ByteBuf> byteBufs) {
                    for (ByteBuf byteBuf : byteBufs) {
                        byteBuf.readerIndex(0);
                        channel.writeAndFlush(byteBuf);
                    }
                }

                @Override
                public void write(Collection<ByteBuf> byteBuf, boolean isDdl) {
                }

                @Override
                public void write(LogEvent logEvent) {
                }
            });
        } catch (Exception e) {
            logger.error("sendResultCode to {} error", channel.remoteAddress(), e);
        }
        logger.info("[Send] {}:{} to {}", errorLogEvent.getErrorNumber(), errorLogEvent.getMessage(), channel.remoteAddress());
    }
}
