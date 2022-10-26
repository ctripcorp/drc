package com.ctrip.framework.drc.core.server.utils;

/**
 * @Author limingdong
 * @create 2022/10/27
 */
public class FileContext {

    private String filePath;

    private long pid;

    public FileContext(String filePath, long pid) {
        this.filePath = filePath;
        this.pid = pid;
    }

    public String getFilePath() {
        return filePath;
    }

    public void setFilePath(String filePath) {
        this.filePath = filePath;
    }

    public long getPid() {
        return pid;
    }

    public void setPid(long pid) {
        this.pid = pid;
    }
}
