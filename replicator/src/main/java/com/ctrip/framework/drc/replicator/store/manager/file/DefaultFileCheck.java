package com.ctrip.framework.drc.replicator.store.manager.file;

import com.ctrip.framework.drc.core.driver.command.netty.codec.FileCheck;
import com.ctrip.framework.drc.core.driver.command.netty.codec.FileManager;
import com.ctrip.framework.drc.core.server.utils.ThreadUtils;
import io.netty.channel.Channel;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Created by jixinwang on 2023/8/15
 */
public class DefaultFileCheck implements FileCheck {

    private FileManager fileManager;

    private Channel channel;

    private String registerKey;

    private long lastLogSize;

    private ScheduledExecutorService scheduledExecutor;

    @Override
    public void setFileManager(FileManager fileManager) {
        this.fileManager = fileManager;
    }

    @Override
    public void setChannel(Channel channel) {
        this.channel = channel;
    }

    public void startCheck() {
        if (scheduledExecutor == null) {
            scheduledExecutor = ThreadUtils.newSingleThreadScheduledExecutor("File-Check");
        }
        scheduledExecutor.scheduleWithFixedDelay(() -> {
            try {
                long currentLogSize = fileManager.getCurrentLogSize();
                System.out.println("check channel: " + channel.toString() + ", lastSize: " + lastLogSize + ", currentSize: " + currentLogSize);
                if (lastLogSize != 0 && lastLogSize == currentLogSize) {
                    lastLogSize = 0;
                    System.out.println("check channel false: " + channel.toString());
                    channel.close();
                } else {
                    System.out.println("check channel true: " + channel.toString());
                    lastLogSize = currentLogSize;
                }
            } catch (Exception e) {
                System.out.println(e);
            }

        }, 10, 60, TimeUnit.SECONDS);
    }

    public void stopCheck() {
        if (scheduledExecutor != null) {
            scheduledExecutor.shutdownNow();
            scheduledExecutor = null;
        }
    }

    @Override
    public boolean check() {
        return false;
    }
}
