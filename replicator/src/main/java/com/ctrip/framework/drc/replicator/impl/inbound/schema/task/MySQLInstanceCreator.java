package com.ctrip.framework.drc.replicator.impl.inbound.schema.task;

import com.wix.mysql.EmbeddedMysql;
import com.wix.mysql.config.MysqldConfig;
import org.apache.commons.io.FileUtils;

import java.io.File;

/**
 * @Author limingdong
 * @create 2022/10/26
 */
public class MySQLInstanceCreator implements MySQLInstance {

    private EmbeddedMysql embeddedDb;

    public MySQLInstanceCreator(EmbeddedMysql embeddedDb) {
        this.embeddedDb = embeddedDb;
    }

    @Override
    public void destroy () {
        embeddedDb.destroy();
        MysqldConfig mysqldConfig = embeddedDb.getConfig();
        deleteDirectory(mysqldConfig.getTempDir());
    }

    public static void deleteDirectory(String directory) {
        try {
            FileUtils.deleteDirectory(new File(directory));
        } catch (Exception e) {
        }
    }
}
