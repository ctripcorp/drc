package ctrip.framework.drc.mysql;

import com.wix.mysql.EmbeddedMysql;
import com.wix.mysql.config.Charset;
import com.wix.mysql.config.DownloadConfig;
import com.wix.mysql.config.MysqldConfig;
import ctrip.framework.drc.mysql.utils.ClassUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

import static com.wix.mysql.EmbeddedMysql.anEmbeddedMysql;
import static com.wix.mysql.config.DownloadConfig.aDownloadConfig;
import static com.wix.mysql.distribution.Version.v8_latest;

/**
 * @Author limingdong
 * @create 2020/3/7
 */
public class EmbeddedDb {

    private final static Logger logger = LoggerFactory.getLogger(EmbeddedDb.class);

    public static final String tmp_path_format = "%s/%s";

    public static final String src_path = "com/ctrip/framework/drc/mysql";

    public static final String jar_prefix_file = "file:";

    public static final String jar_postfix_file = ".jar!/";

    public static final String mysql_prefix_file = "mysql";

    public static final String mysql_postfix_file = ".tar.gz";

    public static final String host = "127.0.0.1";

    public static final String user = "root";

    public static final String password = "";

    public int port = 13306;

    private boolean removeFile = false;

    private String tmpPath = "/data/drc/mysql";

    private static String tmpDir = "/opt/data/drc/%s-%d";

    public EmbeddedMysql mysqlServer() {
        return mysqlServer(new DbKey("default", port), new HashMap<>());
    }

    public EmbeddedMysql mysqlServer(DbKey db, Map<String, Object> variables) {
        return mysqlServer(db, removeFile, tmpPath, variables);
    }

    public EmbeddedMysql mysqlServer(DbKey db, boolean removeOldFile, String filePath, Map<String, Object> variables) {
        initParam(db.getPort(), removeOldFile, filePath);

        String mysqldPath = mysqlInstanceDir(db.getName(), db.getPort());
        MysqldConfig.Builder configBuilder = MysqldConfig.aMysqldConfig(v8_latest)
                .withPort(port)
                .withCharset(Charset.UTF8)
                .withUser(user, password)
                .withTempDir(mysqldPath)
                .withTimeout(60, TimeUnit.SECONDS)
                .withServerVariable("lower_case_table_names", 2)
                .withServerVariable("character_set_server", "utf8mb4")
                .withServerVariable("collation_server", "utf8mb4_general_ci");

        variables.forEach((key, value) -> {
            if (value instanceof String) {
                configBuilder.withServerVariable(key, (String) value);
            } else if (value instanceof Boolean) {
                configBuilder.withServerVariable(key, (Boolean) value);
            } else if (value instanceof Integer) {
                configBuilder.withServerVariable(key, (int) value);
            }
        });

        MysqldConfig config = configBuilder.build();
        String path = ClassUtils.getDefaultClassLoader().getResource(src_path).getPath();
        logger.info("[Resource] path is {}, port:{}", path, port);

        if(copyFileIfNecessary(path)) {
            path = String.format(tmp_path_format, tmpPath, src_path);
        }

        DownloadConfig downloadConfig = aDownloadConfig()
                .withCacheDir(path)
                .build();

        return anEmbeddedMysql(config, downloadConfig).start();
    }

    private void initParam(int mySQLport, boolean removeOldFile, String filePath) {
        this.port = mySQLport;
        this.removeFile = removeOldFile;
        this.tmpPath = filePath;
    }

    private Path getDistFile(String path) {

        try {
            if (path.startsWith(tmpPath)) {
                Path dist = Paths.get(path).normalize();
                Path parent = dist.getParent();
                if (parent != null) {
                    Files.createDirectories(parent);
                }
                return dist;
            }
        } catch (Exception e) {
            logger.error("getDistFile error", e);
        }

        return null;
    }

    private boolean isExist(String fileName) {
        File file = new File(fileName);
        return file.exists();
    }

    private boolean delete(String fileName) {
        File file = new File(fileName);
        return file.delete();
    }

    private String parseFileName(String entryName) {
        int lastSlashIndex = entryName.lastIndexOf(File.separator);
        return entryName.substring(lastSlashIndex + 1);
    }

    private boolean shouldSkip(String fileName) {

        if (isExist(fileName)) {
            if (removeFile) {
                delete(fileName);
            } else  {
                return true;
            }
        }

        return false;
    }

    @SuppressWarnings("findbugs:RE_CANT_USE_FILE_SEPARATOR_AS_REGULAR_EXPRESSION")
    private boolean copyFileIfNecessary(String path) {

        if(path.startsWith(jar_prefix_file) && path.contains(jar_postfix_file)) {
            logger.info("[Copy] tar.gz start for {}", path);

            int index = path.indexOf("!");
            String jarFilePath = path.substring(jar_prefix_file.length(), index);
            JarFile jarFile = null;

            try {
                jarFile = new JarFile(jarFilePath);
                Enumeration<JarEntry> entries = jarFile.entries();

                while (entries.hasMoreElements()) {

                    JarEntry jarEntry = entries.nextElement();
                    String entryName = jarEntry.getName();
                    String fileName = parseFileName(entryName);

                    if (fileName.startsWith(mysql_prefix_file) && fileName.endsWith(mysql_postfix_file)) {

                        String[] paths = entryName.split(File.separator);
                        String finalFileName = String.format(tmp_path_format, tmpPath, src_path + File.separator + paths[paths.length - 2] + File.separator + paths[paths.length - 1]);
                        logger.info("[Calculate] finalFileName to {}", finalFileName);
                        if (shouldSkip(finalFileName)) {
                            logger.info("[Skip] copy jarEntry {} to {}", jarEntry, finalFileName);
                            continue;
                        }

                        InputStream inputStream = jarFile.getInputStream(jarEntry);
                        logger.info("[Copy] jarEntry {} to {}", jarEntry, finalFileName);
                        Files.copy(inputStream, getDistFile(finalFileName));
                    }
                }
            } catch (Exception e) {
                logger.error("copyFileIfNecessary error", e);
            } finally {
                try {
                    jarFile.close();
                } catch (Exception e) {
                }
            }

            return true;
        }

        logger.info("[Skip] copy tar.gz for files downloaded already.");
        return false;
    }

    public static String mysqlInstanceDir(String name, long port) {
        return String.format(tmpDir, name, port);
    }
}
