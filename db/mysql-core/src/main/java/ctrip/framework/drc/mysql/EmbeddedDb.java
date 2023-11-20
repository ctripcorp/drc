package ctrip.framework.drc.mysql;

import com.wix.mysql.EmbeddedMysql;
import com.wix.mysql.config.Charset;
import com.wix.mysql.config.DownloadConfig;
import com.wix.mysql.config.MysqldConfig;
import com.wix.mysql.distribution.Version;
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

    public static final String mysql_postfix_file_gz = ".tar.gz";
    public static final String mysql_postfix_file_xz = ".tar.xz";

    public static final String host = "127.0.0.1";

    public static final String user = "root";

    public static final String password = "";
    public static final Version DEFAULT_VERSION = Version.v5_7_23;

    public int port = 13306;

    private boolean removeFile = false;
    private Version version;

    private String tmpPath = "/data/drc/mysql";

    private static String tmpDir = "/opt/data/drc/%s-%d";

    public EmbeddedMysql mysqlServer() {
        return mysqlServer(new DbKey("default", port), new HashMap<>());
    }

    public EmbeddedMysql mysqlServer(DbKey db, Map<String, Object> variables) {
        return mysqlServer(db, removeFile, tmpPath, variables);
    }

    public EmbeddedMysql mysqlServer(DbKey db, Map<String, Object> variables, Version version) {
        return mysqlServer(db, removeFile, tmpPath, variables, version);
    }

    private EmbeddedMysql mysqlServer(DbKey db, boolean removeOldFile, String filePath, Map<String, Object> variables) {
        return mysqlServer(db, removeOldFile, filePath, variables, DEFAULT_VERSION);
    }

    private EmbeddedMysql mysqlServer(DbKey db, boolean removeOldFile, String filePath, Map<String, Object> variables, Version version) {
        initParam(db.getPort(), removeOldFile, filePath, version);

        String mysqldPath = mysqlInstanceDir(db.getName(), db.getPort());
        MysqldConfig.Builder configBuilder = MysqldConfig.aMysqldConfig(version)
                .withPort(port)
                .withCharset(Charset.UTF8)
                .withUser(user, password)
                .withTempDir(mysqldPath)
                .withTimeout(120, TimeUnit.SECONDS)
                .withServerVariable("lower_case_table_names", 1)
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

        if (copyFileIfNecessary(path)) {
            path = String.format(tmp_path_format, tmpPath, src_path);
        }

        DownloadConfig downloadConfig = aDownloadConfig()
                .withCacheDir(path)
                .build();

        return anEmbeddedMysql(config, downloadConfig).start();
    }

    private void initParam(int mySQLport, boolean removeOldFile, String filePath, Version version) {
        this.port = mySQLport;
        this.removeFile = removeOldFile;
        this.tmpPath = filePath;
        this.version = version;
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

                    if (this.isTargetMysqlArtifactFile(fileName)) {
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

    /**
     * @param fileName only 2 options:
     *                 <p>
     *                 1. mysql-5.7.23-linux-glibc2.12-x86_64.tar.gz
     *                 <p>
     *                 2. mysql-8.0.32-linux-glibc2.12-x86_64.tar.xz
     * @return
     * @see package/drc-replicator-package/src/main/resources/com/ctrip/framework/drc/mysql
     */
    private boolean isTargetMysqlArtifactFile(String fileName) {
        boolean isMysqlArtifactFile = fileName.startsWith(mysql_prefix_file) && (fileName.endsWith(mysql_postfix_file_gz) || fileName.endsWith(mysql_postfix_file_xz));
        boolean isTargetMysqlVersion = fileName.contains(version.getMajorVersion());
        return isMysqlArtifactFile && isTargetMysqlVersion;
    }
}
