package com.ctrip.framework.drc.core.server.utils;

import com.google.common.collect.Lists;
import org.apache.commons.io.FileUtils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * Created by mingdongli
 * 2019/9/16 下午9:14.
 */
public class FileUtil {

    private static final String pid_postfix = "bin/mysqld.pid";

    public static List<File> sortDataDir(File[] files, String prefix, boolean ascending) {
        if (files == null) {
            return new ArrayList<>(0);
        }
        List<File> fileList = Lists.newArrayList();
        for (File file : files) {
            if (file.getName().startsWith(prefix)) {
                fileList.add(file);
            }
        }
        Collections.sort(fileList, new DataDirFileComparator(prefix, ascending));
        return fileList;
    }

    public static long getFileNumFromName(String name, String prefix) {
        long fileNum = -1;
        String[] nameParts = name.split("\\.");
        if (nameParts.length >= 2 && nameParts[0].equals(prefix)) {
            try {
                fileNum = Long.parseLong(nameParts[1]);
            } catch (NumberFormatException e) {
            }
        }
        return fileNum;
    }

    @SuppressWarnings("findbugs:NP_NULL_ON_SOME_PATH_FROM_RETURN_VALUE")
    public static FileContext restore(String dir) throws Exception {
        File mysqlDir = new File(dir);
        File[] files = mysqlDir.listFiles();

        FileTime latestTime = FileTime.fromMillis(1);
        long pid = -1;
        String realPath = null;

        for (File file : files) {
            String tmpName = file.getAbsolutePath();
            File pidFile = new File(String.format("%s/%s", tmpName, pid_postfix));
            BasicFileAttributes attr = Files.readAttributes(pidFile.toPath(), BasicFileAttributes.class);
            FileTime fileTime = attr.creationTime();
            if (fileTime.compareTo(latestTime) > 0) {
                latestTime = fileTime;
                BufferedReader in = new BufferedReader(new FileReader(pidFile));
                StringBuffer sb = new StringBuffer(in.readLine());
                pid = Long.parseLong(sb.toString());
                realPath = tmpName;
                in.close();
            }
        }

        if (pid == -1 || realPath == null) {
            throw new RuntimeException("not found");
        }
        return new FileContext(realPath, pid);
    }

    /**
     * Compare file file names of form "prefix.version". Sort order result
     * returned in order of version.
     */
    public static class DataDirFileComparator implements Comparator<File>, Serializable {

        private static final long serialVersionUID = -2648639884525140318L;

        private String prefix;

        private boolean ascending;

        public DataDirFileComparator(String prefix, boolean ascending) {
            this.prefix = prefix;
            this.ascending = ascending;
        }

        public int compare(File o1, File o2) {
            return this.compare(o1.getName(), o2.getName());
        }

        public int compare(String o1, String o2) {
            long z1 = getFileNumFromName(o1, prefix);
            long z2 = getFileNumFromName(o2, prefix);
            int result = z1 < z2 ? -1 : (z1 > z2 ? 1 : 0);
            return ascending ? result : -result;
        }

    }

    public static void deleteDirectory(String directory) {
        try {
            FileUtils.deleteDirectory(new File(directory));
        } catch (Exception e) {
        }
    }

    public static void deleteFiles(File logDir) {
        File[] files = logDir.listFiles();
        if (files == null) {
            return;
        }
        for (File file : files) {
            if (file.isFile()) {
                file.delete();
            } else {
                deleteFiles(file);
            }
        }
        logDir.delete();
    }
}
