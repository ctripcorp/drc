package com.ctrip.framework.drc.core.server.utils;

import com.google.common.collect.Lists;

import java.io.File;
import java.io.Serializable;
import java.util.*;

/**
 * Created by mingdongli
 * 2019/9/16 下午9:14.
 */
public class FileUtil {

    public static List<File> sortDataDir(File[] files, String prefix, boolean ascending) {
        if (files == null) {
            return new ArrayList<File>(0);
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

}
