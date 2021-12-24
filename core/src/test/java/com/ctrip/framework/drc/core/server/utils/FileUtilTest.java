package com.ctrip.framework.drc.core.server.utils;

import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * Created by mingdongli
 * 2019/9/17 下午5:08.
 */
public class FileUtilTest {

    @Test
    public void testSortDataDirAscending() {
        File[] files = new File[6];

        files[0] = new File("rbinlog.000001");
        files[1] = new File("rbinlog.000010");
        files[2] = new File("rbinlog.000110");
        files[3] = new File("rbinlog.000004");
        files[4] = new File("rbinlog.000021");
        files[5] = new File("rbinlog.000000002");

        File[] orig = files.clone();

        List<File> filelist = FileUtil.sortDataDir(files, "rbinlog", false);

        assertEquals(orig[2], filelist.get(0));
        assertEquals(orig[4], filelist.get(1));
        assertEquals(orig[1], filelist.get(2));
        assertEquals(orig[3], filelist.get(3));
        assertEquals(orig[5], filelist.get(4));
        assertEquals(orig[0], filelist.get(5));
    }

    @Test
    public void testSortDataDirWithDiffName() {
        File[] files = new File[5];

        files[0] = new File("rbinlog.000001");
        files[1] = new File("rbinlog.000010");
        files[2] = new File("rbin.000110");
        files[3] = new File("rbinlog.000004");
        files[4] = new File("rbinlog.000021");

        File[] orig = files.clone();

        List<File> filelist = FileUtil.sortDataDir(files, "rbinlog", false);

        Assert.assertEquals(filelist.size(), 4);

        assertEquals(orig[4], filelist.get(0));
        assertEquals(orig[1], filelist.get(1));
        assertEquals(orig[3], filelist.get(2));
        assertEquals(orig[0], filelist.get(3));
    }

}
