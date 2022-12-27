package com.ctrip.framework.drc.core.server.utils;

import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.FileTime;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static com.ctrip.framework.drc.core.server.utils.FileUtil.deleteFiles;
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

    @Test
    public void testRestore() throws Exception {

        deleteFiles(new File("/tmp/drc"));

        long now = System.currentTimeMillis();
        FileAttribute<Set<PosixFilePermission>> rwx = PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rwxrwxrwx"));
        Files.createDirectories(Path.of("/tmp/drc/1/bin"), rwx);
        Path path = Path.of("/tmp/drc/1/bin/mysqld.pid");

        Files.createFile(path, rwx);
        Files.write(path, "1".getBytes(), StandardOpenOption.WRITE);
        Files.setAttribute(path, "basic:creationTime", FileTime.fromMillis(now + 10));

        Files.createDirectories(Path.of("/tmp/drc/2/bin"), rwx);
        path = Path.of("/tmp/drc/2/bin/mysqld.pid");

        Files.createFile(path, rwx);
        Files.write(path, "2".getBytes(), StandardOpenOption.WRITE);
        Files.setAttribute(path, "basic:creationTime", FileTime.fromMillis(now + 20));

        Files.createDirectories(Path.of("/tmp/drc/3/bin"), rwx);
        path = Path.of("/tmp/drc/3/bin/mysqld.pid");

        Files.createFile(path, rwx);
        Files.write(path, "3".getBytes(), StandardOpenOption.WRITE);
        Files.setAttribute(path, "basic:creationTime", FileTime.fromMillis(now + 30));

        TimeUnit.SECONDS.sleep(1);
        Files.createDirectories(Path.of("/tmp/drc/4/bin"), rwx);
        path = Path.of("/tmp/drc/4/bin/mysqld.pid");

        Files.createFile(path, rwx);
        Files.write(path, "4".getBytes(), StandardOpenOption.WRITE);
        Files.setAttribute(path, "basic:creationTime", FileTime.fromMillis(now + 40));

        FileContext fileContext = FileUtil.restore("/tmp/drc");
        Assert.assertEquals(fileContext.getFilePath(), "/tmp/drc/4");
        Assert.assertEquals(fileContext.getPid(), 4);

    }

}
