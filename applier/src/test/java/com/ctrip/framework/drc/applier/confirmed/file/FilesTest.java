package com.ctrip.framework.drc.applier.confirmed.file;

import com.google.common.io.Files;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

/**
 * @Author Slight
 * Dec 02, 2019
 */
public class FilesTest {
    @Test
    public void testCreateFile() throws IOException {
        File file = new File("/opt/ctrip/data/applier/test-1");
        Files.createParentDirs(file);
        Files.touch(file);
    }
}
