package com.wix.mysql.store;

import de.flapdoodle.embed.process.config.store.FileSet;
import de.flapdoodle.embed.process.config.store.FileType;
import de.flapdoodle.embed.process.config.store.IDownloadConfig;
import de.flapdoodle.embed.process.distribution.Distribution;
import de.flapdoodle.embed.process.extract.DirectoryAndExecutableNaming;
import de.flapdoodle.embed.process.extract.FilesToExtract;
import de.flapdoodle.embed.process.extract.IExtractedFileSet;
import de.flapdoodle.embed.process.extract.ImmutableExtractedFileSet;
import de.flapdoodle.embed.process.io.file.Files;
import de.flapdoodle.embed.process.store.ExtractedArtifactStore;
import de.flapdoodle.embed.process.store.IDownloader;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;

/**
 * This is a wrapper around `ExtractedArtifactStore` which deletes the temp directory BEFORE extracting
 * just in case we have left overs from last crashed run.
 */
public class SafeExtractedArtifactStore extends ExtractedArtifactStore {
    private String directory;
    private final IDownloadConfig downloadConfig;
    private final DirectoryAndExecutableNaming extraction;

    public SafeExtractedArtifactStore(IDownloadConfig downloadConfig, IDownloader downloader, DirectoryAndExecutableNaming extraction, DirectoryAndExecutableNaming directory) {
        super(downloadConfig, downloader, extraction, directory);
        this.downloadConfig = downloadConfig;
        this.extraction = extraction;
        this.directory = directory.getDirectory().asFile().getAbsolutePath();
    }

    @Override
    public IExtractedFileSet extractFileSet(Distribution distribution) throws IOException {
        if (new File(directory + "/bin/mysqld.pid").exists()) {
            FileUtils.forceDelete(new File(directory + "/bin/mysqld.pid"));
            return restoreExtractFileSet(distribution);
        } else {
            return super.extractFileSet(distribution);
        }
    }

    protected IExtractedFileSet restoreExtractFileSet(Distribution distribution) {
        File destinationDir = new File(directory);
        ImmutableExtractedFileSet.Builder fileSetBuilder = ImmutableExtractedFileSet.builder(destinationDir)
                .baseDirIsGenerated(true);

        FileSet fileSet = downloadConfig.getPackageResolver().getFileSet(distribution);
        for (FileSet.Entry file : fileSet.entries()) {
            if (file.type()==FileType.Executable) {
                String executableName = FilesToExtract.executableName(extraction.getExecutableNaming(), file);
                File executableFile = Files.fileOf(destinationDir, executableName);
                fileSetBuilder.file(file.type(), executableFile);
            } else {
                fileSetBuilder.file(file.type(), Files.fileOf(destinationDir, FilesToExtract.fileName(file)));
            }
        }

        IExtractedFileSet extractedFileSet = fileSetBuilder.build();

        return extractedFileSet;
    }
}