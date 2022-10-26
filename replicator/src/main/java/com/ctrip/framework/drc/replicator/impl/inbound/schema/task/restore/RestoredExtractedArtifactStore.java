package com.ctrip.framework.drc.replicator.impl.inbound.schema.task.restore;

import com.wix.mysql.store.SafeExtractedArtifactStore;
import de.flapdoodle.embed.process.config.store.IDownloadConfig;
import de.flapdoodle.embed.process.distribution.Distribution;
import de.flapdoodle.embed.process.extract.DirectoryAndExecutableNaming;
import de.flapdoodle.embed.process.extract.IExtractedFileSet;
import de.flapdoodle.embed.process.store.IDownloader;

import java.io.IOException;

/**
 * @Author limingdong
 * @create 2022/10/27
 */
public class RestoredExtractedArtifactStore extends SafeExtractedArtifactStore {

    public RestoredExtractedArtifactStore(IDownloadConfig downloadConfig, IDownloader downloader, DirectoryAndExecutableNaming extraction, DirectoryAndExecutableNaming temp) {
        super(downloadConfig, downloader, extraction, temp);
    }

    @Override
    public IExtractedFileSet extractFileSet(Distribution distribution) throws IOException {
        return restoreExtractFileSet(distribution);
    }
}