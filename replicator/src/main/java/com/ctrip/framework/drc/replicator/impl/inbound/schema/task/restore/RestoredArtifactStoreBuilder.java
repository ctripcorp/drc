package com.ctrip.framework.drc.replicator.impl.inbound.schema.task.restore;

import com.ctrip.framework.drc.core.server.utils.FileContext;
import com.ctrip.framework.drc.core.server.utils.FileUtil;
import com.wix.mysql.config.DownloadConfig;
import com.wix.mysql.config.DownloadConfigBuilder;
import com.wix.mysql.config.MysqldConfig;
import com.wix.mysql.config.directories.TargetGeneratedFixedPath;
import com.wix.mysql.config.extract.NoopNaming;
import com.wix.mysql.config.extract.PathPrefixingNaming;
import de.flapdoodle.embed.process.extract.DirectoryAndExecutableNaming;
import de.flapdoodle.embed.process.io.directories.FixedPath;
import de.flapdoodle.embed.process.io.directories.IDirectory;
import de.flapdoodle.embed.process.store.Downloader;
import de.flapdoodle.embed.process.store.IArtifactStore;

import java.io.File;

/**
 * @Author limingdong
 * @create 2022/10/26
 */
public class RestoredArtifactStoreBuilder extends de.flapdoodle.embed.process.store.ExtractedArtifactStoreBuilder {

    public RestoredArtifactStoreBuilder defaults(
            final MysqldConfig mysqldConfig,
            final DownloadConfig downloadConfig) throws Exception {

        FileContext fileContext = FileUtil.restore(mysqldConfig.getTempDir());
        String combinedPath = fileContext.getFilePath();
        IDirectory preExtractDir = new FixedPath(new File(downloadConfig.getCacheDir(), "extracted").getPath());

        executableNaming().setDefault(new PathPrefixingNaming("bin/"));
        download().setDefault(new DownloadConfigBuilder().defaults(downloadConfig).build());
        downloader().setDefault(new Downloader());
        extractDir().setDefault(preExtractDir);
        extractExecutableNaming().setDefault(new NoopNaming());

        tempDir().setDefault(new TargetGeneratedFixedPath(combinedPath));

        return this;
    }

    @Override
    public IArtifactStore build() {
        DirectoryAndExecutableNaming extract = new DirectoryAndExecutableNaming(get(EXTRACT_DIR_FACTORY), get(EXTRACT_EXECUTABLE_NAMING));
        DirectoryAndExecutableNaming temp = new DirectoryAndExecutableNaming(tempDir().get(), executableNaming().get());

        return new RestoredExtractedArtifactStore(get(DOWNLOAD_CONFIG), get(DOWNLOADER), extract, temp);
    }

}