package com.wix.mysql;

import com.wix.mysql.distribution.Version;
import de.flapdoodle.embed.process.config.store.FileSet;
import de.flapdoodle.embed.process.config.store.IPackageResolver;
import de.flapdoodle.embed.process.distribution.ArchiveType;
import de.flapdoodle.embed.process.distribution.BitSize;
import de.flapdoodle.embed.process.distribution.Distribution;

import static de.flapdoodle.embed.process.distribution.ArchiveType.TXZ;
import static de.flapdoodle.embed.process.distribution.BitSize.B32;
import static de.flapdoodle.embed.process.distribution.BitSize.B64;
import static java.lang.String.format;

public class PackagePaths implements IPackageResolver {
    @Override
    public FileSet getFileSet(Distribution distribution) {
        return com.wix.mysql.distribution.FileSet.emit(
                distribution.getPlatform(),
                (Version) distribution.getVersion());
    }

    @Override
    public ArchiveType getArchiveType(Distribution distribution) {
        Version version = (Version)distribution.getVersion();
        return version.archiveType();
    }

    @Override
    public String getPath(Distribution distribution) {
        String downloadPath = distribution.getVersion().asInDownloadPath();
        Version version = (Version)distribution.getVersion();

        BitSize bs = distribution.getBitsize();
        switch (distribution.getPlatform()) {
            case OS_X:
                // for mac m2 local deploy; change url if your mac version doesn't match
                if (distribution.getVersion().equals(Version.v8_0_32)) {
                    // https://dev.mysql.com/get/Downloads//MySQL-8.0/mysql-8.0.32-macos13-arm64.tar.gz
                    return format("%s-arm64.tar.gz", downloadPath);
                }
                if (System.getProperty("os.arch").equals("aarch64")) {
                    bs = B64;
                }
                return format("%s-x86%s.tar.gz", downloadPath, bs == B32 ? "" : "_64");
            case Linux:
                String gzOrXz = version.archiveType() == TXZ ? "xz" : "gz";
                return format("%s-%s.tar.%s", downloadPath, bs == B32 ? "i686" : "x86_64", gzOrXz);
            case Windows:
                return format("%s-win%s.zip", downloadPath, bs == B32 ? "32" : "x64");
            default:
                throw new RuntimeException("Not implemented for: " + distribution.getPlatform());
        }
    }
}
