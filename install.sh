#!/bin/bash

set -euo pipefail
LOG_FILE="maven_install.log"
exec > >(tee -a "$LOG_FILE") 2>&1

find_artifact_dirs() {
    find "$1" -type f -name '*-*.pom' -print0 |
    while IFS= read -r -d '' pom_file; do
        dirname "$(dirname "$pom_file")"
    done | sort -u
}

install_artifacts() {
    local root_dir=$1

    echo "==== Processing artifacts in $root_dir ===="

    while IFS= read -r artifact_dir; do
        local artifact_id=$(basename "$artifact_dir")
        echo "[Artifact] $artifact_id"

        find "$artifact_dir" -mindepth 1 -maxdepth 1 -type d | while read version_dir; do
            local version=$(basename "$version_dir")
            local base_file="$version_dir/$artifact_id-$version"
            local pom_file="${base_file}.pom"
            local jar_file="${base_file}.jar"

            if [[ ! -f "$pom_file" ]]; then
                echo "  ❗ 版本 $version 缺少POM文件"
                continue
            fi

            # 智能安装逻辑
            if [[ -f "$jar_file" ]]; then
                echo "  ✓ 安装JAR: ${version} (自动包含POM)"
                mvn install:install-file \
                    -Dfile="$jar_file" \
                    -DpomFile="$pom_file" \
                    -DgeneratePom=false \
                    >> "$LOG_FILE" 2>&1
            else
                echo "  ✓ 安装POM: ${version}"
                mvn install:install-file \
                    -Dfile="$pom_file" \
                    -DpomFile="$pom_file" \
                    -Dpackaging=pom \
                    >> "$LOG_FILE" 2>&1
            fi
        done
    done < <(find_artifact_dirs "$root_dir")
}

echo "===== 开始安装 [$(date +"%Y-%m-%d %H:%M:%S")] ====="

currdir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
cd "$currdir"

# 批量安装所有组件（自动识别类型）
#install_artifacts "com/ctrip"
#install_artifacts "ctripgroup"
install_artifacts "qunar"

echo "===== 安装完成 [$(date +"%Y-%m-%d %H:%M:%S")] ====="
echo "详细日志见: $LOG_FILE"