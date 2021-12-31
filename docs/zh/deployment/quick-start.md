# 准备工作

## Java

* JDK 11+

## 代码编译
- 清空本地 maven 仓库的 com 和 org 目录；
- DRC 所有携程相关的 maven 依赖放置在项目 mvn_repo 分支下；
```
 git checkout mvn_repo
```
在 drc 目录下, 运行 sh install.sh, 自动将非公共 maven 仓库的依赖装载在本地 maven 系统中；
- 编译代码
```
 mvn clean install -Ppackage,local
```

## Docker

```txt
brew install --cask --appdir=/Applications docker 
```

## MySQL

* MySQL镜像，要求：5.7.22+。

```txt
docker pull mysql:5.7
```

# 启动双向复制

```txt


```
