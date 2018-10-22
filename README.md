Drill Storage Plugin for IPFS
============


编译说明
-------

将仓库克隆到Drill代码树的`contrib/storage-ipfs`目录下：

```
cd drill/contrib/
git clone https://git.code.tencent.com/drill-over-ipfs/drill-ipfs-storage.git storage-ipfs
```

编辑storage plugin模块的Parent POM (contrib/pom.xml)，在`<modules>`下添加这个插件：

```
  <modules>
    <module>storage-hbase</module>
    <module>format-maprdb</module>
    .....
    <module>storage-ipfs</module>
  </modules>
```

然后到Drill的代码树根目录下执行Build：

```
mvn -T 2C clean install -DskipTests
```

生成的jar包在storage-ipfs/target目录下


测试运行
-------

生成的Drill可执行文件位于`distribution/target/apache-drill-1.14.0`目录下

将整个目录复制到代码树之外，以便后续运行和测试，例如复制为`drill-run`

将生成的`drill-ipfs-storage-1.14.0.jar`复制到`drill-run/jars`中，ipfs的几个依赖包：

```
cid.jar
junit-4.12.jar
multiaddr.jar
multibase.jar
multihash.jar
hamcrest-core-1.3.jar
```

以及IPFS Java API本身的jar包`java-api-ipfs-v1.2.2.jar` （从github release直接下载jar，或者clone仓库后自行编译）
一起复制到`drill-run/jars/3rdparty`目录下。

启动drill-embedded：

```
drill-run/bin/drill-embedded
```

然后到Drill的webui的Storage 标签页下注册IPFS storage plugin

页面最下方有 New Storage Pulugin 编辑框，Name 填 `ipfs`，点击Create;
然后输入ipfs storage plugin的配置，默认配置在`storage-ipfs/src/resources/bootstrap-storage-plugins.json`
复制中间的一段：

```
ipfs : {
      type:"ipfs",
      host: "127.0.0.1",
      port: 5001,
      enabled: false
    }
```

到Configuration的输入框里，点Update，然后回到Storage主页面就可以看到ipfs插件已经注册到Drill里了。点Enable就可以启用这个插件，在query里用ipfs的前缀可以指定使用ipfs存储引擎。



