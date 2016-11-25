1.上传要处理的文件到hdfs
```
1.创建文件夹
    hadoop fs -mkdir -p /input/mahout/pa3/pre
2.上传文件夹
    hadoop fs -put -p  ./pre/*  /input/mahout/pa3/pre/
```
