
0.下载Hadoop，解压到applications
https://archive.apache.org/dist/hadoop/common/hadoop-2.3.0/hadoop-2.3.0.tar.gz

1.配置环境变量：修改/etc/profile
export HADOOP_HOME=/Applications/hadoop-2.7.3
export PATH=$HADOOP_HOME/bin:$PATH
2.验证配置Hadoop成功
hadoop version

3.导入程序
hadoop jar  /Users/liguohua/Documents/F/code/idea/git/kn-hadoop-maven/target/KangniZhang-hadoop-pa1-1.0-SNAPSHOT.jar \
/Users/liguohua/Documents/F/code/idea/git/kn-hadoop-maven/input/test001 \
/Users/liguohua/Documents/F/code/idea/git/kn-hadoop-maven/output/test001



参考github
https://github.com/cenxiguan/brandeis.big_data
https://github.com/gekonwi/brandeis.big_data# kn-hadoop-maven
# zkn-bigdata
# zkn-bigdata
# zkn-bigdata
# kn-hadoop-maven
