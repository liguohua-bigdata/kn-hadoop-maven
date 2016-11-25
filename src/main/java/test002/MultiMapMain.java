package test002;

/**
 * Created by liguohua on 16/9/21.
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/*
 * MultipleInputs类指定不同的输入文件路径以及输入文化格式
 现有两份数据
 phone
 123,good number
 124,common number
 123,bad number

 user
 zhangsan,123
 lisi,124
 wangwu,125

 现在需要把user和phone按照phone number连接起来。得到下面的结果
 zhangsan,123,good number
 lisi,123,common number
 wangwu,125,bad number
 */
public class MultiMapMain extends Configuration implements Tool {
    private static Configuration conf = new Configuration();

    public static void main(String[] args) throws Exception {
        args = new String[3];
        args[0] = "/Users/liguohua/Documents/F/code/idea/git/kn-hadoop-maven/input/test002/phone";
        args[1] = "/Users/liguohua/Documents/F/code/idea/git/kn-hadoop-maven/input/test002/user";
        args[2] = "/Users/liguohua/Documents/F/code/idea/git/kn-hadoop-maven/output/test002/";

        ToolRunner.run(conf, new MultiMapMain(), args); // 调用run方法
    }

    @Override
    public Configuration getConf() {
        return new Configuration();
    }

    @Override
    public void setConf(Configuration conf) {

    }

    @Override
    public int run(String[] args) throws Exception {

        Job job = new Job(conf);
        job.setJarByClass(MultiMapMain.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlagString.class);

        job.setReducerClass(MultiRedu.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        // MultipleInputs类添加文件路径
        MultipleInputs.addInputPath(job, new Path(args[0]),
                TextInputFormat.class, MultiMap1.class);
        MultipleInputs.addInputPath(job, new Path(args[1]),
                TextInputFormat.class, MultiMap2.class);

        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
        return 0;
    }
}