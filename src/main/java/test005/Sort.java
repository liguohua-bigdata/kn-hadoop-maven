package test005;

/**
 * Created by liguohua on 16/9/27.
 */


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;


public class Sort {


    //map将输入中的value化成IntWritable类型，作为输出的key

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        args = new String[2];
        args[0] = "/Users/liguohua/Documents/F/code/idea/git/kn-hadoop-maven/input/test005/";
        args[1] = "/Users/liguohua/Documents/F/code/idea/git/kn-hadoop-maven/output/test005/";

        Job job = new Job(conf, "Data Sort");
        job.setJarByClass(Sort.class);
        //设置Map和Reduce处理类
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        //设置输出类型
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);
        //设置输入和输出目录
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }

    public static class Map extends Mapper<Object, Text, IntWritable, IntWritable> {
        private static IntWritable data = new IntWritable();

        //实现map函数
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString();
            data.set(Integer.parseInt(line));
            context.write(data, new IntWritable(1));
        }
    }

    //reduce将输入中的key复制到输出数据的key上，
//然后根据输入的value-list中元素的个数决定key的输出次数
//用全局linenum来代表key的位次
    public static class Reduce extends
            Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
        private static IntWritable linenum = new IntWritable(1);

        //实现reduce函数
        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            for (IntWritable val : values) {
                context.write(linenum, key);
                linenum = new IntWritable(linenum.get() + 1);
            }
        }
    }

}