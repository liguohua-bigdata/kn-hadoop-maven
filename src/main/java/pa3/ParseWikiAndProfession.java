package pa3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import utils.HdfsDevUtil;
import utils.StringDevUitl;

import java.io.IOException;

public class ParseWikiAndProfession {
    //处理成name-->other的形式
    private static void parseFileLine(Mapper<LongWritable, Text, Text, Text>.Context context, String[] pairs) throws IOException, InterruptedException {
        if (pairs.length == 2) {
            String name = pairs[0].trim();
            String line = pairs[1].trim();
            if (StringDevUitl.isNotEmputy(name) && StringDevUitl.isNotEmputy(line)) {
                context.write(new Text(name), new Text(line));
            }
        }
    }

    /**
     * 用于处理profesion.txt用 ':'切分文件中的每一行
     */
    static class MyMapperProfession extends Mapper<Text, Text, Text, Text> {
        protected void map(Text key, Text value, org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, Text, Text>.Context context) throws java.io.IOException, InterruptedException {
            String[] pairs = value.toString().trim().split(":");
            parseFileLine(context, pairs);
        }
    }
    /**
     * 用于处理wiki用 '\t'切分文件中的每一行
     */
    static class MyMapperWiki extends Mapper<Text, Text, Text, Text> {
        protected void map(Text key, Text value, org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, Text, Text>.Context context) throws java.io.IOException, InterruptedException {
            String[] pairs = value.toString().trim().split("\t");
            parseFileLine(context, pairs);
        }
    }

    /**
     * 用于两个mapper的reduce操作，将name作为文件名，写出到多个文件中。
     */
    static class MyReduceProfessionAndWiki extends Reducer<Text, Text, Text, Text> {
        private MultipleOutputs multipleOutputs;

        //1.set up multipleOutputs for save data into  separate files
        protected void setup(Context cxt) throws IOException, InterruptedException {
            multipleOutputs = new MultipleOutputs(cxt);
        }

        @Override
        protected void reduce(Text name, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text value : values) {
                //2. save data into  separate files
                multipleOutputs.write(NullWritable.get(), value, name.toString());
            }
        }

        //3.set up multipleOutputs
        protected void cleanup(Context cxt) throws IOException, InterruptedException {
            multipleOutputs.close();
        }
    }

    public static void main(String[] args) throws Exception {
        //##################可被删除的代码###############################
        args = new String[3];
        args[0] = "hdfs://qingcheng11:9000/input/mahout/pa3/pre/p";
        args[1] = "hdfs://qingcheng11:9000/input/mahout/pa3/pre/w";
        args[2] = "hdfs://qingcheng11:9000/input/mahout/pa3/raw/";
        HdfsDevUtil.deleteFileOnExist(args[2]);
        //##################可被删除的代码###############################

        //1. 检测出肉参数
        if (args.length != 3) {
            System.err.println("Usage: GetProfessionLemma  <path to professions.txt>  <path to wiki-big-lemma-index>  <output-dir> ");
            System.exit(-1);
        }

        //2. 创建job
        final Job job = new Job(new Configuration(), "pa3");
        job.setJarByClass(ParseWikiAndProfession.class);
        //3.设置mapper输出格式
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        //4.设置reduce
        job.setReducerClass(MyReduceProfessionAndWiki.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        //5.设置最终结果输出路径
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, MyMapperProfession.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, MyMapperWiki.class);
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        //6.提交job
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}  