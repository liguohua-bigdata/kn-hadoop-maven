package test.java.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import utils.Constaints;
import utils.FileDevUtil;
import utils.FileUtil;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

public class GetProfessionLemma {
    private static String professionsFile;

    public static class GetProfessionLemmaMapper
            extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public static HashMap<String, List<String>> professionsMap = new HashMap<>();

        //1. read profession.txt into memery
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Set<String> professionsList = FileUtil.convertLines2Set(professionsFile);
            for (String line : professionsList) {
                String[] pair = line.split(Constaints.COLON);
                if (pair.length == 2) {
                    // string:name list:professions
                    professionsMap.put(pair[0].trim(), Arrays.asList(pair[1].trim().split(Constaints.COMMA)));
                }
            }
        }

        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
//            String line = value.toString();
//            String[] lineSplits = line.split(Constaints.BLANK);
//
//            word.set(lineSplits[0].trim());
            context.write(key, new IntWritable(2));
//            if (!professionsMap.containsKey(lineSplits[0].trim())){
//                return;
//            }


//            String [] words2=lineSplits[1].split(Constaints.COMMA);
//            for (String w : words2) {
//                word.set(lineSplits[0].trim());
//                context.write(word, one);
//
//            }
        }
    }

    public static class GetProfessionLemmaReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }


    public static void main(String[] args) throws IOException, URISyntaxException,
            InterruptedException, ClassNotFoundException {
        args = new String[3];
        args[0] = "/Users/liguohua/Documents/F/code/idea/git/kn-hadoop-maven/input/pa3/w";
        args[1] = "/Users/liguohua/Documents/F/code/idea/git/kn-hadoop-maven/output/pa3/GetProfessionLemma";
        args[2] = "/Users/liguohua/Documents/F/code/idea/git/kn-hadoop-maven/input/pa3/p";
        FileDevUtil.removeFileName(args[1]);
        //1. check parameters
        if (args.length != 3) {
            System.err.println("Usage: GetProfessionLemma  <path to wiki-big-lemma-index>  <output-dir>  <path to professions.txt>");
            System.exit(-1);
        }
        professionsFile = args[2];

        //2.make job
        Job job = Job.getInstance(new Configuration());
        //2.0 set main class
        job.setJarByClass(GetProfessionLemma.class);
        //2.1 set input
        FileInputFormat.addInputPath(job, new Path(args[0]));
        job.setInputFormatClass(KeyValueTextInputFormat.class);

        //2.2 set mapper
        job.setMapperClass(GetProfessionLemmaMapper.class);
        //2.3 set reducer
        job.setReducerClass(GetProfessionLemmaReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        //2.4 set output
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        //3. submit job and wait complete
        System.exit(job.waitForCompletion(true) ? 0 : 1);


    }
}