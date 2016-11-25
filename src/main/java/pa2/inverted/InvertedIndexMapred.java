package pa2.inverted;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import pa2.lemma.LemmaIndexMapred;
import utils.FileDevUtil;
import pa2.util.StringInteger;
import pa2.util.StringIntegerList;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

public class InvertedIndexMapred {
    public static class InvertedIndexMapper extends Mapper<Text, Text, Text, StringInteger> {
        @Override
        public void map(Text pageTile, Text silistStr, Context context) throws IOException,
                InterruptedException {
            StringIntegerList stringIntegerList = new StringIntegerList();
            stringIntegerList.readFromString(silistStr.toString());
            for (StringInteger si : stringIntegerList.getStringIntegerList()) {
                StringInteger invertWordCount = new StringInteger(pageTile.toString(), si.getWordCount());
                context.write(new Text(si.getWordText()), invertWordCount);
            }
        }
    }

    public static class InvertedIndexReducer extends Reducer<Text, StringInteger, Text, StringIntegerList> {
        @Override
        protected void reduce(Text key, Iterable<StringInteger> values, Context context) throws IOException, InterruptedException {
            List<StringInteger> siList = new ArrayList<>();
            for (StringInteger value : values) {
                siList.add(value);
            }
            StringIntegerList stringIntegerList = new StringIntegerList();
            stringIntegerList.setStringIntegerList(siList);
            context.write(key, stringIntegerList);
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException,
            ClassNotFoundException, URISyntaxException {
        args = new String[2];
        args[0] = "/Users/liguohua/Documents/F/code/idea/git/kn-hadoop-maven/output/pa2/sectionB";
        args[1] = "/Users/liguohua/Documents/F/code/idea/git/kn-hadoop-maven/output/pa2/sectionC";
        FileDevUtil.removeFileName(args[1]);
        //1. check parameters
        if (args.length != 2) {
            System.err.println("Usage: wiki-inverte <input-dir>  <output dir> ");
            System.exit(-1);
        }
        //2.make job
        Job job = Job.getInstance(new Configuration());
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(StringInteger.class);
        //2.1 set input
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        job.setInputFormatClass(KeyValueTextInputFormat.class);
        //2.2 set output
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setOutputFormatClass(TextOutputFormat.class);
        //2.3 set mapper class
        job.setMapperClass(InvertedIndexMapper.class);
        job.setReducerClass(InvertedIndexReducer.class);
        //2.4 set main class
        job.setJarByClass(LemmaIndexMapred.class);
        //3. submit job and wait complete
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}
