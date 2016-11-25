package pa2.articles;

import edu.umd.cloud9.collection.wikipedia.WikipediaPage;
import edu.umd.cloud9.collection.wikipedia.WikipediaPageInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.log4j.Logger;
import utils.FileDevUtil;
import utils.FileUtil;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.HashSet;
import java.util.Set;

public class GetArticlesMapred {
    private static Logger logger = Logger.getLogger(GetArticlesMapred.class);
    private static String peopleFile;
    public static class GetArticlesMapper extends Mapper<LongWritable, WikipediaPage, Text, Text> {
        public static Set<String> peopleList = new HashSet<>();

        //1. read people.txt into memery
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            peopleList= FileUtil.convertLines2Set(peopleFile);
        }

        //2.filter the articles contains by people list
        @Override
        public void map(LongWritable offset, WikipediaPage inPage, Context cxt)
                throws IOException, InterruptedException {
            String pageTile = inPage.getTitle();
            if (peopleList.contains(pageTile)) {
                Text articleXML = new Text(inPage.getRawXML());
                //emmit to reducer for next process
                cxt.write(new Text(pageTile), articleXML);
            }
        }

    }

    public static class GetArticlesReducer extends Reducer<Text, Text, Text, Text> {
        private MultipleOutputs multipleOutputs;

        //1.set up multipleOutputs for save data into  separate files
        protected void setup(Context cxt) throws IOException, InterruptedException {
            multipleOutputs = new MultipleOutputs(cxt);
        }

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context cxt) throws IOException, InterruptedException {
            for (Text value : values) {
                //2. save data into  separate files
                multipleOutputs.write(NullWritable.get(), value, key.toString());
            }
        }

        //3.set up multipleOutputs
        protected void cleanup(Context cxt) throws IOException, InterruptedException {
            multipleOutputs.close();
        }
    }

    public static void main(String[] args) throws IOException, URISyntaxException,
            InterruptedException, ClassNotFoundException {
        args = new String[3];
        args[0] = "/Users/liguohua/Documents/F/code/idea/git/kn-hadoop-maven/input/pa2/w";
        args[1] = "/Users/liguohua/Documents/F/code/idea/git/kn-hadoop-maven/output/pa2/sectionA";
        args[2] = "/Users/liguohua/Documents/F/code/idea/git/kn-hadoop-maven/input/pa2/p";
        FileDevUtil.removeFileName(args[1]);
        //1. check parameters
        if (args.length != 3) {
            System.err.println("Usage: wiki-getArticle <wiki-file>  <output dir>  <people-file>");
            System.exit(-1);
        }
        peopleFile = args[2];
        //2.make job
        Job job = Job.getInstance(new Configuration());
        //2.1 set input
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        job.setInputFormatClass(WikipediaPageInputFormat.class);
        //2.2 set output
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        //2.3 set mapper class
        job.setMapperClass(GetArticlesMapper.class);
        job.setReducerClass(GetArticlesReducer.class);
        //2.4 set main class
        job.setJarByClass(GetArticlesMapred.class);
        //3. submit job and wait complete
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}