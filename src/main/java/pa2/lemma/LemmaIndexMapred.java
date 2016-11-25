package pa2.lemma;

import edu.umd.cloud9.collection.wikipedia.WikipediaPage;
import edu.umd.cloud9.collection.wikipedia.WikipediaPageInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import utils.FileDevUtil;
import pa2.util.StringInteger;
import pa2.util.StringIntegerList;
import pa2.util.XmlUtil;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class LemmaIndexMapred {
    private static String stopWordPath;
    private static String convertFormatPath;

    public static class LemmaIndexMapper extends
            Mapper<LongWritable, WikipediaPage, Text, StringIntegerList> {
        private Tokenizer tokenizer = new Tokenizer(stopWordPath,convertFormatPath);

        @Override
        public void map(LongWritable offset, WikipediaPage inPage, Context cxt)
                throws IOException, InterruptedException {
            // Wikipedia xml text content
            String text = XmlUtil.getConentByTagNameText(inPage.getRawXML());
            // token
            List<String> textTokes = tokenizer.tokenize(text);
            // count
            Map<String, Integer> lemmaCounts = sumTokens(textTokes);
            // right format
            StringIntegerList stringIntegerList = new StringIntegerList();
            for (String k : lemmaCounts.keySet()) {
                stringIntegerList.push(new StringInteger(k, lemmaCounts.get(k)));
            }
            // write out the result data
            cxt.write(new Text(inPage.getTitle()), stringIntegerList);

        }

        private static Map<String, Integer> sumTokens(List<String> tokens) {
            Map<String, Integer> map = new HashMap<String, Integer>();
            for (String token : tokens)
                if (!map.containsKey(token)) {
                    //init: count=1
                    map.put(token, 1);
                } else {
                    //other: count=preCount+1
                    map.put(token, map.get(token) + 1);
                }
            return map;
        }

    }

    public static void main(String[] args) throws IOException, InterruptedException,
            ClassNotFoundException, URISyntaxException {
        args = new String[3];
        args[0] = "/Users/liguohua/Documents/F/code/idea/git/kn-hadoop-maven/output/pa2/sectionA";
        args[1] = "/Users/liguohua/Documents/F/code/idea/git/kn-hadoop-maven/output/pa2/sectionB";
        args[2] = "/Users/liguohua/Documents/F/code/idea/git/kn-hadoop-maven/src/main/java/pa2/lemma/stopword";
        args[3] = "/Users/liguohua/Documents/F/code/idea/git/kn-hadoop-maven/src/main/java/pa2/lemma/convertFormate";
        FileDevUtil.removeFileName(args[1]);
        //1. check parameters
        if (args.length != 4) {
            System.err.println("Usage: wiki-lemma <input-dir>  <output dir>  <stopWords-file>  <convertFormate-file>");
            System.exit(-1);
        }
        stopWordPath = args[2];
        convertFormatPath = args[3];
        //2.make job
        Job job = Job.getInstance(new Configuration());
        //2.1 set input
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        job.setInputFormatClass(WikipediaPageInputFormat.class);
        //2.2 set output
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        //2.3 set mapper class
        job.setMapperClass(LemmaIndexMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(StringIntegerList.class);
        //2.4 set main class
        job.setJarByClass(LemmaIndexMapred.class);
        //3. submit job and wait complete
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}
