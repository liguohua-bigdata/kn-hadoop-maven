package pa1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class PA1 {
    private static String peopleFile;
    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: wiki-peopleName-count <wiki-file>  <output dir>  <people-file>");
            System.exit(2);
        }
        peopleFile = args[2];
        //1.Configuration
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "pa1 count people name");
        //2.job input and output
        job.setJarByClass(PA1.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        //3.job mapper and reducer
        job.setMapperClass(PA1.Pa1Mapper.class);
        job.setReducerClass(PA1.IntSumReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
        //4.submit the job
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static class Pa1Mapper extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        public static Set<String> peopleList = new HashSet<String>();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            File file = new File(peopleFile);
            BufferedReader br = new BufferedReader(new FileReader(file));
            String line;
            // read people.txt into memory
            while ((line = br.readLine()) != null) {
                peopleList.add(line);
            }
            br.close();
        }
        private  boolean isNamedChar(Character c){
            return  'A'<c&&c<'z';
        }
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            Text name0 = new Text();
            Set<String> peopleList = Pa1Mapper.peopleList;
            for (String nameInList : peopleList) {
                String line = value.toString();
                for (int index=-1;(index=(line.indexOf(nameInList)))>=0;) {
                    //prefix is ok?
                    boolean prefix=true;
                    int startIndex=index;
                    if(startIndex>0){
                        if (isNamedChar(line.charAt(startIndex-1))){
                            prefix=false;
                        }
                    }
                    //suffix is ok?
                    boolean suffix=true;
                    int endIndex=index+ nameInList.length();
                    if (endIndex<line.length()-1){
                        if (isNamedChar(line.charAt(endIndex+1))){
                           suffix=false;
                        }
                    }
                    //prefix and suffix both ok ,count the name.
                    if (prefix&&suffix){
                        //find one to count
                        name0.set(nameInList);
                        context.write(name0, one);
                    }
                    //substring for next loop
                    line = line.substring(endIndex);
                }
            }
        }

    }

    public static class IntSumReducer extends Reducer<Text, IntWritable,IntWritable,Text> {
        private IntWritable sum0 = new IntWritable();
        public void reduce(Text nameInList, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            sum0.set(sum);
            context.write(sum0, nameInList);
        }
    }
}
