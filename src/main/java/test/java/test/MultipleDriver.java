package test.java.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
/**
 * input1(/multiple/user/user):
 * username,user_phone
 *
 * input2(/multiple/phone/phone):
 *  user_phone,description
 *
 * output: username,user_phone,description
 *
 * @author fansy
 *
 */
public class MultipleDriver extends Configured implements Tool{
//  private  Logger log = LoggerFactory.getLogger(MultipleDriver.class);

    private String input1=null;
    private String input2=null;
    private String output=null;
    private String delimiter=null;

    public static void main(String[] args) throws Exception {
        Configuration conf=new Configuration();
        ToolRunner.run(conf, new MultipleDriver(), args);
    }

    @Override
    public int run(String[] arg0) throws Exception {


        Configuration conf= getConf();
        conf.set("delimiter", delimiter);
        @SuppressWarnings("deprecation")
        Job job = new Job(conf, "merge user and phone information ");
        job.setJarByClass(MultipleDriver.class);

        job.setReducerClass(MultipleReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlagStringDataType.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);


        MultipleInputs.addInputPath(job, new Path(input1), TextInputFormat.class, Multiple1Mapper.class);
        MultipleInputs.addInputPath(job, new Path(input2), TextInputFormat.class, Multiple2Mapper.class);
        FileOutputFormat.setOutputPath(job, new Path(output));

        int res = job.waitForCompletion(true) ? 0 : 1;
        return res;
    }
}