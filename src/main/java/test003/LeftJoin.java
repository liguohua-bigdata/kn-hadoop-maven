package test003;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.Vector;

public class LeftJoin extends Configured implements Tool {
    public static final String DELIMITER = ",";
    private static final String employeePrefix = "employee";
    private static final String salaryPrefix = "salary";

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        configuration.set("input_dir", "/Users/liguohua/Documents/F/code/idea/git/kn-hadoop-maven/input/test003/");
        configuration.set("output_dir", "/Users/liguohua/Documents/F/code/idea/git/kn-hadoop-maven/output/test003/");
        int res = ToolRunner.run(configuration, new LeftJoin(), args);
        System.exit(res);
    }

    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        GenericOptionsParser optionparser = new GenericOptionsParser(conf, args);
        conf = optionparser.getConfiguration();

        Job job = new Job(conf, "leftjoin");
        job.setJarByClass(LeftJoin.class);
        FileInputFormat.addInputPaths(job, conf.get("input_dir"));
        Path out = new Path(conf.get("output_dir"));
        FileOutputFormat.setOutputPath(job, out);
        job.setNumReduceTasks(conf.getInt("reduce_num", 2));

        job.setMapperClass(LeftJoinMapper.class);
        job.setReducerClass(LeftJoinReduce.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        conf.set("mapred.textoutputformat.separator", ",");
        return (job.waitForCompletion(true) ? 0 : 1);
    }

    public static class LeftJoinMapper extends
            Mapper<LongWritable, Text, Text, Text> {

        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            /*
             * 拿到两个不同文件，区分出到底是哪个文件，然后分别输出
             */
            String filepath = ((FileSplit) context.getInputSplit()).getPath().toString();
            String line = value.toString();
            if (line == null || line.equals("")) return;

            if (filepath.indexOf(employeePrefix) != -1) {
                String[] lines = line.split(DELIMITER);
                if (lines.length < 2) return;

                String company_id = lines[0];//outkey
                String employee = lines[1];//outvalue
                context.write(new Text(company_id), new Text(employeePrefix + employee));
            } else if (filepath.indexOf(salaryPrefix) != -1) {
                String[] lines = line.split(DELIMITER);
                if (lines.length < 2) return;

                String company_id = lines[0];//outkey
                String salary = lines[1];//outvalue
                context.write(new Text(company_id), new Text(salaryPrefix + salary));
            }
        }
    }

    public static class LeftJoinReduce extends
            Reducer<Text, Text, Text, Text> {
        protected void reduce(Text key, Iterable<Text> values,
                              Context context) throws IOException, InterruptedException {
            Vector<String> employeeV = new Vector<String>();
            Vector<String> salaryV = new Vector<String>();
            for (Text each_val : values) {
                String each = each_val.toString();
                if (each.startsWith(employeePrefix)) {
                    employeeV.add(each.substring(employeePrefix.length()));
                } else if (each.startsWith(salaryPrefix)) {
                    salaryV.add(each.substring(salaryPrefix.length()));
                }
            }

            for (int i = 0; i < employeeV.size(); i++) {
                /*
                 * 如果salaryV为空的话，将A里的输出，salary的位置补null。
                 */
                if (salaryV.size() == 0) {
                    context.write(key, new Text(employeeV.get(i) + DELIMITER + "null"));
                } else {
                    for (int j = 0; j < salaryV.size(); j++) {
                        context.write(key, new Text(employeeV.get(i) + DELIMITER + salaryV.get(j)));
                    }
                }
            }
        }
    }

}