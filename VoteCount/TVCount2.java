import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import javax.naming.Context;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class TVCount2 {

    public static class TVCountMapper2 extends Mapper<Object, Text, Text, IntWritable>{

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            ///Mapper Code here
            String placeholder = "total lines: ";
            context.write(new Text(placeholder), new IntWritable(1));
        }
    }
             


    public static class TVReducer2 extends Reducer<Text,IntWritable,Text,IntWritable> {

        public void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {
            ///Reducer Code here
            int total_lines = 0;
            for (IntWritable value : values) {
                total_lines += value.get();
            }
            context.write(key, new IntWritable(total_lines));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "tv count");
        job.setJarByClass(TVCount2.class);
        job.setMapperClass(TVCountMapper2.class);
        job.setReducerClass(TVReducer2.class);
        job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setNumReduceTasks(1);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
