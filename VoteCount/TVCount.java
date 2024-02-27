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


public class TVCount {

    public static class TVCountMapper extends Mapper<Object, Text, Text, IntWritable>{

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            ///Mapper Code here
            String line = value.toString();
            String regex = ",(?=([^\"]*\"[^\"]*\")*[^\"]*$)";
            HashMap<String, String> countryCodes = new HashMap<>();
            countryCodes.put("JP", "Japan");
            countryCodes.put("US", "United States");
            countryCodes.put("FR", "France");
            String[] columns = line.split(regex);
            String country = columns[1];
            if(countryCodes.containsKey(country)) {
                String countryName = countryCodes.get(country);
                String show = columns[3];
                int votes = Integer.parseInt(columns[columns.length - 1]); // index from the end as sometimes movie names contain ','
                context.write(new Text(countryName + " - " + show), new IntWritable(votes));
            } 
            

        }
    }
             


    public static class TVReducer extends Reducer<Text,IntWritable,Text,IntWritable> {

        public void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {
            ///Reducer Code here
            for (IntWritable value : values) {
                context.write(key, value);
            }
           
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "tv count");
        job.setJarByClass(TVCount.class);
        job.setMapperClass(TVCountMapper.class);
        job.setReducerClass(TVReducer.class);
        job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setNumReduceTasks(1);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
