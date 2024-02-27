import java.io.IOException;
import javax.naming.Context;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MaxDeathMapper
extends Mapper < LongWritable, Text, Text, IntWritable > {

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException,
    InterruptedException {
        String line = value.toString();
        String year = line.split(",")[0].split("-")[0].replace("\"", ""); // use the same data processing logic from vanilla version
        int deaths = Integer.parseInt(line.split(",")[8].replace("\"", ""));
        context.write(new Text(year), new IntWritable(deaths));
    }
}