package unhcr.mapreduce.stage2;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
public class Stage2Mapper extends Mapper<LongWritable, Text, Text, Text> {

    private String source;

    @Override
    protected void setup(Context context) {
        String path = ((FileSplit) context.getInputSplit()).getPath().toString();

        if (path.contains("stage1population")) {
            source = "P";
        } else {
            source = "D";
        }
    }

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString();

        if (line.startsWith("year")) return; // skip header

        String[] cols = line.split(",");

        String year = cols[0];
        String coo_id = cols[1];

        String joinKey = year + "_" + coo_id;

        context.write(new Text(joinKey), new Text(source + "|" + line));
    }
}