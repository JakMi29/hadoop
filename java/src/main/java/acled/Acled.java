package acled;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.time.LocalDate;

public class Acled {

    public static class AcledMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
        private final Text outKey = new Text();
        private final LongWritable outFatalities = new LongWritable();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            if (key.get() == 0) return;

            String line = value.toString();
            String[] row = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");

            try {
                if (row.length > 30) {
                    String eventDateStr = row[1].replace("\"", "").trim();
                    int year = LocalDate.parse(eventDateStr).getYear();

                    String iso3 = row[15].replace("\"", "").trim();

                    long fatalities = Long.parseLong(row[28].replace("\"", "").trim());

                    outKey.set(iso3 + "," + year);
                    outFatalities.set(fatalities);

                    context.write(outKey, outFatalities);
                }
            } catch (Exception e) {
            }
        }
    }

    public static class AcledReducer extends Reducer<Text, LongWritable, Text, Text> {

        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long sumFatalities = 0;
            long eventCount = 0;

            for (LongWritable val : values) {
                sumFatalities += val.get();
                eventCount++;
            }

            String result = String.format("%d,%d", eventCount, sumFatalities);
            context.write(key, new Text(result));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "ACLED Yearly Country Summary");

        job.setJarByClass(Acled.class);
        job.setMapperClass(AcledMapper.class);
        job.setReducerClass(AcledReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}