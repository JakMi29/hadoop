package acled;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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

    public static class AcledMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final Text outKey = new Text();
        private final IntWritable outFatalities = new IntWritable();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            if (key.get() == 0) return;

            String line = value.toString();
            String[] row = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");

            try {
                if (row.length > 30) {
                    String eventDate = row[1];
                    String iso3 = row[15];
                    int fatalities = Integer.parseInt(row[30]);

                    outKey.set(iso3 + "," + eventDate);
                    outFatalities.set(fatalities);

                    context.write(outKey, outFatalities);
                }
            } catch (Exception e) {
            }
        }
    }

    public static class AcledReducer extends Reducer<Text, IntWritable, Text, Text> {

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sumFatalities = 0;
            long eventCount = 0;

            for (IntWritable val : values) {
                sumFatalities += val.get();
                eventCount++;
            }

            String result = String.format("%d,%d", eventCount, sumFatalities);

            context.write(key, new Text(result));
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: Acled <input path> <output path>");
            System.exit(-1);
        }

        long start = System.currentTimeMillis();
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "=== ACLED ===");

        job.setJarByClass(Acled.class);

        job.setMapperClass(AcledMapper.class);
        job.setReducerClass(AcledReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean success = job.waitForCompletion(true);
        long end = System.currentTimeMillis();

        System.out.println("==== STAGE 1 ====");
        System.out.println("Status: " + (success ? "SUCCESS" : "FAIL"));
        System.out.println("Czas: " + (end - start) + " ms");

        System.exit(success ? 0 : 1);
    }
}