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

    public static class AcledStep1Mapper extends Mapper<LongWritable, Text, Text, LongWritable> {
        private final Text outKey = new Text();
        private final LongWritable outFatalities = new LongWritable();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            if (key.get() == 0) return;

            String line = value.toString();
            String[] row = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");

            try {
                if (row.length > 28) {
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

    public static class AcledStep1Reducer extends Reducer<Text, LongWritable, Text, Text> {
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

    public static class AcledStep2Mapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] parts = line.split("\t");
            if (parts.length == 2) {
                context.write(new Text(parts[0]), new Text(parts[1]));
            }
        }
    }

    public static class Acled2Reducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text val : values) {
                String[] stats = val.toString().split(",");
                if (stats.length == 2) {
                    long eventCount = Long.parseLong(stats[0]);
                    long sumFatalities = Long.parseLong(stats[1]);

                    double intensity = (eventCount > 0) ? (double) sumFatalities / eventCount : 0.0;
                    String result = String.format("%d,%.4f", sumFatalities, intensity);
                    context.write(key, new Text(result));
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: Acled <input> <output>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Path intermediatePath = new Path("temp_intermediate_data");

        // --- STAGE 1 ---
        long start1 = System.currentTimeMillis();
        Job job1 = Job.getInstance(conf, "ACLED STEP 1");
        job1.setJarByClass(Acled.class);

        job1.setMapperClass(AcledStep1Mapper.class);
        job1.setReducerClass(AcledStep1Reducer.class);

        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(LongWritable.class);

        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, intermediatePath);

        boolean success1 = job1.waitForCompletion(true);
        long end1 = System.currentTimeMillis();

        System.out.println("==== STAGE 1 ====");
        System.out.println("Status: " + (success1 ? "SUCCESS" : "FAIL"));
        System.out.println("Czas: " + (end1 - start1) + " ms");

        if (success1) {
            // --- STAGE 2 ---
            long start2 = System.currentTimeMillis();
            Job job2 = Job.getInstance(conf, "ACLED STEP 2");
            job2.setJarByClass(Acled.class);

            job2.setMapperClass(AcledStep2Mapper.class);
            job2.setReducerClass(Acled2Reducer.class);

            job2.setMapOutputKeyClass(Text.class);
            job2.setMapOutputValueClass(Text.class);

            job2.setOutputKeyClass(Text.class);
            job2.setOutputValueClass(Text.class);

            FileInputFormat.addInputPath(job2, intermediatePath);
            FileOutputFormat.setOutputPath(job2, new Path(args[1]));

            boolean success2 = job2.waitForCompletion(true);
            long end2 = System.currentTimeMillis();

            System.out.println("==== STAGE 2 ====");
            System.out.println("Status: " + (success2 ? "SUCCESS" : "FAIL"));
            System.out.println("Czas: " + (end2 - start2) + " ms");

            System.exit(success2 ? 0 : 1);
        } else {
            System.exit(1);
        }
    }
}