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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class Acled {

    public static class Step1Mapper extends Mapper<LongWritable, Text, Text, LongWritable> {
        private final Text outKey = new Text();
        private final LongWritable outFatalities = new LongWritable();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            if (key.get() == 0) return;
            String[] row = value.toString().split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");
            try {
                if (row.length > 28) {
                    String year = row[1].replace("\"", "").split("-")[0].trim();
                    String iso3 = row[15].replace("\"", "").trim();
                    long fatalities = Long.parseLong(row[28].replace("\"", "").trim());
                    outKey.set(iso3 + "," + year);
                    outFatalities.set(fatalities);
                    context.write(outKey, outFatalities);
                }
            } catch (Exception e) {}
        }
    }

    public static class Step1Reducer extends Reducer<Text, LongWritable, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long sum = 0;
            long count = 0;
            for (LongWritable val : values) {
                sum += val.get();
                count++;
            }
            context.write(key, new Text(count + "," + sum));
        }
    }

    public static class Step2Mapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split("\t");
            if (parts.length == 2) {
                context.write(new Text(parts[0]), new Text(parts[1]));
            }
        }
    }

    public static class Step2Reducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text val : values) {
                String[] stats = val.toString().split(",");
                long count = Long.parseLong(stats[0]);
                long sum = Long.parseLong(stats[1]);
                double intensity = (count > 0) ? (double) sum / count : 0.0;
                context.write(key, new Text(sum + "," + intensity));
            }
        }
    }

    public static class Step3Mapper extends Mapper<LongWritable, Text, Text, Text> {
        private final Text globalKey = new Text("GLOBAL");

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split("\t");
            if (parts.length == 2) {
                context.write(globalKey, new Text(parts[0] + "|" + parts[1]));
            }
        }
    }

    public static class Step3Reducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            List<Double> intensityList = new ArrayList<>();
            List<String> rawRecords = new ArrayList<>();

            for (Text val : values) {
                String valStr = val.toString();
                rawRecords.add(valStr);
                try {
                    double intensity = Double.parseDouble(valStr.split("\\|")[1].split(",")[1]);
                    intensityList.add(intensity);
                } catch (Exception e) {}
            }

            Collections.sort(intensityList);
            if (intensityList.isEmpty()) return;

            // Progi kwantyli
            int n = intensityList.size();
            double q1 = intensityList.get((int) (n * 0.25));
            double q2 = intensityList.get((int) (n * 0.50));
            double q3 = intensityList.get((int) (n * 0.75));

            for (String record : rawRecords) {
                String[] parts = record.split("\\|");
                String countryYear = parts[0];
                String[] stats = parts[1].split(",");

                long sumFatalities = Long.parseLong(stats[0]);
                double currentInt = Double.parseDouble(stats[1]);

                String quantile = (currentInt <= q1) ? "LOW" :
                    (currentInt <= q2) ? "MID_LOW" :
                        (currentInt <= q3) ? "MID_HIGH" : "HIGH";

                context.write(new Text(countryYear), new Text(quantile + "," + sumFatalities));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Path p1 = new Path("temp_1");
        Path p2 = new Path("temp_2");

        // JOB 1
        long startTimeJ1 = System.currentTimeMillis();
        Job j1 = Job.getInstance(conf, "ACLED 1: Aggr");
        j1.setJarByClass(Acled.class);
        j1.setMapperClass(Step1Mapper.class);
        j1.setReducerClass(Step1Reducer.class);
        j1.setMapOutputValueClass(LongWritable.class);
        j1.setOutputKeyClass(Text.class);
        j1.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(j1, new Path(args[0]));
        FileOutputFormat.setOutputPath(j1, p1);

        if (j1.waitForCompletion(true)) {
            long endTimeJ1 = System.currentTimeMillis();
            System.out.println(">>> JOB 1 zakończony w: " + (endTimeJ1 - startTimeJ1) / 1000.0 + "s");

        // JOB 2
        long startTimeJ2 = System.currentTimeMillis();
        Job j2 = Job.getInstance(conf, "ACLED 2: Intensity");
        j2.setJarByClass(Acled.class);
        j2.setMapperClass(Step2Mapper.class);
        j2.setReducerClass(Step2Reducer.class);
        j2.setOutputKeyClass(Text.class);
        j2.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(j2, p1);
        FileOutputFormat.setOutputPath(j2, p2);

        if (j2.waitForCompletion(true)) {
            long endTimeJ2 = System.currentTimeMillis();
            System.out.println(">>> JOB 2 zakończony w: " + (endTimeJ2 - startTimeJ2) / 1000.0 + "s");

            // JOB 3
            long startTimeJ3 = System.currentTimeMillis();
            Job j3 = Job.getInstance(conf, "ACLED 3: Quantiles");
            j3.setJarByClass(Acled.class);
            j3.setMapperClass(Step3Mapper.class);
            j3.setReducerClass(Step3Reducer.class);
            j3.setNumReduceTasks(1);
            j3.setOutputKeyClass(Text.class);
            j3.setOutputValueClass(Text.class);
            FileInputFormat.addInputPath(j3, p2);
            FileOutputFormat.setOutputPath(j3, new Path(args[1]));

            boolean success = j3.waitForCompletion(true);
            long endTimeJ3 = System.currentTimeMillis();

            if (success) {
                System.out.println(">>> JOB 3 zakończony w: " + (endTimeJ3 - startTimeJ3) / 1000.0 + "s");
                System.out.println(">>> CAŁKOWITY CZAS: " + (endTimeJ3 - startTimeJ1) / 1000.0 + "s");
            }

            System.exit(success ? 0 : 1);
            }
        }
    }
}