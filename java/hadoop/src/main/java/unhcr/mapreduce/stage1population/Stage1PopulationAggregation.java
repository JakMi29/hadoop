package unhcr.mapreduce.stage1population;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Stage1PopulationAggregation {

    public static void main(String[] args) throws Exception {

        long start = System.currentTimeMillis();

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Stage1 Population Aggregation");

        job.setJarByClass(Stage1PopulationAggregation.class);

        job.setMapperClass(Stage1PopulationMapper.class);
        job.setReducerClass(Stage1PopulationReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

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