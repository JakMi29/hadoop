package unhcr.mapreduce;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import unhcr.mapreduce.stage1population.Stage1PopulationMapper;
import unhcr.mapreduce.stage1population.Stage1PopulationReducer;

import unhcr.mapreduce.stage1demographics.Stage1DemographicsMapper;
import unhcr.mapreduce.stage1demographics.Stage1DemographicsReducer;

import unhcr.mapreduce.stage2.Stage2Mapper;
import unhcr.mapreduce.stage2.Stage2Reducer;
public class Main {
    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();

        FileSystem fs = FileSystem.get(conf);

        // cleanup outputów
        fs.delete(new Path("/output/unhcr/stage1population"), true);
        fs.delete(new Path("/output/unhcr/stage1demographics"), true);
        fs.delete(new Path("/output/unhcr/stage2"), true);


        // ===== JOB 1 =====
        Job job1 = Job.getInstance(conf, "Stage1 Population");
        job1.setJarByClass(Main.class);

        job1.setMapperClass(Stage1PopulationMapper.class);
        job1.setReducerClass(Stage1PopulationReducer.class);

        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);

        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job1, new Path("/acled/unhcr_population.csv"));
        FileOutputFormat.setOutputPath(job1, new Path("/output/unhcr/stage1population"));


        // ===== JOB 2 =====
        Job job2 = Job.getInstance(conf, "Stage1 Demographics");
        job2.setJarByClass(Main.class);

        job2.setMapperClass(Stage1DemographicsMapper.class);
        job2.setReducerClass(Stage1DemographicsReducer.class);

        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);

        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job2, new Path("/acled/unhcr_demographics.csv"));
        FileOutputFormat.setOutputPath(job2, new Path("/output/unhcr/stage1demographics"));


        // ===== START równoległy =====
        long start1 = System.currentTimeMillis();
        long start2 = System.currentTimeMillis();

        job1.submit();
        job2.submit();

        // ===== Czekamy na Stage1 =====
        boolean success1 = job1.waitForCompletion(true);
        long end1 = System.currentTimeMillis();

        boolean success2 = job2.waitForCompletion(true);
        long end2 = System.currentTimeMillis();


        // ===== LOGI STAGE1 =====
        System.out.println("==== STAGE 1 POPULATION ====");
        System.out.println("Status: " + (success1 ? "SUCCESS" : "FAIL"));
        System.out.println("Czas: " + (end1 - start1) + " ms");

        System.out.println("==== STAGE 1 DEMOGRAPHICS ====");
        System.out.println("Status: " + (success2 ? "SUCCESS" : "FAIL"));
        System.out.println("Czas: " + (end2 - start2) + " ms");


        // jeśli któryś fail → kończymy
        if (!success1 || !success2) {
            System.exit(1);
        }


        // ===== STAGE 2 (JOIN) =====
        long start3 = System.currentTimeMillis();

        Job job3 = Job.getInstance(conf, "Stage2 Join");
        job3.setJarByClass(Main.class);

        job3.setMapperClass(Stage2Mapper.class);
        job3.setReducerClass(Stage2Reducer.class);

        job3.setMapOutputKeyClass(Text.class);
        job3.setMapOutputValueClass(Text.class);

        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job3, new Path("/output/unhcr/stage1population"));
        FileInputFormat.addInputPath(job3, new Path("/output/unhcr/stage1demographics"));

        FileOutputFormat.setOutputPath(job3, new Path("/output/unhcr/stage2"));

        boolean success3 = job3.waitForCompletion(true);

        long end3 = System.currentTimeMillis();


        // ===== LOGI STAGE2 =====
        System.out.println("==== STAGE 2 JOIN ====");
        System.out.println("Status: " + (success3 ? "SUCCESS" : "FAIL"));
        System.out.println("Czas: " + (end3 - start3) + " ms");


        // ===== EXIT =====
        System.exit(success3 ? 0 : 1);
    }
}