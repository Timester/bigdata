package net.talqum.hadoop.pageviewstats;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * HTML lapletöltések hisztogramja
 * Created by Imre on 2014.11.12..
 *
 * meg kell nézni minden hostra hogy hány lapot nézett
 * mapper -> host,1
 * combiner -> host,x
 * reducer -> host, letöltésszám
 * az azonos számúakat nézőket megszámolni
 */

public class SiteViewDriver{

    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Stage1");
        job.setJarByClass(SiteViewDriver.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(SiteViewMapper.class);
        job.setCombinerClass(SiteViewReducer.class);
        job.setReducerClass(SiteViewReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}