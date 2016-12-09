/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package drivers;

import mappers.CalculatePageRank1Mapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.KeyValueTextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import reducers.CalculatePageRank1Reducer;

/**
 *
 * @author FiqieUlya
 */
public class CalculatePageRank extends Configured implements Tool{
    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
	JobConf job = new JobConf(conf, this.getClass());
	Path in = new Path(args[0]);
	Path out = new Path(args[1]);
	FileInputFormat.setInputPaths(job, in);
	FileOutputFormat.setOutputPath(job, out);
	job.setJobName("Fiqie|Calculate");
	job.setMapperClass(CalculatePageRank1Mapper.class);
	job.setReducerClass(CalculatePageRank1Reducer.class);
	job.setInputFormat(KeyValueTextInputFormat.class);
	job.setOutputFormat(TextOutputFormat.class);
	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(Text.class);
	JobClient.runJob(job);
	return 0;
    }
}
