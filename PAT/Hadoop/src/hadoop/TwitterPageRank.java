/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package hadoop;

import org.apache.hadoop.fs.Path;
import mappers.InitPageRankMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import static org.apache.hadoop.mapred.JobHistory.RecordTypes.Job;
import reducers.InitPageRankReducer;

/**
 *
 * @author FiqieUlya
 */
public class TwitterPageRank {

    private static Configuration conf = new Configuration();
    
    public static String getRootDirectory() throws Exception {
        FileSystem dfs = FileSystem.get(conf);
        Path now = dfs.getHomeDirectory();
        return (now.getParent().getParent()).toString();
    }
    
    public static String getMyDirectory() throws Exception {
        FileSystem dfs = FileSystem.get(conf);
        Path now = dfs.getHomeDirectory();
        now = new Path(now.toString()).getParent();
        return (new Path(now.toString() + "/fiqie")).toString();
    }
    
    public static void cleanUp() throws Exception {
        String basePath = getMyDirectory();
        FileSystem dfs = FileSystem.get(conf);

        for (int i = 0; i <= 3; i++) {
            dfs.mkdirs(new Path(basePath + "/iteration" + i));
            dfs.delete(new Path(basePath + "/iteration" + i + "/output"), true);
        }
        dfs.delete(new Path(basePath + "/result"), true);
    }
    
    public static void init() throws Exception {
        /*JobConf job = new JobConf(conf, this.getClass());
        job.setJobName("Fiqie|init");
        job.setMapperClass(InitPageRankMapper.class);
        job.setReducerClass(InitPageRankReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        String inputPath = getRootDirectory() + "user/fiqie";
        String outputPath = getMyDirectory() + "/iteration0/output";
        System.out.println("twitter path: " + inputPath);
        System.out.println("output path: " + outputPath);
        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        JobClient.runJob(job);*/
  }
    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        // TODO code application logic here
    }
    
}
