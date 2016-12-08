/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package hadoop;
import drivers.CalculatePageRank;
import drivers.FinishPageRank;
import drivers.InitPageRank;
import org.apache.commons.lang.time.StopWatch;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;
/**
 *
 * @author FiqieUlya
 */
public class Main {
    static final int NUMBER_OF_ITERATION = 3;
            
	public static void main(String[] args) throws Exception {

		StopWatch timer = new StopWatch();
		timer.start();

                System.out.println("Main program is starting");
                
		String dir = "hdfs://127.0.1.1:9000/user/fiqie/twitter/";

		Configuration conf = new Configuration();
                conf.set("fs.default.name", "hdfs://127.0.1.1:9000");
                FileSystem fs = FileSystem.get(conf);
                
		String[] prepareOpts = { dir + "input/small.txt", dir + "output/pr-0.out" };
		ToolRunner.run(new Configuration(), new InitPageRank(), prepareOpts);

		//String[] initOpts = { dir + "output/prepared.out", dir + "output/pr-0.out" };
		//ToolRunner.run(new Configuration(), new InitPageRankDriver(), initOpts);
				
		for (int i = 1; i <= NUMBER_OF_ITERATION; i++) {
		 	String previous = dir + "output/pr-" + (i - 1) + ".out";
		 	String current = dir + "output/pr-" + i + ".out";
		 	String[] opts = {previous, current};
		 	ToolRunner.run(new Configuration(), new CalculatePageRank(), opts);
                        
                        if (i == NUMBER_OF_ITERATION) {
                            String[] finalOpts = { dir + "output/pr-" + i + ".out", dir + "output/pr-final.out" };
                            ToolRunner.run(new Configuration(), new FinishPageRank(), finalOpts);
                        }
		}
		                
		timer.stop();
		System.out.println("Elapsed " + timer.toString());

	}
}
