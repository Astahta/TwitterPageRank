/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package mappers;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
/**
 *
 * @author FiqieUlya
 */
public class InitPageRankMapper extends MapReduceBase implements Mapper<Text, Text, Text, Text>{

    @Override
    public void map(Text key, Text value, OutputCollector<Text, Text> oc, Reporter rprtr) throws IOException {
      	//value: user\tfollower\n
        oc.collect(value, key);
        //key: f1, value: u1
    }
    
}
