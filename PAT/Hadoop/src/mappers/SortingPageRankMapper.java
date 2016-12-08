/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package mappers;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

/**
 *
 * @author FiqieUlya
 */
public class SortingPageRankMapper extends MapReduceBase implements Mapper<Text, Text, Text, Text>{
    //input: fi[ ]u1,u2,u3,3,PR 
    @Override
    public void map(Text key, Text value, OutputCollector<Text, Text> oc, Reporter rprtr) throws IOException {
        String [] temp = value.toString().split(" ");
        String u = temp[0]; 
        if(temp.length > 1){
            String [] val = temp[1].split(",");
            oc.collect(new Text(val[val.length-1]), new Text(u));
        }
    }
    
}
