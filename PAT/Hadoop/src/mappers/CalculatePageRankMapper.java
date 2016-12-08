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
public class CalculatePageRankMapper extends MapReduceBase implements Mapper<Text, Text, Text, Text>{

    @Override
    public void map(Text key, Text value, OutputCollector<Text, Text> oc, Reporter rprtr) throws IOException {
        //output: u1[ ]PR,f1,3,f2,2
        String [] temp = value.toString().split(" ");
        String u = temp[0]; 
        if(temp.length > 1){
            String [] val = temp[1].split(",");
            //val: PR,f1,3,f2,2
            for(int i = 1; i < val.length - 1; i+=2){
                oc.collect(new Text(val[i]), new Text(u+","+val[i]+","+
                        val[i+1]+","+(Double.parseDouble(val[0]))/Double.parseDouble(val[i+1])));
            }
            //key:f1   value:u1,f1,3,PR/3
        }
    }
    
}
