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
public class CalculatePageRank1Mapper extends MapReduceBase implements Mapper<Text, Text, Text, Text>{

    @Override
    public void map(Text key, Text value, OutputCollector<Text, Text> oc, Reporter rprtr) throws IOException {
        //input: f1[ ]u1,u2,u3,3,PR
        String [] temp = value.toString().split(" ");
        String f = temp[0]; 
        if(temp.length > 1){
            String [] val = temp[1].split(",");
            //val: u1
            for(int i = 0; i < val.length - 2; i++){
                oc.collect(new Text(val[i]), new Text(Double.toString(0.85 *Double.parseDouble(val[val.length-1])/Double.parseDouble(val[val.length-2]))));
            }
            //key:u1   value:0.85*PR/3 
            oc.collect(new Text(f),new Text("user,"+temp[1]));
            //key:f1   value:user,u1,u2,u3,3,PR
        }
    }
    
}
