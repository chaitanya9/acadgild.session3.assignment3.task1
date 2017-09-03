import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class rmduplicaterecordmapper extends Mapper<LongWritable, Text, Text, IntWritable>{

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    @Override
    protected void map(LongWritable key, Text value,
                       Context context)
            throws IOException, InterruptedException {
        String[] linesArray = value.toString().split("  ");
        for(String s: linesArray){
            System.out.println(s);}
        for(String line : linesArray){
            String[] lineArray = line.split("\\|");
            for(String s: lineArray){
                System.out.println(s);}
            Text company = new Text(lineArray[0]);
            Text product = new Text(lineArray[1]);
            if(!company.equals(new Text("NA"))){
                if(!product.equals(new Text("NA"))){
            context.write(company,one);
            }}
        }
        }

    }

