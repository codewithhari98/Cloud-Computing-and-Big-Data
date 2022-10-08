import java.io.*;
import java.util.*;
import java.util.Scanner;
import java.lang.String.*;
import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
 
public class Netflix {
    public static class hprakashMapper1 extends Mapper<Object,Text,IntWritable,IntWritable>{
        @Override
        public void map ( Object key, Text line, Context context )
                        throws IOException, InterruptedException {

                Scanner scannedLine = new Scanner(line.toString()).useDelimiter(",");
                if(!((line.toString()).endsWith(":"))){
                    int user = scannedLine.nextInt();
                    int rating = scannedLine.nextInt();
                    context.write(new IntWritable(user),new IntWritable(rating));  
                    }   
                scannedLine.close();
                }                      
    }

    public static class hprakashReducer1 extends Reducer<IntWritable,IntWritable,IntWritable,IntWritable> {
        @Override
        public void reduce ( IntWritable user, Iterable<IntWritable> ratings, Context context )
                           throws IOException, InterruptedException {
            double sum = 0.0;
            long count = 0;
            for (IntWritable r: ratings) {
                sum += r.get();
                count++;
            };
            int avg10x=(int)(sum/count*10);
            context.write(user,new IntWritable(avg10x));
        }
    }
    
    public static class hprakashMapper2 extends Mapper<Object,Text,IntWritable,IntWritable>{
        @Override
        public void map ( Object key, Text value, Context context )
                        throws IOException, InterruptedException {
                Scanner scannedLine = new Scanner(value.toString()).useDelimiter("\t");
                int user = scannedLine.nextInt();
                int rating = scannedLine.nextInt();
                context.write(new IntWritable(rating), new IntWritable(1));
                scannedLine.close();

            }
        }

    public static class hprakashReducer2 extends Reducer<IntWritable,IntWritable,DoubleWritable,IntWritable> {
        @Override
        public void reduce ( IntWritable rating, Iterable<IntWritable> values, Context context )
                           throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable v: values) {
                sum += v.get();
            };
            double ratingdouble=(double)(rating.get());
            context.write(new DoubleWritable(ratingdouble/10),new IntWritable(sum));
        }
    }

    public static void main ( String[] args ) throws Exception {
        Job hprakashjob = Job.getInstance();
        hprakashjob.setJobName("MyJob1");
        hprakashjob.setJarByClass(Netflix.class);
        hprakashjob.setOutputKeyClass(IntWritable.class);
        hprakashjob.setOutputValueClass(IntWritable.class);
        hprakashjob.setMapOutputKeyClass(IntWritable.class);
        hprakashjob.setMapOutputValueClass(IntWritable.class);
        hprakashjob.setMapperClass(hprakashMapper1.class);
        hprakashjob.setReducerClass(hprakashReducer1.class);
        hprakashjob.setInputFormatClass(TextInputFormat.class);
        hprakashjob.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.setInputPaths(hprakashjob,new Path(args[0]));
        FileOutputFormat.setOutputPath(hprakashjob,new Path(args[1]));
        hprakashjob.waitForCompletion(true);

        Job hprakashjob2 = Job.getInstance();
        hprakashjob2.setJobName("MyJob2");
        hprakashjob2.setJarByClass(Netflix.class);
        hprakashjob2.setOutputKeyClass(DoubleWritable.class);
        hprakashjob2.setOutputValueClass(IntWritable.class);
        hprakashjob2.setMapOutputKeyClass(IntWritable.class);
        hprakashjob2.setMapOutputValueClass(IntWritable.class);
        hprakashjob2.setMapperClass(hprakashMapper2.class);
        hprakashjob2.setReducerClass(hprakashReducer2.class);
        hprakashjob2.setInputFormatClass(TextInputFormat.class);
        hprakashjob2.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.setInputPaths(hprakashjob2 ,new Path(args[1]));
        FileOutputFormat.setOutputPath(hprakashjob2 ,new Path(args[2]));
        System.exit(hprakashjob2.waitForCompletion(true) ? 0 : 1);
    }
}