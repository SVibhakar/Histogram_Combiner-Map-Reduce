import java.io.*;
import java.util.Scanner;
import java.util.Vector;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import java.io.IOException;
    
class Color implements WritableComparable<Color> {
	public int type;       
	public int intensity;  
	Color() {}
	Color (int type, int value) {
		this.type=type;
		this.intensity=value;
	}
	public String toString() {
		return (this.type + " " + this.intensity);
	}
	public void write (DataOutput output) throws IOException {
		output.writeInt(this.type);
		output.writeInt(this.intensity);
	}
	public void readFields (DataInput input) throws IOException {
		type = input.readInt();
		intensity = input.readInt();
	}
    public boolean equals(Object obj){ 
         if (this == obj) { 
            return true; 
        } 
        if (obj == null) { 
            return false; 
        }  
        Color o = (Color)obj;
        int color1=this.type*1000+this.intensity;
        int color2=o.type*1000+o.intensity;
        if(color1 != color2)
        {return false;}
        return true; 
    } 
	public int compareTo (Color key){
		int color1=this.type*1000+this.intensity;
        int color2=key.type*1000+key.intensity;
        if(color1 < color2)
        {
            return -1;
        }
        else if(color1 == color2)
        {
            return 0;
        }
        else
        {
            return 1;
        }
	}
    public int hashcode(){
        return (this.type*1000+this.intensity);
    }
}

public class Histogram {
	public static class HistogramMapper extends Mapper<Object,Text,Color,IntWritable> 
	{   @Override
		public void map ( Object key, Text value, Context context) throws IOException, InterruptedException {
			int red=0, blue=0, green=0;
			Scanner sc = new Scanner(value.toString()).useDelimiter(",");
			red = sc.nextInt();
            green = sc.nextInt();
            blue = sc.nextInt(); 
            context.write(new Color(1,red), new IntWritable(1));
            context.write(new Color(2,green), new IntWritable(1));
            context.write(new Color(3,blue), new IntWritable(1));
            sc.close();
		}
	}
    public static class InMapperCombiner extends Mapper<Object,Text,Color,IntWritable> {
        public Hashtable<Color, Integer>  m2 ;
        protected void setup (Context context) throws IOException,InterruptedException {
            m2 = new Hashtable<Color, Integer>();
        }
        @Override
        public void map( Object key, Text value, Context context) throws IOException, InterruptedException {
			Scanner sc = new Scanner(value.toString()).useDelimiter(",");
            Color red = new Color(1, sc.nextInt());
            Color green = new Color(2, sc.nextInt());
            Color blue = new Color(3, sc.nextInt());    
            if(m2.containsKey(red))
            
                m2.put(red,m2.get(red)+1);
                
            else
                m2.put(red,1);
            
            if(m2.containsKey(green))

                m2.put(green,m2.get(green)+1);

            else
                m2.put(green,1);

            if(m2.containsKey(blue))

                m2.put(blue,m2.get(blue)+1);

            else
                m2.put(blue,1);

            sc.close();
        }
        protected void cleanup ( Context context ) throws IOException,InterruptedException {
            for (Color key : m2.keySet()) {
                context.write(key,new IntWritable(m2.get(key)));
            }
            m2.clear();
        }
    }
    public static class Combiner extends Reducer<Color,IntWritable,Color,IntWritable> {
        @Override
        public void reduce ( Color key, Iterable<IntWritable> values, Context context ) throws IOException, InterruptedException {
            int sum=0;
            for (IntWritable v: values) {
                sum =sum+v.get(); 
            } 
            context.write(key,new IntWritable(sum));
        }
    }
	public static class HistogramReducer extends Reducer<Color,IntWritable,Color,LongWritable> 
	{   @Override
		public void reduce ( Color key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
		{
			int sum = 0;
			for (IntWritable v : values){
				sum += v.get();
			}
			context.write(key, new LongWritable(sum));
		}
	}
	public static void main ( String[] args ) throws Exception{
		Job job = Job.getInstance();
        Job job1 = Job.getInstance();
		job.setJobName("MyJob");
        job1.setJobName("MyJob1");
        job.setJarByClass(Histogram.class);
        job.setOutputKeyClass(Color.class);
        job.setOutputValueClass(IntWritable.class);
        job.setMapOutputKeyClass(Color.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setMapperClass(HistogramMapper.class);
        job.setReducerClass(HistogramReducer.class); 
        job.setCombinerClass(Combiner.class);                
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));
        job.waitForCompletion(true);

        job1.setJarByClass(Histogram.class);
        job1.setOutputKeyClass(Color.class);		       
        job1.setOutputValueClass(IntWritable.class);
        job1.setMapOutputKeyClass(Color.class);
        job1.setMapOutputValueClass(IntWritable.class);
        job1.setMapperClass(InMapperCombiner.class);
        job1.setReducerClass(HistogramReducer.class);
        job1.setInputFormatClass(TextInputFormat.class);		       
        job1.setOutputFormatClass(TextOutputFormat.class);		       
        FileInputFormat.setInputPaths(job1,new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1]+"2"));
        job1.waitForCompletion(true);
	}
}