package comp9313.ass4;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class SetSimJoin {
    public static String IN = "input";
    public static String OUT = "output";
    
    public static class ComputeJSMapper extends Mapper<Object, Text, Text, Text> {
        
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] list = value.toString().split(" ");
            Configuration conf = context.getConfiguration();
            double threshhold = conf.getDouble("threshhold", 0.0);
            /*  Record 1  {1,2,4,5,6}
             *  Record 2  {2,3,4,5,6}
             *  min similarity 0.7
             *  So Record1 and Record2 should have Math.ceil(5 * 0.7) = 4 common elements.
             *  In this condition, prefix length should be 2.
             *  Prefix length + total common elements number = length of Record + 1;
             */
            int prefix = (int) (list.length - 1 + 1 -(Math.ceil((list.length - 1) * threshhold)));
            for(int index = 1; index <= prefix; index++){
                context.write(new Text(list[index]),value);
            }
        }
    }
    public static class ComputeJSReducer extends Reducer<Text,Text, IntWritable, Text> {
        
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            ArrayList<String[]> Grouplist = new ArrayList<String[]>();
            for(Text val: values){
                String[] Record  = val.toString().split(" ");
                Grouplist.add(Record);
            }
            if(Grouplist.size() > 0){
                //Two layer for loop to generate combination of RID1 and RID2
                //Using Hashset to store elements of RID1
                for(int i = 0; i < Grouplist.size()-1; i ++){
                    Set<String> SetForRID1 = new HashSet<String>();
                    for(int index_1 = 1; index_1 < Grouplist.get(i).length; index_1++){
                        SetForRID1.add(Grouplist.get(i)[index_1]);
                    }
                    for(int j = i+1; j < Grouplist.size();j++){
                        Set<String> Union = new HashSet<String>();
                        Set<String> Intersection = new HashSet<String>();
                        for(int index_2 = 1; index_2 < Grouplist.get(j).length; index_2++){
                            //Using 2 Hashsets to store elements of RID2
                            //Union will be the union set of RID1 and RID2
                            //Intersection will be the intersection set of RID1 and RID2
                            Union.add(Grouplist.get(j)[index_2]);
                            Intersection.add(Grouplist.get(j)[index_2]);
                        }
                        Union.addAll(SetForRID1);
                        Intersection.retainAll(SetForRID1);
                        Configuration conf = context.getConfiguration();
                        double threshhold = conf.getDouble("threshhold", 0.0);
                        //Jaccard similarity computation.
                        double jd_sim = (double)(Intersection.size()) / (double)(Union.size());
                        if(jd_sim >= threshhold){
                            //Rearange order of RID1 and RID2 as required.
                            if(Integer.parseInt(Grouplist.get(i)[0]) < Integer.parseInt(Grouplist.get(j)[0])){
                                context.write(new IntWritable(Integer.parseInt(Grouplist.get(i)[0])), new Text(Grouplist.get(j)[0] + "\t" + jd_sim));
                                
                            }else{
                                context.write(new IntWritable(Integer.parseInt(Grouplist.get(j)[0])), new Text(Grouplist.get(i)[0] + "\t" + jd_sim));
                                
                            }
                        }
                    }
                }
            }
        }
    }
    public static class RemoveMapper extends Mapper<Object, Text, IntWritable, Text> {
        
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] line = value.toString().split("\t");
            context.write(new IntWritable(Integer.parseInt(line[0])), new Text(line[1] + "\t"+line[2]));
        }
    }
    public static class RemoveReducer extends Reducer<IntWritable,Text, Text, Text> {
        
        @Override
        public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            //HashSet is used for reduce duplicate information.
            Set<String> final_set = new HashSet<String>();
            for(Text val: values){
                if(!final_set.contains(val.toString())){
                }
                final_set.add(val.toString());
            }
            Map<Integer,String> map = new HashMap<Integer,String>();
            for(String s:final_set){
                String[] l = s.split("\t");
                map.put(Integer.parseInt(l[0]), l[1]);
            }
            //TreeMap is used for sorting RID2.
            Map<Integer,String> SortMap = new TreeMap<Integer,String>(map);
            for(Integer i : SortMap.keySet()){
                context.write(new Text("(" + key.toString() + "," + i.toString()+ ")"),new Text(SortMap.get(i)));
            }
        }
    }
    
    
    public static void main(String[] args) throws Exception {
        IN = args[0];
        OUT = args[1];
        
        int numberOfreducer = Integer.parseInt(args[3]);
        int iteration = 0;
        Configuration conf = new Configuration();
        conf.setDouble("threshhold", Double.parseDouble(args[2]));
        Job job1 = Job.getInstance(conf, "SetSimJoin");
        job1.setJarByClass(SetSimJoin.class);
        job1.setMapperClass(ComputeJSMapper.class);
        job1.setReducerClass(ComputeJSReducer.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);
        job1.setOutputKeyClass(IntWritable.class);
        job1.setOutputValueClass(Text.class);
        job1.setNumReduceTasks(numberOfreducer);
        
        String input = IN;
        String output = OUT + iteration;
        
        FileInputFormat.addInputPath(job1, new Path(input));
        FileOutputFormat.setOutputPath(job1, new Path(output));
        job1.waitForCompletion(true);
        
        
        Job job2 = Job.getInstance(conf, "SetSimJoin");
        job2.setJarByClass(SetSimJoin.class);
        job2.setMapperClass(RemoveMapper.class);
        job2.setReducerClass(RemoveReducer.class);
        job2.setMapOutputKeyClass(IntWritable.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        job2.setNumReduceTasks(numberOfreducer);
        
        
        iteration++;
        String input2 = output;
        String output2 = OUT + iteration;
        FileInputFormat.addInputPath(job2, new Path(input2));
        FileOutputFormat.setOutputPath(job2, new Path(output2));
        job2.waitForCompletion(true);
    }
}
