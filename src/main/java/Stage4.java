import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;

public class Stage4 {
    private static int n;
    private static int k;

    public static class simplificationMapper extends Mapper<Object, Text, Text, Text> {
//        input format: a   xa, ya, cellId, [b:dis_b, c:dis_c]
//        output format: a  [b:dis_b, c:dis_c]
        List nodeList = new ArrayList<>();
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] mapSplits = value.toString().split(",|\\[|\\]|\\s+");
            mapSplits = Arrays.stream(mapSplits)
                    .filter(s -> (s != null && s.length() > 0))
                    .toArray(String[]::new);
            String nodeId = mapSplits[0];
            for(int i = 4; i < mapSplits.length; i++){
                nodeList.add(mapSplits[i]);
            }
            context.write(new Text(nodeId), new Text(nodeList.toString()));
            nodeList.clear();
        }
    }


    public static class finalSumReducer extends Reducer<Text, Text, Text, Text> {
//        input format: a  [b:dis_b, c:dis_c], [e:dis_e, f:dis_f]
//        output format: a   [b:dis_b, f:dis_f]
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            List totalList = new ArrayList<>();
            String nodeId;
            String distance;
            SortedMap<String, String> topK = new TreeMap<String, String>();
            for(Text val : values){
//                context.write(key,val);
                String[] reducerSplits = val.toString().split(",|\\[|\\]|\\s+");
                reducerSplits = Arrays.stream(reducerSplits)
                        .filter(s -> (s != null && s.length() > 0))
                        .toArray(String[]::new);
                for(String item : reducerSplits){
                    nodeId = item.split(":")[0];
                    distance = item.split(":")[1];
//                  if two nodes have the same distance to the current node
                    if(topK.containsKey(distance)){
                        distance = distance + "_"+ nodeId;
                    }
                    topK.put(String.valueOf(distance), nodeId);
                    if (topK.size() > k){
                        topK.remove(topK.lastKey());
                    }
                }
            }
            for (String nodeDistance : topK.keySet()) {
                String nodeInTree = topK.get(nodeDistance);
                totalList.add(nodeInTree+":"+nodeDistance.split("_")[0]);
            }
            context.write(key, new Text(totalList.toString()));
        }
    }


    public static void main(String[] args) throws Exception {
        // input format: input/ output4/ n k
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        System.out.println(fs.listFiles(new Path("input"), false));
        n = Integer.parseInt(args[2]);
        k = Integer.parseInt(args[3]);
        Job job = Job.getInstance(conf, "stage 4");
        job.setJarByClass(Stage4.class);
        job.setMapperClass(Stage4.simplificationMapper.class);
        job.setReducerClass(Stage4.finalSumReducer.class);
        job.setNumReduceTasks(1);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
