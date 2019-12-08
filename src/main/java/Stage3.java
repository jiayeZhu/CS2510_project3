import com.sun.xml.bind.v2.TODO;
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

public class Stage3 {
    private static int n;
    private static int k;

    public static class CheckMapper extends Mapper<Object, Text, Text, Text> {
        private Text cellId = new Text();
        private Text output = new Text();
        List nodeList = new ArrayList<>();
        String flag = "true";
//        input format: a   xa, ya, cellId, [b:dis_b, c:dis_c]
//        output format: cellId   a   xa, ya, [b:dis_b, c:dis_c], true/false
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
//          split and remove null
            String[] mapSplits = value.toString().split(",|\\[|\\]|\\s+");
            mapSplits = Arrays.stream(mapSplits)
                    .filter(s -> (s != null && s.length() > 0))
                    .toArray(String[]::new);
            String nodeId = mapSplits[0];
            double x = Double.parseDouble(mapSplits[1]);
            double y = Double.parseDouble(mapSplits[2]);
            String Cell = mapSplits[3];
            for(int i = 4; i < mapSplits.length; i++){
                nodeList.add(mapSplits[i]);
            }
//            TODO: check overlap, chang cellID and flag
            output.set(nodeId + ", " + x + ", " + y + ", " + nodeList.toString() + ", "+ flag);
            context.write(new Text(Cell), output);
            nodeList.clear();
        }
    }

    public static class CalculationReducer extends Reducer<Text, Text, Text, Text> {
        private Text output = new Text();
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
//            input format: cellId   a   xa, ya, [b:dis_b, c:dis_c], true/false
//            output format: a    xa, ya, cellID, [e:dis_e, f:dis_f]
            String cellID = key.toString();
            String flag;
            List nodeList = new ArrayList<>();
            for (Text val: values){
                String[] reduceSplits = val.toString().split(",|\\[|\\]|\\s+");
                reduceSplits = Arrays.stream(reduceSplits)
                        .filter(s -> (s != null && s.length() > 0))
                        .toArray(String[]::new);
                String nodeId = reduceSplits[0];
//                context.write(key, new Text(nodeId));

                double x = Double.parseDouble(reduceSplits[1]);
                double y = Double.parseDouble(reduceSplits[2]);
                for(int i = 3; i < reduceSplits.length-1; i++){
                    nodeList.add(reduceSplits[i]);
                }
                flag = reduceSplits[reduceSplits.length-1];
                if(flag.equals("true")){
                    output.set(x + ", " + y + ", "+ cellID + "," + nodeList.toString());
                    context.write(new Text(nodeId), output);
                    nodeList.clear();
                }else{
//                    TODO: compute knn node in overlapped cell, parameter: cell id,  return: nodeList of that cell
                    List newNodeList = null;
                    output.set(x + ", " + y + ", " + ", "+ cellID + "," + newNodeList.toString());
                    context.write(new Text(nodeId), output);
                    newNodeList.clear();
                }
            }
        }
    }


    public static void main(String[] args) throws Exception {
        // input format: input/ output3/ n k
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        System.out.println(fs.listFiles(new Path("input"), false));
        n = Integer.parseInt(args[2]);
        k = Integer.parseInt(args[3]);
        Job job = Job.getInstance(conf, "stage 3");
        job.setJarByClass(Stage3.class);
        job.setMapperClass(Stage3.CheckMapper.class);
        job.setReducerClass(Stage3.CalculationReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1] + new SimpleDateFormat("yyyy-MM-dd_HH-mm-ss").format(new Date().getTime())));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
