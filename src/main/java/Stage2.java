import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;

public class Stage2 {
    private static double[] range;
    private static int n;
    private static int k;

    public static class NodeListMapper extends Mapper<Object, Text, Text, Text> {
        private Text cellId = new Text();
        private Text node = new Text();
        HashMap<Integer,Integer> testMapping;
        {
            try {
                testMapping = Util.loadMapping("idMapping/mapping");
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
//        input format: a   xa, ya
//        output format: cellId   a, xa, ya
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
                String[] splits = value.toString().split(",");
                String nodeId = splits[0];
                double x = Double.parseDouble(splits[1]);
                double y = Double.parseDouble(splits[2]);
                String oldCellId = Util.getCellId(x, y, range, n);
                int newCellId = Util.id2UID(Integer.parseInt(oldCellId) ,testMapping);
                cellId.set(String.valueOf(newCellId));
                node.set(nodeId + "," + x + "," + y);
                context.write(cellId, node);
        }
    }


    public static class NodeSumReducer extends Reducer<Text, Text, Text, Text> {
//        input format: cellId   a, xa, ya, b, xb, yb
//        output format: a   xa, ya, cellId, [b:dis_b, c:dis_c]
        private Text newKey = new Text();
        private Text result = new Text();
        private String nodeId1, nodeIdn;
        private Double node1X, node1Y, nodenX, nodenY;
        private Text cellId = new Text();
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            List<String> copiedValue = new ArrayList();
            List<String> finalList = new ArrayList();
            SortedMap<Double, String> topK = new TreeMap<Double, String>();
            cellId.set(key);
            for (Text val : values){
                copiedValue.add(val.toString());
            }
            for (String node1 : copiedValue) {
                String[] node1splits = node1.split(",");
                newKey.set(node1splits[0]);
                nodeId1 = node1splits[0];
                node1X = Double.parseDouble(node1splits[1]);
                node1Y = Double.parseDouble(node1splits[2]);
                Double distance;
                for(String noden : copiedValue){
                    String[] nodensplits = noden.split(",");
                    nodeIdn = nodensplits[0];
                    if(nodeId1.equals(nodeIdn)) continue;
                    nodenX = Double.parseDouble(nodensplits[1]);
                    nodenY = Double.parseDouble(nodensplits[2]);
                    distance = Util.getEuclideanDistance(node1X, node1Y, nodenX, nodenY);
                    topK.put(distance, nodeIdn);
                    if (topK.size() > k){
                        topK.remove(topK.lastKey());
                    }
                }
                for (Double nodeDistance : topK.keySet()) {
                    String nodeId = topK.get(nodeDistance);
                    finalList.add(nodeId+":"+nodeDistance);
                }
                result.set(node1X + ", " + node1Y + ", " + cellId + ", "+ finalList.toString());
                context.write(newKey, result);
                topK.clear();
                finalList.clear();
            }
        }
    }

    public static void main(String[] args) throws Exception {
        // input format: input/ output2/ n k
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        System.out.println(fs.listFiles(new Path("input"), false));
        range = Util.getRange("input/50p.csv");
        n = Integer.parseInt(args[2]);
        k = Integer.parseInt(args[3]);
        Job job = Job.getInstance(conf, "stage 2");
        job.setJarByClass(Stage2.class);
        job.setMapperClass(Stage2.NodeListMapper.class);
        job.setReducerClass(Stage2.NodeSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
