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
    private static double[] range;
    private static int n;
    private static int k;
    private static HashMap<Integer, Integer> mapping;
    private static HashMap<Integer, ArrayList<Point>> lut;

    public static class CheckMapper extends Mapper<Object, Text, Text, Text> {
        private Text cellId = new Text();
        private Text output = new Text();
        private List nodeList = new ArrayList<>();

        String flag = "false";
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
            cellId.set(Cell);
            for(int i = 4; i < mapSplits.length; i++){
                nodeList.add(mapSplits[i]);
            }
            double r = Double.parseDouble(mapSplits[mapSplits.length-1].split(":")[1]);
//            TODO: check overlap, change cellID and flag
            ArrayList<Integer> overlappedList = Util.getOverlappedCellList(x,y,Integer.parseInt(Cell),r,n,range,mapping,lut);
            if (overlappedList.size()==0){
                flag = "true";
                output.set(nodeId + ", " + x + ", " + y + ", " + nodeList.toString() + ", "+ flag);
                context.write(cellId, output);
                System.out.println(cellId.toString()+"\t"+output.toString());
            }
            else {
                flag = "false";
                output.set(nodeId + ", " + x + ", " + y + ", " + nodeList.toString() + ", "+ flag);
                context.write(cellId, output);
                System.out.println(cellId.toString()+"\t"+output.toString());
                for (int i = 0; i < overlappedList.size(); i++) {
                    cellId.set(new Text(String.valueOf(overlappedList.get(i))));
                    context.write(cellId, output);
                    System.out.println(cellId.toString()+"\t"+output.toString());
                }
            }

            nodeList.clear();
        }
    }

    public static class CalculationReducer extends Reducer<Text, Text, Text, Text> {
        private Text output = new Text();
//        HashMap<Integer, ArrayList<Point>> lut;
//        {
//            try {
//                lut = Util.loadCell2PointLUT("cell2pointLUT/23p");
//            } catch (IOException e) {
//                e.printStackTrace();
//            }
//        }
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
//            input format: cellId   a   xa, ya, [b:dis_b, c:dis_c], true/false
//            output format: a    xa, ya, cellID, [e:dis_e, f:dis_f]
            String cellID = key.toString();
            String flag;
            List nodeList = new ArrayList<>();
            SortedMap<String, String> topK = new TreeMap<String, String>();
            List newNodeList = new ArrayList<>();
            for (Text val: values){
                String[] reduceSplits = val.toString().split(",|\\[|\\]|\\s+");
                reduceSplits = Arrays.stream(reduceSplits)
                        .filter(s -> (s != null && s.length() > 0))
                        .toArray(String[]::new);
                String nodeId = reduceSplits[0];
                double x = Double.parseDouble(reduceSplits[1]);
                double y = Double.parseDouble(reduceSplits[2]);
                flag = reduceSplits[reduceSplits.length-1];
                if(flag.equals("true")){
                    for(int i = 3; i < reduceSplits.length-1; i++){
                        nodeList.add(reduceSplits[i]);
                    }
                    output.set(x + ", " + y + ", "+ cellID + "," + nodeList.toString());
                    context.write(new Text(nodeId), output);
                    nodeList.clear();
                }else{
                    List<Point> nearbyNodes = lut.get(Integer.parseInt(cellID));
                    for(Point nearbynode : nearbyNodes){
                        String nearbyId = String.valueOf(nearbynode.id);
                        if (nodeId.equals(nearbyId)) continue;
                        Double nearbyx = nearbynode.x;
                        Double nearbyy = nearbynode.y;
                        String distance = String.valueOf(Util.getEuclideanDistance(nearbyx, nearbyy, x, y));
                        if(topK.containsKey(distance)){
                            distance = distance + "_"+ nearbyId;
                        }
                        topK.put(String.valueOf(distance), nearbyId);
                        if (topK.size() > k){
                            topK.remove(topK.lastKey());
                        }
                    }
                    for (String nearbyDistance : topK.keySet()) {
                        String theNodeId = topK.get(nearbyDistance);
                        newNodeList.add(theNodeId +":"+nearbyDistance.split("_")[0]);
                    }
                    output.set(x + ", " + y+ ", "+ cellID + "," + newNodeList.toString());
                    context.write(new Text(nodeId), output);
                    newNodeList.clear();
                    topK.clear();
                }
            }
        }
    }


    public static void main(String[] args) throws Exception {
        // input format: input/ output3/ n k
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        range = Util.getRange("input/23p.csv");
        n = Integer.parseInt(args[2]);
        k = Integer.parseInt(args[3]);
        try {
            mapping = Util.loadMapping("idMapping/mapping");
            lut = Util.loadCell2PointLUT("cell2pointLUT/23p");
        } catch (IOException e) {
            e.printStackTrace();
        }
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
