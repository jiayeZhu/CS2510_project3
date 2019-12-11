import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class Stage1 {
    public static class CellIdMapper extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private final static IntWritable zero = new IntWritable(0);
        private Text word = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            double[] range = Arrays.stream(conf.get("range").split(",")).mapToDouble(Double::parseDouble).toArray();
            int n = Integer.parseInt(conf.get("n"));
            FileSplit split = (FileSplit) context.getInputSplit();
            for (int i = 0; i < (int) Math.pow(2, 2 * n); i++) {
                context.write(new Text(Integer.toString(i)), zero);
            }
            StringTokenizer itr = new StringTokenizer(value.toString(), "\n");
            while (itr.hasMoreTokens()) {
                itr.nextToken();
                String[] splits = value.toString().split(",");
                double x = Double.parseDouble(splits[1]);
                double y = Double.parseDouble(splits[2]);
                word.set(Util.getCellId(x, y, range, n));
                context.write(word, one);
            }
        }
    }

    public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);

        Path _p = new Path(args[0] + "data.csv");
        double[] range = Util.getRange(_p);
        conf.set("n",args[2]);
        conf.set("range",range[0]+","+range[1]+","+range[2]);
        Job job = Job.getInstance(conf, "stage 1");
        job.setJarByClass(Stage1.class);
        job.setMapperClass(Stage1.CellIdMapper.class);
        job.setCombinerClass(Stage1.IntSumReducer.class);
        job.setReducerClass(Stage1.IntSumReducer.class);
        job.setNumReduceTasks(Integer.parseInt(args[4])); //arg[3] is k but not used here
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        int exitSig = job.waitForCompletion(true) ? 0 : 1;
        if (exitSig == 1) System.exit(1);
        System.out.println("====== start merging ======");
        Util.getMergeInHdfs(conf, args[1], args[1] + "../stage1_merged/stage1_output.txt");
        Merge.main(new String[]{args[1] + "../stage1_merged/stage1_output.txt", "input/data.csv", args[3]}); //arg[3] is k
        System.out.println("====== merging finished ======");
        System.exit(0);
    }
}
