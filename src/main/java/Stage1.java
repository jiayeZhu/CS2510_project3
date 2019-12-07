import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class Stage1 {
    private static double[] range;
    private static int n;

    public static class CellIdMapper extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private final static IntWritable zero = new IntWritable(0);
        private Text word = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
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
        System.out.println(fs.listFiles(new Path("input"), false));

        range = Util.getRange("input/1k.csv");
        n = Integer.parseInt(args[2]);
        Job job = Job.getInstance(conf, "stage 1");
        job.setJarByClass(Stage1.class);
        job.setMapperClass(Stage1.CellIdMapper.class);
        job.setCombinerClass(Stage1.IntSumReducer.class);
        job.setReducerClass(Stage1.IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
