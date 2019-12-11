import java.io.IOException;
import java.text.SimpleDateFormat;
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
    private static HashMap<Path, double[]> rangeLUT;
    private static int n;

    public static class CellIdMapper extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private final static IntWritable zero = new IntWritable(0);
        private Text word = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            FileSplit split = (FileSplit) context.getInputSplit();
            Path filePath = split.getPath();
            double[] _range = rangeLUT.get(filePath);
            for (int i = 0; i < (int) Math.pow(2, 2 * n); i++) {
                context.write(new Text(Integer.toString(i)), zero);
            }
            StringTokenizer itr = new StringTokenizer(value.toString(), "\n");
            while (itr.hasMoreTokens()) {
                itr.nextToken();
                String[] splits = value.toString().split(",");
                double x = Double.parseDouble(splits[1]);
                double y = Double.parseDouble(splits[2]);
                word.set(Util.getCellId(x, y, _range, n));
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
        FileStatus[] status = fs.listStatus(new Path(args[0]));
        rangeLUT = new HashMap<>();
        for (FileStatus fileStatus : status) {
            Path _p = fileStatus.getPath();
            rangeLUT.put(_p, Util.getRange(_p));
        }
        n = Integer.parseInt(args[2]);
        Job job = Job.getInstance(conf, "stage 1");
        job.setJarByClass(Stage1.class);
        job.setMapperClass(Stage1.CellIdMapper.class);
        job.setCombinerClass(Stage1.IntSumReducer.class);
        job.setReducerClass(Stage1.IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
//        FileOutputFormat.setOutputPath(job, new Path(args[1] + new SimpleDateFormat("yyyy-MM-dd_HH:mm:ss").format(new Date().getTime())));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
