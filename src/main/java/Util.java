import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.util.LineReader;
import org.apache.hadoop.io.Text;

public class Util {
    public static double[] getRange(String filePath) {
        Configuration conf = new Configuration();
        try {
            FileSystem fs = FileSystem.get(conf);
            FSDataInputStream inputStream = fs.open(new Path(filePath));
            LineReader reader = new LineReader(inputStream);
            Text text = new Text();
            double xMin = 0;
            double xMax = 0;
            double yMin = 0;
            double yMax = 0;
            while (reader.readLine(text) != 0) {
                String[] parts = text.toString().split(",");
                double xTmp = Double.parseDouble(parts[1]);
                double yTmp = Double.parseDouble(parts[2]);
                xMin = Math.min(xTmp, xMin);
                xMax = Math.max(xTmp, xMax);
                yMin = Math.min(yTmp, yMin);
                yMax = Math.max(yTmp, yMax);
            }
            double length = xMax - xMin;
            double width = yMax - yMin;
            if (length > width) {
                double deltaWidth = length - width;
                yMin = yMin - deltaWidth / 2;
                yMax = yMax + deltaWidth / 2;
            } else if (length < width) {
                double deltaLength = width - length;
                xMin = xMin - deltaLength / 2;
                xMax = xMax + deltaLength / 2;
            }
//            System.out.println(xMin);
//            System.out.println(xMax);
//            System.out.println(yMin);
//            System.out.println(yMax);
            return new double[]{xMin, yMin, xMax - xMin}; //(LeftDown.x, LeftDown.y, borderLength)
        } catch (Exception e) {
            System.err.println("ERROR: get range failed.");
            System.err.println(e.toString());
            return new double[]{};
        }

    }

    public static String getCellId(double x, double y, double[] range, int n) {
        double xMin = range[0];
        double yMin = range[1];
        double borderLength = range[2];
        double xMax = xMin + borderLength;
        double yMax = yMin + borderLength;
        int splitNumber = (int) Math.pow(2, n);
        double splitLength = borderLength / splitNumber;

        int cellId_x = (int) ((x - xMin) / splitLength);
        int cellId_y = (int) ((y - yMin) / splitLength);
        return Integer.toString(cellId_x * splitNumber + cellId_y);
    }
}
