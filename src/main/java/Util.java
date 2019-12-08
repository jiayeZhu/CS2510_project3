import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.util.LineReader;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.HashMap;

public class Util {
    public static int log2(double x) {
        return (int) (Math.log(x) / Math.log(2));
    }

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
        if (cellId_x == (int) Math.pow(2, n)) cellId_x--;
        int cellId_y = (int) ((y - yMin) / splitLength);
        if (cellId_y == (int) Math.pow(2, n)) cellId_y--;
        cellId_y = (int) Math.pow(2, n) - 1 - cellId_y;
        return Integer.toString(cellId_x + cellId_y * splitNumber);
    }

    public static HashMap<Integer, long[][]> generateQTreeFromFile(String filePath) throws IOException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        FSDataInputStream inputStream = fs.open(new Path(filePath));
        LineReader reader = new LineReader(inputStream);
        Text text = new Text();
        HashMap<Long, Long> baseData = new HashMap<>();
        while (reader.readLine(text) != 0) {
            String[] parts = text.toString().split("\t");
            //add <cellId, cellSum> to hash map
            baseData.put(Long.parseLong(parts[0]), Long.parseLong(parts[1]));
        }

        int n = log2(Math.sqrt(baseData.size())); // calculate the n;
        //construct the Q-tree using hashmap to represent all the levels
        HashMap<Integer, long[][]> QTree = new HashMap<>();
        for (int level = n; level >= 0; level--) {
            if (level == n) {
                int dim = (int) Math.pow(2, level);
                long[][] treeLevel = new long[dim][dim];
                for (int i = 0; i < dim; i++) {
                    for (int j = 0; j < dim; j++) {
                        treeLevel[i][j] = baseData.get((long) (i * dim + j));
                    }
                }
                QTree.put(level, treeLevel);
            } else {
                long[][] nextLevel = QTree.get(level + 1);
                int dim = nextLevel.length;
                int newDim = dim / 2;
                long[][] treeLevel = new long[newDim][newDim];
                for (int i = 0; i < dim; i += 2) {
                    for (int j = 0; j < dim; j += 2) {
                        treeLevel[(i / 2)][(j / 2)] = nextLevel[i][j] + nextLevel[i + 1][j] + nextLevel[i][j + 1] + nextLevel[i + 1][j + 1];
                    }
                }
                QTree.put(level, treeLevel);
            }
        }
        return QTree;

    }

    public static int calculateUID(int row, int col, int height, int depth) {
        int border = (int) Math.pow(2, depth - height);
        int base = 0;
        for (int i = 0; i < height; i++) {
            base += (int) Math.pow(2, 2 * (depth - i));
        }
        int bias = row * border + col;
        return base + bias;
    }

    public static void printQTree(HashMap<Integer, long[][]> QTree) {
        int depth = QTree.size();
        for (int d = 0; d < depth; d++) {
            long[][] treeLevel = QTree.get(d);
            System.out.println("Level:" + d);
            int dim = treeLevel.length;
            for (int i = 0; i < dim; i++) {
                for (int j = 0; j < dim; j++) {
                    System.out.print(treeLevel[i][j] + "\t");
                }
                System.out.println();
            }
        }
    }

    public static int id2UID(int id, HashMap<Integer, Integer> mapping) throws IOException {
        int result = id;
        while (mapping.get(result) != result) result = mapping.get(result);
        return result;
    }

    public static HashMap<Integer, Integer> loadMapping(String mappingFilePath) throws IOException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        FSDataInputStream inputStream = fs.open(new Path(mappingFilePath));
        LineReader reader = new LineReader(inputStream);
        Text text = new Text();
        HashMap<Integer, Integer> mapping = new HashMap<>();
        while (reader.readLine(text) != 0) {
            String[] parts = text.toString().split("->");
            int source = Integer.parseInt(parts[0]);
            int target = Integer.parseInt(parts[1]);
            mapping.put(source, target);
        }
        return mapping;
    }

    public static Double getEuclideanDistance(double x1, double y1, double x2, double y2) {
        double deltaX = x1 - x2;
        double deltaY = y1 - y2;
        double result = Math.sqrt(deltaX*deltaX + deltaY*deltaY);
        return result;
    }
}
