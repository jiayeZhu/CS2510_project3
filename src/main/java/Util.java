import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.util.LineReader;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

public class Util {
    public static int log2(double x) {
        return (int) (Math.log(x) / Math.log(2));
    }

    public static double[] getRange(Path p) {
        return getRange(p.toString());
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

    public static HashMap<Integer, long[][]> generateQTreeFromFile(Path filePath) throws IOException {
        return generateQTreeFromFile(filePath.toString());
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
//        System.out.println(id);
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
//            int level = Integer.parseInt(parts[0].split("@")[1]);
            int target = Integer.parseInt(parts[1]);
//            if (map2level) {
//                mapping.put(source, level);
//            } else {
                mapping.put(source, target);
//            }
        }
        return mapping;
    }

    public static Double getEuclideanDistance(double x1, double y1, double x2, double y2) {
        double deltaX = x1 - x2;
        double deltaY = y1 - y2;
        double result = Math.sqrt(deltaX * deltaX + deltaY * deltaY);
        return result;
    }

    public static HashMap<Integer, ArrayList<Point>> loadCell2PointLUT(String LUTPath) throws IOException {
        return loadCell2PointLUT(new Path(LUTPath));
    }

    public static HashMap<Integer, ArrayList<Point>> loadCell2PointLUT(Path LUTPath) throws IOException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        FSDataInputStream inputStream = fs.open(LUTPath);
        LineReader reader = new LineReader(inputStream);
        Text text = new Text();
        HashMap<Integer, ArrayList<Point>> LUT = new HashMap<>();
        while (reader.readLine(text) != 0) {
            String[] parts = text.toString().split("->");
            int cellId = Integer.parseInt(parts[0]);
            String[] arrStrPoints = parts[1].split(";");
            ArrayList<Point> points = new ArrayList<>();
            for (String arrStrPoint : arrStrPoints) {
                String[] dataInfoParts = arrStrPoint.split(",");
                points.add(new Point(Long.parseLong(dataInfoParts[0]), Double.parseDouble(dataInfoParts[1]), Double.parseDouble(dataInfoParts[2])));
            }
            LUT.put(cellId, points);
        }
        return LUT;
    }

    public static int[] getCellLevelAndBase(int cellId, int n) {
        int base = 0;
        int height = 0;
        while (base + (int) Math.pow(2, 2 * (n - height)) < cellId) {
            base += (int) Math.pow(2, 2 * (n - height));
            height++;
        }
        return new int[]{n - height, base};
    }

    public static ArrayList<Integer> getOverlappedCellList(double x, double y, int cellId, double r, int n, double[] range, HashMap<Integer, Integer> mapping,HashMap<Integer, ArrayList<Point>> lut) throws IOException {
        int[] levelNbase = getCellLevelAndBase(cellId, n);
        int cellLevel = levelNbase[0];
        int cellBase = levelNbase[1];
        int border = (int) Math.pow(2, cellLevel);
        int bias = cellId - cellBase;
        int row = bias / border;
        int col = bias - row * border;

        double WholeBorderLength = range[2];
        double Whole_x_min = range[0];
        double Whole_y_min = range[1];
        double Whole_x_max = Whole_x_min + WholeBorderLength;
        double Whole_y_max = Whole_y_min + WholeBorderLength;

        double cellBorderLength = WholeBorderLength / border;
        double cell_x_min = Whole_x_min + cellBorderLength * col;
        double cell_y_min = Whole_y_min + cellBorderLength * (border - row - 1);
        double cell_x_max = cell_x_min + cellBorderLength;
        double cell_y_max = cell_y_min + cellBorderLength;

        ArrayList<Integer> cellList = new ArrayList<>();

        // check if the circle is inside the cell
        if (x + r <= cell_x_max && x - r >= cell_x_min && y + r <= cell_y_max && y - r >= cell_y_min)
            return cellList;

        //sad. there will be overlapped cell
        int splitNumber = (int) Math.pow(2, n);
        double minBorderLength = WholeBorderLength / splitNumber;
        double square_x_min = cell_x_min;
        double square_x_max = cell_x_max;
        double square_y_min = cell_y_min;
        double square_y_max = cell_y_max;
        //search for the real square_x_min
        while (square_x_min > x - r && square_x_min != Whole_x_min)
            square_x_min = Math.max(square_x_min - minBorderLength, Whole_x_min);
        //search for the real square_x_max
        while (square_x_max < x + r && square_x_max != Whole_x_max)
            square_x_max = Math.min(square_x_max + minBorderLength, Whole_x_max);
        //search for the real square_y_min
        while (square_y_min > y - r && square_y_min != Whole_y_min)
            square_y_min = Math.max(square_y_min - minBorderLength, Whole_y_min);
        //search for the real square_y_max
        while (square_y_max < y + r && square_y_max != Whole_y_max)
            square_y_max = Math.min(square_y_max + minBorderLength, Whole_y_max);

        //calculate the cellIds in the square search area
        int col_start = (int) Math.floor((square_x_min - Whole_x_min) / minBorderLength);
        int col_end = (int) Math.ceil((square_x_max - Whole_x_min) / minBorderLength);
        int row_start = (int) Math.floor((Whole_y_max - square_y_max) / minBorderLength);
        int row_end = (int) Math.floor((Whole_y_max - square_y_min) / minBorderLength);

//        System.out.println("x:"+x+"\ty:"+y);
        for (int C = col_start; C < col_end; C++) {
            for (int R = row_start; R < row_end; R++) {
                //calculate 4 corner to see if the small cell is overlapped or not
                double _x_min = C * minBorderLength;
                double _y_max = Whole_y_max - R * minBorderLength;
                double _x_max = (C + 1) * minBorderLength;
                double _y_min = Whole_y_max - (R + 1) * minBorderLength;
                if (getEuclideanDistance(x, y, _x_min, _y_min) <= r
                        || getEuclideanDistance(x, y, _x_min, _y_max) <= r
                        || getEuclideanDistance(x, y, _x_max, _y_min) <= r
                        || getEuclideanDistance(x, y, _x_max, _y_max) <= r) {
                    int _id = C + R * splitNumber;

                    if (id2UID(_id, mapping) != cellId && lut.get(_id) != null) cellList.add(_id);
                }
            }
        }
        return cellList;
    }
}
