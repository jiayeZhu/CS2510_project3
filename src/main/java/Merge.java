import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.LineReader;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class Merge {
    public static void main(String[] args) throws IOException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        String inputFilePath = args[0];
        FileStatus[] status = fs.listStatus(new Path("input/"));
        Path[] arrInputFilePath = new Path[status.length];
        for (int i = 0; i < arrInputFilePath.length; i++) {
            arrInputFilePath[i] = status[i].getPath();
        }
        Path dataFilePath = new Path(args[1]);
        int k = Integer.parseInt(args[2]);
//        for (Path p : arrInputFilePath) {
        Path p = new Path(inputFilePath);
        HashMap<Integer, long[][]> QTree = Util.generateQTreeFromFile(p);
        Util.printQTree(QTree);
        int n = QTree.size() - 1; // calculate the n;
        //construct the Q-tree using hashmap to represent all the levels
        HashMap<Integer, Integer> idMapping = new HashMap<>();
        for (int level = n; level >= 0; level--) {
            long[][] treeLevel = QTree.get(level); // get data at that level
            int dim = treeLevel.length; // get dimension of that level
            System.out.println("creating mapping for level:" + level);
            if (level == 0) {
                idMapping.put(Util.calculateUID(0, 0, n - level, n), Util.calculateUID(0, 0, n - level, n));
                break;
            }
            for (int i = 0; i < dim; i += 2) {
                for (int j = 0; j < dim; j += 2) {
                    int currentId = Util.calculateUID(i, j, n - level, n);
                    //check if need merge
                    if (treeLevel[i][j] < k + 1 | treeLevel[i][j + 1] < k + 1 | treeLevel[i + 1][j] < k + 1 | treeLevel[i + 1][j + 1] < k + 1) {
                        idMapping.put(Util.calculateUID(i, j, n - level, n), Util.calculateUID(i / 2, j / 2, n - level + 1, n));
                        idMapping.put(Util.calculateUID(i + 1, j, n - level, n), Util.calculateUID(i / 2, j / 2, n - level + 1, n));
                        idMapping.put(Util.calculateUID(i, j + 1, n - level, n), Util.calculateUID(i / 2, j / 2, n - level + 1, n));
                        idMapping.put(Util.calculateUID(i + 1, j + 1, n - level, n), Util.calculateUID(i / 2, j / 2, n - level + 1, n));
                    } else {
                        idMapping.put(Util.calculateUID(i, j, n - level, n), Util.calculateUID(i, j, n - level, n));
                        idMapping.put(Util.calculateUID(i + 1, j, n - level, n), Util.calculateUID(i + 1, j, n - level, n));
                        idMapping.put(Util.calculateUID(i, j + 1, n - level, n), Util.calculateUID(i, j + 1, n - level, n));
                        idMapping.put(Util.calculateUID(i + 1, j + 1, n - level, n), Util.calculateUID(i + 1, j + 1, n - level, n));
                    }
                }
            }
        }
        FSDataOutputStream outputStream = fs.create(new Path("idMapping/mapping"));
        for (Map.Entry<Integer, Integer> entry : idMapping.entrySet()) {
            String line = entry.getKey().toString() + "->" + entry.getValue().toString() + "\n";
            outputStream.write(line.getBytes(StandardCharsets.UTF_8));
        }
        System.out.println("Mapping file created for: "+p.toString());
        outputStream.close();

        System.out.println("Generating detail point data for each new cell using data file: "+dataFilePath.toString());
        //read the original data file

        double[] range = Util.getRange(dataFilePath.toString());
        FSDataInputStream inputStream = fs.open(dataFilePath);
        LineReader reader = new LineReader(inputStream);
        Text text = new Text();
        HashMap<Long, Integer> pointId2cellId = new HashMap<>();
        HashMap<Integer, ArrayList<String>> cellId2pointId = new HashMap<>();
        while (reader.readLine(text) != 0) {
            String[] parts = text.toString().split(",");
            long pointId = Long.parseLong(parts[0]);
            double pointX = Double.parseDouble(parts[1]);
            double pointY = Double.parseDouble(parts[2]);
            String oldCellId = Util.getCellId(pointX, pointY, range, n);
            int newCellId = Util.id2UID(Integer.parseInt(oldCellId), idMapping);

            pointId2cellId.put(pointId, newCellId);

            ArrayList<String> list = cellId2pointId.get(newCellId);
            if (list == null) list = new ArrayList<>();
            list.add(pointId + "," + pointX + "," + pointY);    //<id,x,y>
            cellId2pointId.put(newCellId, list);
            ArrayList<String> old_list = cellId2pointId.get(Integer.parseInt(oldCellId));
            if (old_list == null) old_list = new ArrayList<>();
            old_list.add(pointId + "," + pointX + "," + pointY);    //<id,x,y>
            cellId2pointId.put(Integer.parseInt(oldCellId), old_list);
        }
        String[] p_parts = dataFilePath.toString().split("/");
        outputStream = fs.create(new Path("point2cellLUT/" + p_parts[p_parts.length - 1].split("\\.")[0]));
        for (Map.Entry<Long, Integer> entry : pointId2cellId.entrySet()) {
            String line = entry.getKey().toString() + "->" + entry.getValue().toString() + "\n";
            outputStream.write(line.getBytes(StandardCharsets.UTF_8));
        }
        System.out.println("point2cell LUT created.");
        outputStream.close();
        outputStream = fs.create(new Path("cell2pointLUT/" + p_parts[p_parts.length - 1].split("\\.")[0]));
        for (Map.Entry<Integer, ArrayList<String>> entry : cellId2pointId.entrySet()) {
            String line = entry.getKey().toString() + "->";
            ArrayList<String> pointList = entry.getValue();
            for (int i = 0; i < pointList.size(); i++) {
                if (i != 0) line += ";";
                line += pointList.get(i);
            }
            line += "\n";
            outputStream.write(line.getBytes(StandardCharsets.UTF_8));
        }
        outputStream.close();
//        }

        /**
         * example of loading Look Up Table to see detail points info in a particular cell
         * */
//        HashMap<Integer, ArrayList<Point>> lut =  Util.loadCell2PointLUT("cell2pointLUT/23p");
//        System.out.println(lut.get(83).toString());
    }
}
