import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.LineReader;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class Merge {
    public static void main(String[] args) throws IOException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        String inputFilePath = args[0];
        int k = Integer.parseInt(args[1]);
        HashMap<Integer, long[][]> QTree = Util.generateQTreeFromFile(inputFilePath);
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
            outputStream.write(line.getBytes("UTF-8"));
        }
        System.out.println("Mapping file created.");
        outputStream.close();

        /**
         * Here is the example usage of loading mapping file and use that mapping file to map original cell id to merged UID
         * */
//        HashMap<Integer,Integer> testMapping = Util.loadMapping("idMapping/mapping");
//        for (int i = 0; i < 64; i++) {
//            System.out.println(i+"=>"+Util.id2UID(i,testMapping));
//        }
    }
}
