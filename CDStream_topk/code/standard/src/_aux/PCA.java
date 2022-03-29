//package _aux;
//
//import org.apache.spark.SparkConf;
//import org.apache.spark.SparkContext;
//import org.apache.spark.api.java.*;
//import org.apache.spark.mllib.linalg.Matrix;
//import org.apache.spark.mllib.linalg.Vector;
//import org.apache.spark.mllib.linalg.Vectors;
//import org.apache.spark.mllib.linalg.distributed.RowMatrix;
//
//import java.util.LinkedList;
//
//public class PCA {
//    public static double[][] PCA(double[][] array, int n_components) {
//        SparkConf conf = new SparkConf().setAppName("PCA Example");
//        SparkContext sc = new SparkContext(conf);
//
//
//        LinkedList<Vector> rowsList = new LinkedList<Vector>();
//        for (int i = 0; i < array.length; i++) {
//            Vector currentRow = Vectors.dense(array[i]);
//            rowsList.add(currentRow);
//        }
//        JavaRDD<Vector> rows = JavaSparkContext.fromSparkContext(sc).parallelize(rowsList);
//
//        // Create a RowMatrix from JavaRDD<Vector>.
//        RowMatrix mat = new RowMatrix(rows.rdd());
//
//        // Compute the principal components.
//        Matrix pc = mat.computePrincipalComponents(n_components);
//        RowMatrix projected = mat.multiply(pc);
//
//        Matrix test = (Matrix) projected.toBreeze();
//        double[][] out = (double[][]) test.toArray();
//        return out;
//    }
//}