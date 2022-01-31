//package clustering;
//
//import java.util.ArrayList;
//import java.util.LinkedList;
//import java.util.Random;
//
//import _aux.lib;
//import _aux.lib.*;
//
//
//
//
//public class lshClustering {
//
//    double[][] data; // array with the data of dim (#rows, #cols) = (n_dim, n_vec)
//    ArrayList<Cluster> clusters;
//    double epsilon;
//    int n_dim;
//    int n_vec;
//    int[][][] hashResults;
//    Random rand = new Random();
//
//
//    public ArrayList<Cluster> fitAndGetClusters(double[][] stockdata, double epsilon, int numberOfBands, int bandSize){
//        this.fit(stockdata,epsilon, numberOfBands, bandSize);
//        return this.getClusters();
//    }
//
//    public void fit(double[][] stockdata, double epsilon, int numberOfBands, int bandSize){
//        this.data = stockdata;
//        this.n_dim = data.length;
//        this.n_vec = data[0].length;
//        this.epsilon = epsilon;
//        this.clusters = new ArrayList<Cluster>(); // delete potential prior fit clusterings for refitting
//        this.hashResults = euclideanHashComposed(0.5*epsilon, numberOfBands, bandSize);
//        for(int i=0; i<n_vec; i++){
//            boolean pointInCluster = false;
//            int[][] vecHash = getVectorHash(hashResults, i);
//
//            for(Cluster c: clusters){
//                int[][] centroidHash = c.getCentroidHash();
//
//                if(appendToClusterBool(vecHash, centroidHash)){
//                    c.addItem(i, getColumn(this.data, i));
//                    c.addVectorHash(vecHash); // for debugging and checking if the clustering works
//
//                    double dist = lib.euclidean(c.centroid, getColumn(this.data, i));
//                    c.checkAndSetMaxDist(dist); // might come in handy for making tighter bounds
//
//                    pointInCluster = true;
//                }
//            }
//
//            if(!pointInCluster){ // the point is not added to a cluster yet, so create a new cluster with this point
//                Cluster c = new Cluster(clusters.size(), clusters.size(), getColumn(this.data, i));
//                c.setCentroidHash(vecHash); // for now just make the first vector to be added to a cluster the centroid
//                this.clusters.add(c);
//            }
//        }
//    }
//
//    public boolean appendToClusterBool(int[][] vectorHash, int[][] centroidHash){
//        boolean toAppend = false;
//        for(int i=0; i<vectorHash.length; i++){ // check each band in the OR-composition
//            boolean conjunction = true;
//            for(int j = 0; j<vectorHash[i].length; j++){ //check the AND composition
//                if(vectorHash[i][j] != centroidHash[i][j]){ // if one of the hash results in the AND-composition is different, the whole band is False
//                    conjunction = false;
//                    break;
//                }
//            }
//            if(conjunction){
//                toAppend = true;
//                return toAppend;
//            }
//        }
//        return toAppend;
//    }
//
//    public int[][][] euclideanHashComposed(double binWidth, int numberOfBands, int bandSize){
//        // number of bands indicates the amount of OR compositions
//        // bandSize indicates the amount of AND compositions within the OR compositions
//        int[][][] table = new int[numberOfBands][bandSize][n_vec];
//        for(int i = 0; i<numberOfBands; i++){
//            for(int j=0; j<bandSize; j++){
//                table[i][j] = euclideanHashOnce(binWidth);
//            }
//        }
//        return table;
//    }
//
//    public int[] euclideanHashOnce(double binWidth){
//        // one iteration of LSH hashing -> perform this multiple times to get good probabilistic guarantees
//        double[] projections = randomProject(this.data);
//        int[] binIDs = new int[n_vec];
//        for(int i=0; i<binIDs.length; i++){
//            double bucketID = Math.floor(projections[i] / binWidth);
//            binIDs[i] = (int) bucketID;
//        }
//        return binIDs;
//    }
//
//    public double[] randomProject(double[][] dataset){ // dataset should be of shape (rows = n_dim, cols = n_vec)
//
//        // create a random projection vector:
//        double[] projection = new double[dataset.length];
//        for(int i=0; i<n_dim; i++){
//            projection[i] = rand.nextGaussian();
//        }
//        // l2 normalization
//        double l2 = 0;
//        for(double i: projection){l2+= Math.pow(i,2);} // calculate l2-norm
//        // normalize vector (see https://www.cs.utah.edu/~jeffp/teaching/cs5955/L6-LSH.pdf -> 6.4 LSH for ...)
//        for(int i = 0; i<n_dim; i++){projection[i]/=l2;}
//        double[] out = new double[n_vec];
//        for(int i=0; i<n_vec; i++){ // project each time series/vector
//            out[i] = dotProduct(getColumn(dataset, i), projection);
//        }
//        return out; // the projection of each vector in the dataset
//    }
//
//
//
//
//    // some more auxiliary functions:
//
//
//
//
//
//    public int[][] getVectorHash(int[][][] hashTable, int vecID){
//        int[][] vecHash = new int[hashTable.length][hashTable[0].length];
//        for(int i=0; i<hashTable.length; i++){
//            for(int j = 0; j<hashTable[i].length; j++){
//                vecHash[i][j] = hashTable[i][j][vecID];
//            }
//        }
//        return vecHash;
//    }
//
//    public double[] getColumn(double[][] array, int colID){ // convenience method to get a vector out of the dataset
//        double[] col = new double[array.length];
//        for(int i=0; i<array.length; i++){
//            col[i]=array[i][colID];
//        }
//        return col;
//    }
//
//    public double dotProduct(double[] a, double[] b){ // dot product for Eucldidean LSH
//        double result = 0;
//        for(int i = 0; i<a.length; i++){
//            result += a[i]*b[i];
//        }
//        return result;
//    }
//
//    public ArrayList<Cluster> getClusters(){return this.clusters;}
//
//
//}
