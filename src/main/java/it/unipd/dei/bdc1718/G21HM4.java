package it.unipd.dei.bdc1718;

import org.apache.spark.Partition;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import java.io.IOException;
import java.lang.reflect.Array;
import java.util.*;

import static it.unipd.dei.bdc1718.InputOutput.readVectors;

public class G21HM4 {

    public static void main(String[] args) throws IOException {


        //taking all the input parameters from args
        int k = Integer.parseInt(args[0]);
        int numBlocks = Integer.parseInt(args[1]);

        //configuring spark
        SparkConf configuration = new SparkConf(true)
                .setAppName("Diversity Maximization");
        JavaSparkContext sc = new JavaSparkContext(configuration);
        JavaRDD<Vector> pointsrdd = readVectors(sc, args[2]).repartition(numBlocks).cache();

        //appling runMapReduce to gather the sample dpoints
        ArrayList<Vector> kPoints = runMapReduce(pointsrdd,k,numBlocks);


        //computing the average between the selected points
        double avgDist = measure(kPoints);

        System.out.println("La the average distance is =" + avgDist);

    }

    //method that select k points of the set using mapreduce and sequential approx alg.
    public static ArrayList<Vector> runMapReduce(JavaRDD<Vector> notPartitionedPointsrdd, int k, int numBlocks){

        //variables used for time estimation
        long startCoreset,endCoreset,startComputation, endComputation = 0;

        //(a)partitioning set
        startCoreset = System.currentTimeMillis();


        //(b)extracts k points from each subset by running the sequential Farthest-First Traversal algorithm
        JavaRDD<Vector> chosenK = notPartitionedPointsrdd.repartition(numBlocks).mapPartitions((set) ->{
            ArrayList<Vector> chosen = new ArrayList<>();
            while(set.hasNext()){
                chosen.add(set.next());
            }
            ArrayList<Vector> chosenKVector = kCenter(chosen,k);
            return chosenKVector.iterator();
        });

        //(c)collect all the selected k points into an array, using a cast to force list into arraylist
        ArrayList<Vector> coreset = new ArrayList<Vector>(chosenK.collect());
        endCoreset = System.currentTimeMillis();
        //compute and print time coreset construction
        System.out.println("The coreset construction time is = "+(endCoreset-startCoreset)+" ms");

        //(d)execute the sequential approximation max-density algorithm(defined at the end) on the coreset
        //and return the result
        startComputation = System.currentTimeMillis();
        ArrayList<Vector> maxDistancePoint = runSequential(coreset,k);
        endComputation = System.currentTimeMillis();

        System.out.println("The solution computation time is = "+(endComputation-startComputation)+" ms");

        return maxDistancePoint;
    }

    //method that compute and return the average distance between all the points of the input arraylist
    public static double measure(ArrayList<Vector> pointlist){


        int dimension = pointlist.size();
        //the number of pairs
        int numPairs = (dimension*(dimension-1))/2;

        double sumDistances=0;
        for(int i = 0; i< dimension; i++){
            for(int j =i+1; j< dimension; j++){

                sumDistances += Math.sqrt(Vectors.sqdist(pointlist.get(i),pointlist.get(j)));
            }
        }
        double avgDist = sumDistances/numPairs;
        return avgDist;
    }

    //Sequential approximation algorithm based on matching.
    public static ArrayList<Vector> runSequential(final ArrayList<Vector> points, int k) {

        final int n = points.size();
        if (k >= n) {
            return points;
        }

        ArrayList<Vector> result = new ArrayList<>(k);
        boolean[] candidates = new boolean[n];
        Arrays.fill(candidates, true);
        for (int iter=0; iter<k/2; iter++) {
            // Find the maximum distance pair among the candidates
            double maxDist = 0;
            int maxI = 0;
            int maxJ = 0;
            for (int i = 0; i < n; i++) {
                if (candidates[i]) {
                    for (int j = i+1; j < n; j++) {
                        if (candidates[j]) {
                            double d = Math.sqrt(Vectors.sqdist(points.get(i), points.get(j)));
                            if (d > maxDist) {
                                maxDist = d;
                                maxI = i;
                                maxJ = j;
                            }
                        }
                    }
                }
            }
            // Add the points maximizing the distance to the solution
            result.add(points.get(maxI));
            result.add(points.get(maxJ));
            // Remove them from the set of candidates
            candidates[maxI] = false;
            candidates[maxJ] = false;
        }
        // Add an arbitrary point to the solution, if k is odd.
        if (k % 2 != 0) {
            for (int i = 0; i < n; i++) {
                if (candidates[i]) {
                    result.add(points.get(i));
                    break;
                }
            }
        }
        if (result.size() != k) {
            throw new IllegalStateException("Result of the wrong size");
        }
        return result;
    }//end of seq approx alg



    //kcenter retrived from hm3
    private static ArrayList<Vector> kCenter(ArrayList<Vector> vectorsArray, int iKCenters) {
        System.out.println("Applying kcenter algorithm... with k ="+ iKCenters);
        ArrayList<Vector> vectorsSetC = new ArrayList<Vector>(); // set C of k centers

        int iCompleteDataSetSize = vectorsArray.size();

        // If the chosen K is equal to the size of the dataset, return the dataset
        if (iKCenters == iCompleteDataSetSize)
            return vectorsArray;

        Random random = new Random();
        int iFirstVectorIndex = random.nextInt(iCompleteDataSetSize);

        Vector firstVector = vectorsArray.get(iFirstVectorIndex);
        vectorsSetC.add(0, firstVector);

        double[] dMinDist = new double[iCompleteDataSetSize];
        Arrays.fill(dMinDist, Double.MAX_VALUE);
        Boolean[] bVectorRemoved = new Boolean[iCompleteDataSetSize];
        Arrays.fill(bVectorRemoved, false);
        bVectorRemoved[iFirstVectorIndex] = true;

        for (int i = 1; i < iKCenters; i++) {
            int iFarthestVectorIndex = -1;
            double dMaxDistFromClustersCenters = Double.MIN_VALUE;
            Vector lastAddKCenterVector = vectorsSetC.get(i - 1);

            for (int j = 0; j < iCompleteDataSetSize; j++) {
                // If the vector has been "removed" from P, because it was add to C, then continue
                if (bVectorRemoved[j])
                    continue;

                Vector currentVector = vectorsArray.get(j);
                double dCurrentDist = Vectors.sqdist(currentVector, lastAddKCenterVector);
                // for all P - C points update their min distances from the k centers
                if (dCurrentDist < dMinDist[j])
                    dMinDist[j] = dCurrentDist;

                // find the max distance in the set of the min distances
                if (dMinDist[j] > dMaxDistFromClustersCenters) {
                    dMaxDistFromClustersCenters = dMinDist[j];
                    iFarthestVectorIndex = j;
                }
            }

            // Add the farthest vector from the set of k centers to C; "remove" it from the initial data set P
            vectorsSetC.add(i, vectorsArray.get(iFarthestVectorIndex));
            bVectorRemoved[iFarthestVectorIndex] = true;
        }
        //return the cluster centers set
        return vectorsSetC;
    }//end of kcenter

}
