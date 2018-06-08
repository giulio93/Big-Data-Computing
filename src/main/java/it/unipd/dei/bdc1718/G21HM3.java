package it.unipd.dei.bdc1718;

import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Random;
import java.util.Scanner;


public class G21HM3 {
    //NB: inputs are passed by console
    //main
    public static void main(String[] args) throws IOException {

        //Scanner and variables
        Scanner consoleInput = new Scanner(System.in);

        //time check parameters
        long start = 0;
        long end = 0;

        //infinite execution loop
        while (true) {
            System.out.println("Executing: \n kcenter; \n then kmeans++ and kmeansObj; \n then kcenter, kmeans++ and kmeans obj");

            //file choice
            System.out.println("Choose a file(they need to be located in project folder); they are datasets of increasing dimensions:\n" +
                    "1 - vecs-50-10000.txt\t" + "2 - vecs-50-50000.txt\t" + "3 - vecs-50-100000.txt\t" + "4 - vecs-50-500000.txt\t" + "5 - exiting program\t");
            int fileNumber = consoleInput.nextInt();
            if (fileNumber == 5) {
                break;
            }
            ArrayList<Vector> P = choosingFile(fileNumber);
            P.size();
            //weight array filled with ones
            ArrayList<Long> WP = new ArrayList<Long>();
            //filling weigth vector of 1s
            for (int i = 0; i < P.size(); i++) {
                WP.add(1L);
            }

            //asking user for a cluster set dimension
            int[] chosenKs = IntegerSelection(P);

            //the first call of kcenter is longer than all the others
            kCenter(P, chosenKs[0]);
            //executing only kcenter and measuring the time it requires
            System.out.println("Executing: ");
            start = System.currentTimeMillis();
            ArrayList<Vector> centers = kCenter(P, chosenKs[0]);
            end = System.currentTimeMillis();
            //diagnostic
            System.out.println("* Chosen centers :");
            for (int tmp = 0; tmp< centers.size(); tmp++){
                System.out.println("* "+ centers.get(tmp));
            }
            System.out.println("* Kcenter terminated returning centers vector.");
            System.out.println("Kcenter elapsed time " + (end - start) + " ms");
            System.out.println();
            System.out.println("****************************************************************************************************");
            System.out.println("****************************************************************************************************");
            System.out.println();
            System.out.println();

            //executing only kmeans++ and applying kmeansObj to the result
            System.out.println("Executing Kmeans++ and applying kmeansObj to the result: ");
            start = System.currentTimeMillis();
            ArrayList<Vector> C = kMeansPP(P, WP, chosenKs[0]);
            end = System.currentTimeMillis();
            //diagnostic
            System.out.println("* Chosen centers :");
            for (int tmp = 0; tmp< C.size(); tmp++){
                System.out.println("* "+ C.get(tmp));
            }
            System.out.println("Kmeans++ elapsed time " + (end - start) + " ms");
            double result2 = kMeansObj(P, C);
            System.out.println("The average minimal distance between points and centers: " + result2);
            System.out.println();
            System.out.println("****************************************************************************************************");
            System.out.println("****************************************************************************************************");
            System.out.println();
            System.out.println();

            //applying kcenter to the original set, kmeans++ to the clusters set obtained by kcenter and kmeansObj on the result of kmans++
            ArrayList<Vector> X = kCenter(P, chosenKs[1]);
            //diagnostic
            System.out.println("* Chosen centers :");
            for (int tmp = 0; tmp< X.size(); tmp++){
                System.out.println("* "+ X.get(tmp));
            }
            ArrayList<Vector> C1 = kMeansPP(X, WP, chosenKs[0]);
            //diagnostic
            System.out.println("* Chosen centers :");
            for (int tmp = 0; tmp< C1.size(); tmp++){
                System.out.println("* "+ C1.get(tmp));
            }
            double result3 = kMeansObj(P, C1);
            System.out.println("The average minimal distance between points and centers: " + result3);
            System.out.println();
            System.out.println("****************************************************************************************************");
            System.out.println("****************************************************************************************************");
            System.out.println();
            System.out.println();

        }//end of infinite loop
    }//end of main

    // kcenter
    public static ArrayList<Vector> kCenter(ArrayList<Vector> vectorsArray, int iKCenters) {
        System.out.println("Applying kcenter algorithm...");
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


    //kmeans++
    public static ArrayList<Vector> kMeansPP(ArrayList<Vector> vectorsArray, ArrayList<Long> weights, int iKCenters) {
        System.out.println("Applying kmeans++ algorithm...");
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
            double dSumOfSquaredMinDist = 0.0;
            double[] dProbabilities = new double[iCompleteDataSetSize];
            Arrays.fill(dProbabilities, 0.0);
            double dMaxProbability = 0.0;
            int iMoreProbableVectorIndex = -1;
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

                dSumOfSquaredMinDist += dMinDist[j] * weights.get(j);
            }

            // compute the probabilities
            for (int k = 0; k < iCompleteDataSetSize; k++) {
                if (bVectorRemoved[k])
                    continue;

                dProbabilities[k] = dMinDist[k] * weights.get(k) / dSumOfSquaredMinDist;
                if (dProbabilities[k] > dMaxProbability) {
                    dMaxProbability = dProbabilities[k];
                    iMoreProbableVectorIndex = k;
                }
            }

            // Add the more probable vector to C; "remove" it from the initial data set P
            vectorsSetC.add(i, vectorsArray.get(iMoreProbableVectorIndex));
            bVectorRemoved[iMoreProbableVectorIndex] = true;
        }

        return vectorsSetC;
    }//end of kmeans++

    // kmeansObj
    public static double kMeansObj(ArrayList<Vector> completeDataSetP, ArrayList<Vector> centersDataSetC) {
        System.out.println("Applying kmeansObj algorithm...");
        ArrayList<Vector> dataSet = new ArrayList<>(completeDataSetP);
        int iCompleteDataSetSize = completeDataSetP.size();
        int iCentersDataSetSize = centersDataSetC.size();

        // If the two datasets are the same
        if (iCompleteDataSetSize == iCentersDataSetSize)
            return 0.0;

        double dSumOfSquaredMinDist = 0.0;

        // Update the new dataset without the center of the clusters
        for (int i = 0; i < iCentersDataSetSize; i++) {
            Vector currCenterVector = centersDataSetC.get(i);
            if (dataSet.contains(currCenterVector))
                dataSet.remove(dataSet.indexOf(currCenterVector));
        }

        int iDatasetSize = dataSet.size();
        double[] dMinDist = new double[iDatasetSize];
        Arrays.fill(dMinDist, Double.MAX_VALUE);

        // Compute the distances from the closest center for all P - C points
        for (int i = 0; i < iCentersDataSetSize; i++) {
            for (int j = 0; j < iDatasetSize; j++) {
                Vector currentVector = dataSet.get(j);
                Vector centerVector = centersDataSetC.get(i);
                double dCurrentDist = Vectors.sqdist(currentVector, centerVector);

                if (dCurrentDist < dMinDist[j])
                    dMinDist[j] = dCurrentDist;
            }
        }

        // Compute the sum of the squared min distances
        for (int k = 0; k < iDatasetSize; k++)
            dSumOfSquaredMinDist += dMinDist[k];

        // Compute and return the mean squared min distance from the centers
        double dMeanSquaredMinDist = dSumOfSquaredMinDist / iDatasetSize;

        return dMeanSquaredMinDist;
    }//end of kmeansObj

    //method of choosing file
    private static ArrayList<Vector> choosingFile(int sChosenFileMethod) throws IOException {
        switch (sChosenFileMethod) {
            case 1:
                return InputOutput.readVectorsSeq("vecs-50-10000.txt");
            case 2:
                return InputOutput.readVectorsSeq("vecs-50-50000.txt");
            case 3:
                return InputOutput.readVectorsSeq("vecs-50-100000.txt");
            case 4:
                return InputOutput.readVectorsSeq("vecs-50-500000.txt");

            default:
                System.out.println("Invalid algorithm number. Using the smaller set");
                return InputOutput.readVectorsSeq("vecs-50-10000.txt");
        }
    }

    // Method to select K and K1 for the algorithms computation
    private static int[] IntegerSelection(ArrayList<Vector> vectorsArray)
    {
        int[] aiInputValues = new int[2];

        Scanner consoleInput = new Scanner(System.in);
        while (true)
        {
            System.out.println("Choose an integer k between 1 and " + vectorsArray.size() + " corresponding to the target number of clusters");
            int iK = consoleInput.nextInt();

            if (iK < 1 || iK > vectorsArray.size())
                System.out.println("Invalid number of clusters. Retry...");
            else
            {
                aiInputValues[0] = iK;
                Boolean bCorrectK1 = false;
                while (!bCorrectK1)
                {
                    System.out.println("Choose an integer k1 > k");
                    int iK1 = consoleInput.nextInt();
                    if (iK1 <= iK)
                        System.out.println("Invalid value for k1. Retry...");

                    else
                    {
                        aiInputValues[1] = iK1;
                        bCorrectK1 = true;
                    }
                }

                break;
            }
        }

        return aiInputValues;
    }
}