package it.unipd.dei.bdc1718;

import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Random;
import java.util.Scanner;

public class G21HM3booble
{
    public static void main(String[] args) throws IOException
    {
        ArrayList<Vector> vectorsSetP = FileSelection(args);
        if (vectorsSetP.size() == 0)
            return;
        // iK and iK1 selection
        int[] iClustersNumberK = IntegerSelection(vectorsSetP);

        int iDataSetSize = vectorsSetP.size();

        // default case for the kmeans++ algorithm
        ArrayList<Long> firstWeightsArray = new ArrayList<Long>();
        for (int j = 0; j < iDataSetSize; j++)
            firstWeightsArray.add(j, 1L);

        System.out.println("******************* Computation for k selected: " + iClustersNumberK[0] + " *******************");

        // Kcenter -> using P, k
        long lStart = System.currentTimeMillis();
        ArrayList<Vector> kCenterSetC = KCenterAlgorithm(vectorsSetP, iClustersNumberK[0]);
        long lEnd = System.currentTimeMillis();
        System.out.println("... Elpased time [ms]: " + (lEnd - lStart) + "\n");

        System.out.println("The k centers of the clusters using kcenter are:");
        for (int i = 0; i < iClustersNumberK[0]; i++)
            System.out.println(kCenterSetC.get(i));
        //////////

        // KMeans++ -> using P, k
        System.out.println();
        lStart = System.currentTimeMillis();
        ArrayList<Vector> kMeansCentersSetC = KMeansPPAlgorithm(vectorsSetP, firstWeightsArray, iClustersNumberK[0]);
        lEnd = System.currentTimeMillis();
        System.out.println("... Elpased time [ms]: " + (lEnd - lStart) + "\n");

        System.out.println("The k centers of the clusters using kmeans++ are:");
        for (int l = 0; l < iClustersNumberK[0]; l++)
            System.out.println(kMeansCentersSetC.get(l));
        //////////

        // KmeansObj
        System.out.println();
        lStart = System.currentTimeMillis();
        double dMeanSquaredDistFromClosestCenter = KMeansObjAlgorithm(vectorsSetP, kMeansCentersSetC);
        lEnd = System.currentTimeMillis();
        System.out.println("... Elpased time [ms]: " + (lEnd - lStart) + "\n");

        System.out.println("The average squared distance from the closest center is: " + dMeanSquaredDistFromClosestCenter);
        /////////
        System.out.println("***************************************************************************\n");

        System.out.println("******************* Computation for k1 selected: " + iClustersNumberK[1] + " *******************");

        // Kcenter -> using P, k1
        lStart = System.currentTimeMillis();
        kCenterSetC = KCenterAlgorithm(vectorsSetP, iClustersNumberK[1]);
        lEnd = System.currentTimeMillis();
        System.out.println("... Elpased time [ms]: " + (lEnd - lStart) + "\n");

        System.out.println("The k centers of the clusters using kcenter are:");
        for (int i = 0; i < iClustersNumberK[1]; i++)
            System.out.println(kCenterSetC.get(i));
        //////////

        // default case with all weights equal to 1
        int iCenterSetSize = kCenterSetC.size();
        ArrayList<Long> secondWeightsArray = new ArrayList<Long>();
        for (int j = 0; j < iCenterSetSize; j++)
            secondWeightsArray.add(j, 1L);

        // KMeans++ -> using C, k
        System.out.println();
        lStart = System.currentTimeMillis();
        kMeansCentersSetC = KMeansPPAlgorithm(kCenterSetC, secondWeightsArray, iClustersNumberK[0]);
        lEnd = System.currentTimeMillis();
        System.out.println("... Elpased time [ms]: " + (lEnd - lStart) + "\n");

        System.out.println("The k centers of the clusters using kmeans++ are:");
        for (int l = 0; l < iClustersNumberK[0]; l++)
            System.out.println(kMeansCentersSetC.get(l));
        //////////

        System.out.println("***************************************************************************\n");
    }

    // Method to select the file to use for the analysis
    public static ArrayList<Vector> FileSelection(String[] sFileNames) throws IOException
    {
        ArrayList<Vector> vectorsArray = new ArrayList<Vector>();
        Scanner consoleInput = new Scanner(System.in);
        Boolean bCorrectFile = false;

        if (sFileNames.length == 0)
        {
            System.out.println("No input file. Exiting...");
            return vectorsArray;
        }

        while (!bCorrectFile)
        {
            System.out.println("Choose the file to use for the analysis by entering the corresponding number:");

            for (int i = 0; i < sFileNames.length; i++)
                System.out.println(i + ") " + sFileNames[i]);

            System.out.println("Press 9 to exit instead");

            int iFileChosen = consoleInput.nextInt();

            switch (iFileChosen)
            {
                case 0:
                    vectorsArray = InputOutput.readVectorsSeq(sFileNames[0]);
                    System.out.println(sFileNames[0] + " chose\n");
                    bCorrectFile = true;
                    break;

                case 1:
                    vectorsArray = InputOutput.readVectorsSeq(sFileNames[1]);
                    System.out.println(sFileNames[1] + " chose\n");
                    bCorrectFile = true;
                    break;

                case 2:
                    vectorsArray = InputOutput.readVectorsSeq(sFileNames[2]);
                    System.out.println(sFileNames[2] + " chose\n");
                    bCorrectFile = true;
                    break;

                case 3:
                    vectorsArray = InputOutput.readVectorsSeq(sFileNames[3]);
                    System.out.println(sFileNames[3] + " chose\n");
                    bCorrectFile = true;
                    break;

                case 9:
                    System.out.println("Exiting...");
                    bCorrectFile = true;
                    break;

                default:
                    System.out.println("Invalid selection. Retry...");
                    break;
            }
        }

        return vectorsArray;
    }

    // Method to select K and K1 for the algorithms computation
    public static int[] IntegerSelection(ArrayList<Vector> vectorsArray)
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

    // kcenter
    public static ArrayList<Vector> KCenterAlgorithm(ArrayList<Vector> vectorsArray, int iKCenters)
    {
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

        for (int i = 1; i < iKCenters; i++)
        {
            int iFarthestVectorIndex = -1;
            double dMaxDistFromClustersCenters = Double.MIN_VALUE;
            Vector lastAddKCenterVector = vectorsSetC.get(i - 1);

            for (int j = 0; j < iCompleteDataSetSize; j++)
            {
                // If the vector has been "removed" from P, because it was add to C, then continue
                if (bVectorRemoved[j])
                    continue;

                Vector currentVector = vectorsArray.get(j);
                double dCurrentDist = Vectors.sqdist(currentVector, lastAddKCenterVector);
                // for all P - C points update their min distances from the k centers
                if (dCurrentDist < dMinDist[j])
                        dMinDist[j] = dCurrentDist;

                // find the max distance in the set of the min distances
                if (dMinDist[j] > dMaxDistFromClustersCenters)
                {
                    dMaxDistFromClustersCenters = dMinDist[j];
                    iFarthestVectorIndex = j;
                }
            }

            // Add the farthest vector from the set of k centers to C; "remove" it from the initial data set P
            vectorsSetC.add(i, vectorsArray.get(iFarthestVectorIndex));
            bVectorRemoved[iFarthestVectorIndex] = true;
        }

        return vectorsSetC;
    }

    // kmeans++
    public static ArrayList<Vector> KMeansPPAlgorithm(ArrayList<Vector> vectorsArray, ArrayList<Long> weights, int iKCenters)
    {
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

        for (int i = 1; i < iKCenters; i++)
        {
            double dSumOfSquaredMinDist = 0.0;
            double[] dProbabilities = new double[iCompleteDataSetSize];
            Arrays.fill(dProbabilities, 0.0);
            double dMaxProbability = 0.0;
            int iMoreProbableVectorIndex = -1;
            Vector lastAddKCenterVector = vectorsSetC.get(i - 1);
            for (int j = 0; j < iCompleteDataSetSize; j++)
            {
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
            for (int k = 0; k < iCompleteDataSetSize; k++)
            {
                if (bVectorRemoved[k])
                    continue;

                dProbabilities[k] = dMinDist[k] * weights.get(k) / dSumOfSquaredMinDist;
                if (dProbabilities[k] > dMaxProbability)
                {
                    dMaxProbability = dProbabilities[k];
                    iMoreProbableVectorIndex = k;
                }
            }

            // Add the more probable vector to C; "remove" it from the initial data set P
            vectorsSetC.add(i, vectorsArray.get(iMoreProbableVectorIndex));
            bVectorRemoved[iMoreProbableVectorIndex] = true;
        }

        return vectorsSetC;
    }

    // kmeansObj
    public static double KMeansObjAlgorithm(ArrayList<Vector> completeDataSetP, ArrayList<Vector> centersDataSetC)
    {
        System.out.println("Applying kmeansObj algorithm...");
        ArrayList<Vector> dataSet = new ArrayList<>(completeDataSetP);
        int iCompleteDataSetSize = completeDataSetP.size();
        int iCentersDataSetSize = centersDataSetC.size();

        // If the two datasets are the same
        if (iCompleteDataSetSize == iCentersDataSetSize)
            return 0.0;

        double dSumOfSquaredMinDist = 0.0;

        // Update the new dataset without the center of the clusters
        for (int i = 0; i < iCentersDataSetSize; i++)
        {
            Vector currCenterVector = centersDataSetC.get(i);
            if (dataSet.contains(currCenterVector))
                dataSet.remove(dataSet.indexOf(currCenterVector));
        }

        int iDatasetSize = dataSet.size();
        double[] dMinDist = new double[iDatasetSize];
        Arrays.fill(dMinDist, Double.MAX_VALUE);

        // Compute the distances from the closest center for all P - C points
        for (int i = 0; i < iCentersDataSetSize; i++)
        {
            for (int j = 0; j < iDatasetSize; j++)
            {
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
    }
}