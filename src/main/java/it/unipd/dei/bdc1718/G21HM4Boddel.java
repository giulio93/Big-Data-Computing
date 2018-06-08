package it.unipd.dei.bdc1718;

import com.google.common.collect.Lists;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.rdd.RDD;
import scala.Tuple2;

import javax.swing.plaf.synth.SynthScrollBarUI;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.Serializable;
import java.security.InvalidParameterException;
import java.util.*;
import java.io.IOException;

public class G21HM4Boddel
{
    public static void main(String[] args) throws IOException
    {
        // Setup Spark
        SparkConf conf = new SparkConf(true)
                .setAppName("Homework 4");
        JavaSparkContext sc = new JavaSparkContext(conf);

        int iK = Integer.parseInt(args[0]);
        int iNumBlocks = Integer.parseInt(args[1]);
        /*
        String fileChosen = FileSelection(args);
        int [] aiInputValues = IntegerSelection();
        int iNumBlocks = aiInputValues[0];
        int iK = aiInputValues[1];
        */
        JavaRDD<Vector> pointsrdd = InputOutput.readVectors(sc, args[2]).repartition(iNumBlocks).cache();
        long start = System.currentTimeMillis();
        ArrayList<Vector> solutionpoints = runMapReduce(pointsrdd, iK, iNumBlocks);


        double dAverageDistance = measure(solutionpoints);
        long end = System.currentTimeMillis();

        System.out.println("Time elapsed for computing the average distance: " + (end - start) + " [ms]");
        System.out.println("The average distance is: " + dAverageDistance);
    }

    public static String FileSelection(String[] sFileNames) throws IOException
    {
        String fileChosen = "";
        JavaRDD<Vector> vectorsRDD = null;
        Scanner consoleInput = new Scanner(System.in);
        Boolean bCorrectFile = false;

        if (sFileNames.length == 0)
        {
            System.out.println("No input file. Exiting...");
            return fileChosen;
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
                    fileChosen = sFileNames[0];
                    System.out.println(sFileNames[0] + " chose\n");
                    bCorrectFile = true;
                    break;

                case 1:
                    fileChosen = sFileNames[1];
                    System.out.println(sFileNames[1] + " chose\n");
                    bCorrectFile = true;
                    break;

                case 2:
                    fileChosen = sFileNames[2];
                    System.out.println(sFileNames[2] + " chose\n");
                    bCorrectFile = true;
                    break;

                case 3:
                    fileChosen = sFileNames[3];
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

        return  fileChosen;
    }

    public static int[] IntegerSelection()
    {
        int[] aiInputValues = new int[2];

        Scanner consoleInput = new Scanner(System.in);
        while (true)
        {
            System.out.println("Choose a positive integer 'numBlocks' used to partition into subsets the original dataset");
            int iNumBlocks = consoleInput.nextInt();

            if (iNumBlocks < 0)
                System.out.println("Invalid negative value for numBlocks. Retry...");

            else
            {
                aiInputValues[0] = iNumBlocks;
                Boolean bCorrectK = false;
                while (!bCorrectK)
                {
                    System.out.println("Choose an integer k used to extract the points");
                    int iK = consoleInput.nextInt();
                    if (iK < 0)
                        System.out.println("Invalid negative value for k. Retry...");

                    else
                    {
                        aiInputValues[1] = iK;
                        bCorrectK = true;
                    }
                }

                break;
            }
        }

        return aiInputValues;
    }

    public static double measure(ArrayList<Vector> pointsList)
    {
        double averageDist = 0;
        double sumOfDistances = 0;
        int iDataSetSize = pointsList.size();
        int iDistinctPairsNumber = (iDataSetSize * (iDataSetSize - 1)) / 2;

        if (iDistinctPairsNumber == 0)
        {
            System.out.println("There are no distinct pairs... returning 0");
            return averageDist;
        }
        for (int i = 0; i < iDataSetSize; i++)
        {
            for (int j = 0; j < iDataSetSize; j++)
            {
                if (j > i)
                {
                    sumOfDistances += Math.sqrt(Vectors.sqdist(pointsList.get(i), pointsList.get(j)));
                }
            }
        }

        averageDist = sumOfDistances / iDistinctPairsNumber;

        return averageDist;
    }

    // runmapReduce method
    public static ArrayList<Vector> runMapReduce(JavaRDD<Vector> pointsrdd, int iK, int iNumBlocks)
    {
        long start = System.currentTimeMillis();
        JavaRDD<Vector> kCenterRDD = pointsrdd.repartition(iNumBlocks).mapPartitions((set) ->
        {
            ArrayList<Vector> subset = new ArrayList<>();
            while(set.hasNext())
                subset.add(set.next());

            if (iK < 1 || iK > subset.size())
            {
                System.out.println("Invalid number of clusters... Returning the total subset");
                return set;
            }

            ArrayList<Vector> kCenterSet = G21HM3.kCenter(subset, iK);
            return kCenterSet.iterator();
        });

        ArrayList<Vector> coreset = new ArrayList<>(kCenterRDD.collect());
        long end = System.currentTimeMillis();
        System.out.println("Time elapsed for coreset construction: " + (end - start) + " [ms]");
        System.out.println("Coreset dimension is: "+ coreset.size());

        start = System.currentTimeMillis();
        ArrayList<Vector> maxDistancePoints = runSequential(coreset, iK);
        end = System.currentTimeMillis();
        System.out.println("Time elapsed for runSequential computation: " + (end - start) + " [ms]");

        return maxDistancePoints;
    }

    /**
     * Sequential approximation algorithm based on matching.
     */
    public static ArrayList<Vector> runSequential(final ArrayList<Vector> points, int k)
    {
        final int n = points.size();
        if (k >= n)
        {
            return points;
        }

        ArrayList<Vector> result = new ArrayList<>(k);
        boolean[] candidates = new boolean[n];
        Arrays.fill(candidates, true);
        for (int iter = 0; iter < k/2; iter++)
        {
            // Find the maximum distance pair among the candidates
            double maxDist = 0;
            int maxI = 0;
            int maxJ = 0;
            for (int i = 0; i < n; i++)
            {
                if (candidates[i])
                {
                    for (int j = i+1; j < n; j++)
                    {
                        if (candidates[j])
                        {
                            double d = Math.sqrt(Vectors.sqdist(points.get(i), points.get(j)));
                            if (d > maxDist)
                            {
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
        if (k % 2 != 0)
        {
            for (int i = 0; i < n; i++)
            {
                if (candidates[i])
                {
                    result.add(points.get(i));
                    break;
                }
            }
        }

        if (result.size() != k)
        {
            throw new IllegalStateException("Result of the wrong size");
        }
        return result;
    }


}