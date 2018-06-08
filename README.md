# Big-Data-Computing
Here i push my homeworks about Big Data Computing.
I really want to thanks Edoardo and Edoardo Borsato, cause the help me out 
a lot and they always come with some new tips and a faster/efficent code.


## Homework 1
The purpose of this first homework is to set up the environment for developing Spark code on your machine and to get acquainted with the principles of functional programming, on which MapReduce and Spark are based.

### Machine setup for Java users
Before doing any work, you should setup your machine through the following steps.
First of all, you need to have the Java Development Kit (JDK) version 8 installed on your machine. If the command
javac -version
fails or returns something which is not along the lines of javac 1.8, then head to Oracle’s download page and download the Java Development Kit version 8. Version 9 has problems with Spark, so we should avoid it for the time being. If you are using Linux, you can instal the JDK 8 with your distibution’s package manager.

Download the project template available here. Unpack it somewhere on your filesystem.
Head over to the download page of Intellij Idea (Community edition) and install it on your system. In the Installation Options dialog window, select 32-bit or 64-bit launcher (depending on your machine) and specify Java in the Create Associations section. (For a more comprehensive guide you can look at the official install and set-up page.)
After installation is completed, you must configure Intellij for a first run. Launch Intellij. In first startup screen choose not to import any settings. In the second one (about user interface theme) choose "Skip Remaining and Set Defaults". Then, in the third screen select Import Project: use the file selection dialog that pops up to select the file build.gradle file contained in the directory you downloaded.


### Preliminaries
One of the core ideas of functional programming is that functions can be arguments to other functions. For instance, a function implementing a sorting algorithm may take as a parameter the comparison function along with the data to be sorted.
Java 8 introduced support for this style of programming by adding new syntax for specifying so-called anonymous functions, also called lambdas. This syntax allows to write functions directly in the argument list of other functions. The syntax for specifying a function is the following:

(T1 param1, T2 param2, ...) -> {
  // Body of the function
  // with as many statements as you need
  // separated by semicolons, just like regular
  // Java statements.
  return /* possibly something */;
}
Where T1, and T2 are the types of param1 and param2, respectively.

If the function is made by a single statement, a more concise syntax can be used:

(T1 param1, T2 param2) -> /* single statement with no semicolon */
the result of the single statement will be the return value of the function.

If the type of the parameters can be inferred from the context, it can be omitted.

An example will make things clearer. Imagine you have a collection coll of Double with a method map (more on such collections later). The map method transforms the collection in a new one by applying the function passed as a parameter to each element. Therefore, to obtain a collection of the squared values you should do the following:

coll.map((Double x) -> x*x);
Since the collection is of Double, the compiler can infer the type of x, so in this case we can write:

coll.map((x) -> x*x);
To make another example, imagine that you want to transform your collection of Double into a collection of differences from some other value, defined in a variable:

double fixed = 1.5;

coll.map((x) -> {
  double diff = fixed - x;
  return diff;
});
Note that fixed is used in the body of the anonymous function, but is defined outside of it! In such cases we say that the anonymous function captures a variable. You cannot re-assign a captured variable within an anonymous function. Trying to do it will result in a compilation error mentioning that all captured variables must be effectively final, which is the compiler’s way of saying that you cannot re-assign them.

Java 8 also introduced another way of passing functions to other functions, namely method references. Suppose you have the following class:

public class Operations {

  public static double square(double x, double y) {
    return x * y;
  }
}
You may pass the static method square to the method map instead of defining a lambda function, like in the examples above. The syntax to refer the static method square is the following:

coll.map(Operations::square);
note the double colon joining the method name square to the class it belongs to, Operations.

Therefore, you have two ways of passing a function to a method: either you pass an anonymous function or a method reference. Usually, lambda functions are used when the functionality can be coded in a few statements and is limited to a single occurrence. Method references, on the other hand, are useful when the code gets more complex or when it should be reused in several places.

So far, we have assumed the existence of a collection type providing a map method accepting a function as an argument. There are several types of collections providing such a method (including many from the Java Standard Library). However, since these homeworks are about Spark, we will focus on collections provided by Spark, namely Resilient Distributed Datasets (RDD for short). An RDD is a collection of elements that can be possibly partitioned across many machines and on which operations execute in parallel. In the Spark Java API, the class defining the RDD data structure is JavaRDD.

The peculiarity of the RDD data structure is that it does not allow in-place updates. The only way to modify the contents of an RDD is to transform it in another collection by means of some method. Some methods to transform an RDD into another are the following:

map: yields another RDD by applying the supplied function on each element
filter: returns an RDD containing only the elements for which the given boolean function returns true.
There are also functions to get a single value which is the result of some operation on the entire collection, which are called actions:

count: returns the number of elements of the RDD

collect: store all the data of the RDD in a local List.

reduce: returns the result of reducing the collection with the given commutative and associative function. Conceptually, this operation is equivalent to applying the function to the first two elements of the collections, then to the result and the third, then to the result and the fourth and so on, until there are no more values. No assumptions on the order of applications can be made, which is why the function needs to be associative and commutative. The following picture depicts an example for the addition function on the list 47 11 42 13

These are not all of the methods available to transform and collect data in Spark. The complete set of methods can be found in the official Spark Java API. Also, for the most useful methods, a more detailed guide prepared by dr. Ceccarello, can be found here.

Note that the methods of JavaRDD that we saw are all functional in nature (well, except count and collect): they accept another function as a parameter to know what to do with elements.

## Variable names

In the templates that we provide, we tend to use variable names with one-letter prefixes to distinguish variables representing local data and RDD data. Local data will be prefixed by l (e.g. lName) and distributed data by d (e.g. dName).

Now, open the file src/main/java/it/unipd/dei/bdc1718/FirstHomework.java in the project template. It contains: some setup code (most of which will be explained in the next homework); the reading of a file of doubles (dataset.txt) into a JavaRDD called dNumbers; and a short code that computes and prints the sum of the squares of the doubles. Run the program and check the result at the bottom of the Intellij window, which refers to the sample dataset.txt file provided together with the program. You can use FirstHomework.java as a template for the homework.

## Assignment (Java users)

After downloading the directory bdc1718 and setting up the machine create a Java program GxxHM1.java, where xx is two-digit group number, which does the following things:
Read an input a file dataset.txt of doubles into a JavaRDD dNumbers (as in the template FirstHomework.java)
Create a new JavaRDD dDiffavgs containing the absolute value of the difference between each element of dNumbers and the arithmetic mean of all values in dNumbers.
Compute and print the minimum value in dDiffavgs. Do it in two ways:
using the reduce method;
using the min method of the JavaRDD class passing to it a comparator as explained here.
Compute and print another statistics of your choice on the data in dNumbers. Make sure that you use at least one more method chosen among those of the JavaRDD interface. Have a look at the official Spark Java API and, for details about some methods, here.
Add short but explicative comments to your code and when you print a value print also a short description of what that value is. Return the file GxxHM1.java with your program by mail to bdc-course@dei.unipd.it
Assignment (Python users)
You have to learn how to use the Spark API with Python. Refer to the official Apache Spark's site and, in particular, to the Spark Overview and the Quick Start pages. Also, look at the "Note for Python users" on this page, at the end of the Machine setup section.
By the deadline, you must return the following files:


## Homework 2: MapReduce with Spark
In this second homework, we will look at Spark in more details and will learn how to implement MapReduce computations in Spark, using the classic word count problem as an example.

Spark context
Let us first look at the basic settings required in your program to use Spark, which were already present in the template provided for Homework 1. The entry point to Spark is the Spark context. Since Spark can run on your laptop as well as on many different cluster architectures, to simplify the user experience Spark developers have created a single entry point that handles all the gory details behind the scenes. In the Java API, the relevant class is JavaSparkContext. To instantiate such a class, you need to provide some configuration using the class SparkConf as follows:

SparkConf configuration =
  new SparkConf(true)
    .setAppName("application name here")
    .setMaster("<master>");
Let’s break down the code snippet above. On line 2, we pass true to the SparkConf constructor. The effect is that configuration properties will be read from system properties (i.e., the ones passed on the command line after the java command using the -Dproperty.name=property-value sintax). Line 3 sets the name of your application. Note that this line and the following one are method invocations on the SparkConf object being created. Finally, line 4 sets the address of the master. As detailed in the Spark documentation, there are several values that this string can take. For this course, two are interesting.

"local[*]": use the local resources of the computer. This sets up a Spark process on the local machine, using the available cores for parallelism. Use this setting when testing code on your local machine.
"yarn": run Spark on the Yarn cluster manager. This is the cluster manager used by the cloud computing platform available for the course. Use this setting when running on it.
There is also the possibility of not setting the master in the SparkConf object. In this case, you should specify the Spark master either using the Java property spark.master on the command line (for instance when running locally on your laptop), or by specifying the --master option of the spark-submit command (documentation). (In fact, this is the choice that we made in the template provided for Homework 1.) By not hardcoding the master configuration in you code, you have the flexibility of running on different architectures. If you are using the Intellij Idea IDE, you can configure the Spark master using the configuration dialog box that can be accessed from Run -> Edit configurations, as shown in the following figure, where the relevant configuration is specified in the VM options.

_images/configure-master.png
A run configuration is created for you the first time you try to run a main method by clicking on the green arrow beside the line of the main method itself.

Once you have created a SparkConf object, you can instantiate a JavaSparkContext through:

JavaSparkContext sc = new JavaSparkContext(configuration);
Now we are ready to use this Spark context to load data.

Reading from a file. In the first homework, we built an RDD by calling sc.parallelize on an existing collection. Alternatively, the following line of code can be used to load a text file (filename.txt) into an RDD of strings, where each string corresponds to a distinct line of the file:

JavaRDD<String> lines = sc.textFile("filename.txt");
The string "filename.txt" can be substituted with args[0], if the name of the file is passed as the first parameter on the command line. Note that by passing to textFile a directory name rather than a file name, Spark will load all files found in the directory into the RDD.

## Profiling
Time measurements. In Java programs, measuring time can be done thorugh the System.currentTimeMillis() method. In Spark programs, however, the use of this method requires some care due to the fact that transformations are lazy, in the sense that they are executed only once an action (such as counting the elements or writing them to a file) requires the transformed data. Suppose we want to process the text file filename.txt. If you do the following:

JavaRDD<String> docs = sc.textFile("filename.txt");
long start = System.currentTimeMillis();
// Code of which we want to measure the running time
long end = System.currentTimeMillis();
System.out.println("Elapsed time " + (end - start) + " ms");
then you would be measuring also the time to load the text file! Indeed, sc.textFile is not executed immediately, rather it is executed when an action requires it, after the start of the stopwatch. Therefore, you need to force the loading of the file to happen before the stopwatch is started. In order to do so, you can run an action on the docs RDD, and the simplest one is count(). However, simply invoking count would not do: we have to explicitly tell Spark to cache the results in memory:

JavaRDD<String> docs = sc.textFile("filename.txt").cache();
docs.count();

// Now the RDD has been loaded and cached in memory and
// we can start measuring time
long start = System.currentTimeMillis();

// Code of which we want to measure the running time

long end = System.currentTimeMillis();
System.out.println("Elapsed time " + (end - start) + " ms");
There are several alternatives for caching an RDD in memory. They are described here.
Web interface The above strategy is good to take the overall running time of a section of the program, but it is inadequate for finer grained profiling. To see how much time your Spark program spends running each transformation and action, you can use the web interface that is built-in into Spark. This interface runs alongside your program, and exits when the program terminates. In order to have time to consult it, we have to suspend the execution of the program. The simplest way is by inserting an input statement right before the end of your main method:

System.out.println("Press enter to finish");
System.in.read();
Now, when your program reaches the input statement, open a browser and visit localhost:4040. You will see the web interface of your running program, which you are encouraged to explore.

## MapReduce with Spark
Key-value pairs. In Java, a dataset of key-value pairs with keys of type K and values of type V is implemented through a JavaPairRDD<K,V> object, which is an RDD whose elements are instances of the class Tuple2<K,V>.

For Python users: a dataset of key-value pairs can be implemented as a simple RDD whose elements are key-value pairs. All transformations can be applied, but some transformations require that the elements of the RDD be key-value pairs.

Map phase. In order to implement a map phase where each key-value pair, individually, is transformed into 0, 1 or more key-value pairs, the following methods can be invoked from a JavaPairRDD<K,V> object X:

mapToPair. It applies a function f passed as a parameter to each individual key-value pair of X, transforming it into a key-value pair of type Tuple2<K',V'> (with arbitrary K' and V'). Hence, X.mapToPair(f) returns a JavaPairRDD<K',V'> object. (The method can also be invoked from a JavaRDD<T> object.)
flatMapToPair. It applies a function f passed as a parameter to each individual key-value pair of X, transforming it into 0, 1 or more key-value pairs of type Tuple2<K',V'> (with arbitrary K' and V'), which are returned as an iterator. Hence, X.flatMapToPair(f) returns a JavaPairRDD<K',V'> object. (The method can also be invoked from a JavaRDD<T> object.)
mapValues. It transforms each key-value pair (k,v) in X into a key-value pair (k,v'=f(v)) of type Tuple2<K,V'> (with arbitrary V') where f is a function passed as a parameter. Hence, X.mapValues(f) returns a JavaPairRDD<K,V'> object.
flatMapValues. It transforms each key-value pair (k,v) in X into multiple key-value pairs (k,w_1), (k,w_2) , ... of type Tuple2<K,V'> (with arbitrary V'). The w_i's are returned as an Iterable<V'> by f(v), where f is a function passed as a parameter. Hence, X.flatMapValues(f) returns a JavaPairRDD<K,V'> object.
Reduce phase. In order to implement a reduce phase where each set of key-value pairs with the same key are transformed into a set of 0, 1 or more key-value pairs, the following methods can be invoked from a JavaPairRDD<K,V> object X (read details here):

groupByKey. For each key k occurring in X, it creates a key-value pair (k,w) where w is an Iterable<V> containing all values of the key-value pairs with key k in X. Hence, X.groupByKey() returns a JavaPairRDD<K,Iterable<V> object. The reduce phase of MapReduce can be implemented by applying flatMapToPair after groupByKey.
reduceByKey. For each key k occurring in X, it creates a key-value pair (k,v) where v is obtained by applying an commutative and associative function f passed as a parameter (e.g., (x,y)->x+y) to all values of the key-value pairs with key k in X. Hence, X.reduceByKey(f) returns a JavaPairRDD<K,V> object.
For Python users: all of the above methods have a Python equivalent with the same name, except for mapToPair and flatMapToPair which, in Python, become map and flatMap.

For more details on the classes and the methods mentioned above refer to the official RDD Programming guide and, for Java users, to the JavaPairRDD api and to this guide.

Partitioning. RDD is subdivided into a configurable number of partitions, which may be distributed across many machines. In order to implement a transformation acting on each individual element of an RDD (e.g., thorugh the map method), Spark defines a number of tasks equal to the number of partitions. Each task corresponds to the application of the given function to the elements of a distinct partition. Also, in Spark each machine is called an executor, and may have many cores. Each core of each executor will be assigned a task to execute. In principle, with a higher number of partitions a higher level of parallelism is achievable and a smaller local space is required by each task. Note that the number of partitions and the total number of available cores may differ: if there are more cores than partitions, then some cores will be idle, while if there are more partitions than cores, some tasks will wait for others to finish. Sometimes having many more partitions than cores yields better load balancing, hence better performance: cores completing faster tasks will have more work assigned.

The number of partitions, say num-part, can be set by invoking X.repartition(num-part) from an RDD X. Also, it can be passed as input to the textFile method described above (e.g., JavaRDD<String> docs = sc.textFile("filename.txt",num-part), but in this latter case it is regarded as a "minimum" number of partitions.

## Counting words
Let us see how to implement the MapReduce word count in Spark. First download the sample file text-sample.txt and place it in the root directory of your code. This file, obtained from a recent dump of Wikipedia, contains 10122 documents (one document per line) with 3503570 word occurrences overall.

Suppose that the file has been loaded into a JavaRDD<String> docs, where each element corresponds to a document which is a string of space-separated words. The following code implements the straightforward MapReduce word count algorithm and stores the words and their counts into a JavaPairRDD<String,Long> object wordcounts.

JavaPairRDD<String, Long> wordcounts = docs
  .flatMapToPair((document) -> {             // <-- Map phase
    String[] tokens = document.split(" ");
    ArrayList<Tuple2<String, Long>> pairs = new ArrayList<>();
    for (String token : tokens) {
      pairs.add(new Tuple2<>(token, 1L));
    }
    return pairs.iterator();
  })
  .groupByKey()                       // <-- Reduce phase
  .mapValues((it) -> {
    long sum = 0;
    for (long c : it) {
      sum += c;
    }
    return sum;
  });  
A few observations are needed.

In the map phase, which is implemented through the flatMapToPair method, the method split of String is used to split a document into its constituent words.
The function passed as a parameter to mapValues takes as input (it) the value of a key-value pair in the RDD returned by groupByKey. In this example, it is a collection of type Iterable<Long> (in fact, it will be a sequence of 1's). The for (long c : it) cycle is an example of the Java for-each loop, which iterates over all values contained in the collection it, successively assigned to variable c.
Assignment
Create a program GxxHM2.java (for Java users) or GxxHM2.py (for Python users), where xx is your two-digit group number, which receives in input a collection of documents, represented as a text file (one line per document) whose name is provided on the command line, and does the following things:

Runs 3 versions of MapReduce word count and returns their individual running times, carefully measured:
a version that implements the Improved Word count 1 described in class.
a version that implements the Improved Word count 2 described in class.
a version that uses the reduceByKey method.
Try to make each version as fast as possible. You can test it on the text-sample.txt file you downloaded earlier or even on a much larger file you can create yourself.
Asks the user to input an integer k and returns the k most frequent words (i.e., those with largest counts), with ties broken arbitrarily.
Add short but explicative comments to your code and when you print a value print also a short description of what that value is.


# Homework 3: k-center vs k-means++
The third homework pursues the following objectives: (1) develop and efficient sequential implementation of the Farthest-First Traversal algorithm for the k-center problem (which will turn out useful also for the last homework); (2) check whether k-means++, which provides a good initialization for the Lloyd's algorithm, can be executed on a coreset extracted through k-center, rather than on the whole dataset, without sacrificing the quality of its output too much. For reviewing the algorithms look at the slides on Clustering, Part 1 (slides 30-31) and (Part 2) (slides 14-16).

## Datasets
For this homework we will work on points in Euclidean space represented by vectors of reals (double, in Java). Download this zip file containing 4 datasets of various sizes which you can use for testing your program. The datasets are made of points in 50-dimension Euclidean space, which are vectorized representations of pages sampled from a recent dump of Wikipedia. The datasets are:

vecs-50-10000.txt: 9960 points
vecs-50-50000.txt: 50047 points
vecs-50-100000.txt: 99670 points
vecs-50-500000.txt: 499950 points
In Spark, the points can be represented as instances of the class org.apache.spark.mllib.linalg.Vector and can be manipulated through static methods offered by the class org.apache.spark.mllib.linalg.Vectors (these classes are available both for Java and Python). For example, method Vectors.dense(x) transforms an array x of double into an instance of class Vector, while method Vectors.sqdist(a,b) computes the squared L2-distance between two instances a and b of class Vector. For Java users, you can download the class InputOutput, where you find a method InputOutput.readVectorsSeq that, given in input the name (or path) of a text file containing points in Euclidean space (one point per line with coordinates separated by space, as in the files above) transforms it into a java.util.ArrayList<Vector>.

## Warning
In Spark, there is also a class Vector in the org.apache.spark.ml package, which is functionally equivalent, but incompatible with org.apache.spark.mllib.linalg.Vector. This unfortunate difference is due to the history of Spark's API. For the homeworks we will use classes from the org.apache.spark.mllib package.

## Assignment (Java users)
For this homework you need not RDDs! You must develop 3 methods.

A method kcenter(P,k) that receives in input a set of points P and an integer k, and returns the set C of k centers computed by the Farthest-First Traversal algorithm. Both P and C must be represented as instances of java.util.ArrayList<Vector>.
A method kmeansPP(P,WP,k) that receives in input a set of points P, a set of weigths WP for P, and an integer k, and returns a set C of k centers computed with a weighted variant of the kmeans++ algorithm where, in each iteration, the probability for a non-center point p of being chosen as next center is
w_p*(d_p)^2/(sum_{q non center} w_q*(d_q)^2)

where d_p is the distance of p from the closest among the already selected centers and w_p is the weight of p. WP must be represented as instance of java.util.ArrayList<Long>, where the i-th element of WP is the weight of the i-th element of P.

A method kmeansObj(P,C) that receives in input a set of points P and a set of centers C, and returns the average squared distance of a point of P from its closest center (i.e., the kmeans objective function for P with centers C, divided by the number of points of P).
Make sure that kcenter(P,k) and kmeansPP(P,WP,k) run in time O(|P|*k)

Finally, you must create a program GxxHM3.java, where xx is your two-digit group number, which receives in input a set P of points in Euclidean space (provided as a text file as the above datasets), and 2 integers k, k1, with k < k1. The program incorporates the methods developed above and does the following:

Runs kcenter(P,k) printing its running time.
Runs kmeansPP(P,WP,k) with all weights in WP equal to 1, to obtain a set of k centers C, and then runs kmeansObj(P,C) printing the returned value
Runs kcenter(P,k1) to obtain a set of k1 centers X; then runs kmeansPP(X,WX,k) to obtain a set of k centers C, and finally runs kmeansObj(P,C) printing the returned value. Here the idea is to test whether k1>k centers extracted with the kcenter primitive can provide a good coreset on which running kmeans++. Of course, the larger k1 and the better the set of centers computed by kmeansPP(X,WX,k). But you can also play with the weights W(X). The easiest thing to do is to set all weights equal to 1. But if you feel adventurous, you can explore other avenues.


