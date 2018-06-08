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
