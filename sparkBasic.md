# Apache spark basics learning 

1. **Map** -> takes the input of streams and converts them into another RDD on applying the function given. i.e. [1,4,,9,16] on applying sqrt function will generate a new RDD [1,2,3,4].
2. **Reduce** -> this will apply the function on two values such to reduce them into a single RDD. i.e on applying sum reduce to above sqrt values it will have ans 10. 
3. **PairRDD** -> This is same as that of RDD but it will have  data in  key value pairs. Only difference between this and map  is that in this we can have multiple keys which is not possible in java collections. 
4. **Coalesce** -> When you are working with very big data and have lots of data. There could be multiple partitions to work with initially. But after some transformation there could be scenario that there are very less records are present in each partition. And doing operations on these partitions will not be very performance efficient. This is why we use coelesce to increase the partition size.
5. **Join** -> It is the inner Join which only takes the row RDD which has matching columns.
6. **LeftOuterJoin** -> Joins two RDDs, keeping all the rows from left RDD and picking up the matching values from right RDD.
7. **RightOuterJoin** -> Joins two RDDs, keeping all the rows from right RDD and picking up matching values form the left RDD. 


# Performance of Spark 

- **Difference b/w Transformation and Actions:** 
  - At Runtime spark doesn't builds the RDD during transformation. It only creates the action plan which it needs to perform. It does the calculation whenever java related things are called for i.e. actions.
- **DAG and SparkUI:**
  - WebUI gives the full view of Execution plan like how is exactly execution plan(DAG -> Directed Acyclic Graph) is settled-up. 
  - To Check the webUI in local run we can insert a input scanner at last of out code goto localhost:4040. There we can see the execution plans and executions of out spark code.(This is not good way to Trace spark execution ideally this should have been done in server implementation.)
  - Jobs in UI -> these are the actions which are defined in the spark implementation
- **Detailed Narrow and Wide Transformation:**
  - Partition: it is a chunk of data to be processed.
  - Block of code executing against a partition is called as tasks.
  - Spark can implement transformations without moving any of the data around. For this reason this is called as Narrow Transformation e.g. Filter,mapToPair Transformation.
  - Wide Transformation are the transformations where spark needs to perform some kind of shuffling in order to achieve a transformation. e.g. groupByKey Transformation.
  - If wide transformation is necessary in your implementation always try to do it as much as later in the code as possible.
  - In General Sense, Stage is a series of Transformation that don't need to shuffle. When ever a shuffle is required spark creates a new stage. 
- **Partition and Key Skew:**
  - Salting is the way to artificially spread data into different partition.
  - Always try to reduce the key skew as much as possible.
  - **skew key** -> scenario where all the keys are stored in a single partition.
  - When we are using groupByKey instead of groupBy then in this scenario first reduce will happen. and then shuffle happens which impacts in large way in big data. This is called as Map Side Reduce.
- **Caching and Persistence:** 
  - Every time an action is performed spark goes to last written RDD results and uses that result for next action.
  - We can call cache method on results to have results in memory. It will be store in an object.
  - For same place also we can use persist instead of cache.