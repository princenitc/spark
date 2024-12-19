## Apache spark basics learning 

1. Map -> takes the input of streams and converts them into another RDD on applying the function given. i.e. [1,4,,9,16] on applying sqrt function will generate a new RDD [1,2,3,4].
2. Reduce -> this will apply the function on two values such to reduce them into a single RDD. i.e on applying sum reduce to above sqrt values it will have ans 10. 
3. PairRDD -> This is same as that of RDD but it will have the things in key, value pairs. Only difference between this and map in java collections is that in this we can have multiple keys which is not possible in java collections. 
4. Coalesce -> When you are working with very big data and have lots of data. There could be multiple partitions to work with initially. But after some transformation there could be scenario that there are very less records are present in each partition. And doing operations on these partitions will not be very performance efficient. This is why we use coelesce to increase the partition size.
5. 