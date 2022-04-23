# README
**Question 1**. (4 points) What is the default block size on HDFS? What is the default replication factor of HDFS on Dataproc?



**Question 2**. (2 points) Use enwiki_test.xml as input and run the program locally on a Single Node cluster using 4 cores. Include your screenshot of the dataproc job. What is the completion time of the task?

![902E9E94-B7B7-4AE0-8770-764502CE8EFA](https://user-images.githubusercontent.com/90008408/164945930-6a4e3df4-f201-48b3-9b2c-28a248b00b77.png)

The completion time is *5mins 22secs*

**Question 3.** (2 points) Use enwiki_test.xml as input and run the program under HDFS inside a 3 node cluster (2 worker nodes). Include your screenshot of the dataproc job. Is the performance getting better or worse in terms of completion time? Briefly explain.

![133DEEF2-E030-4165-AD96-07165B3B5D1B](https://user-images.githubusercontent.com/90008408/164945933-dc2d5a6e-6226-4f96-ab0d-6ba8f8364589.png)

The performance gets better as the completion time shortens to *3mins 20 secs*. 


**Question 4.**(2 points) For this question, change the default block size in HDFS to be 64MB and repeat Question 3. Include your screenshot of the dataproc job. Record run time, is the performance getting better or worse in terms of completion time? Briefly explain.

![2B46B17D-CE19-4075-96B6-3DFF1248382D](https://user-images.githubusercontent.com/90008408/164945936-833c72d5-9950-4cf1-93ac-49aa3cf6cb39.png)


**Question 5.** (2 points) Use enwiki_whole.xml as input and run the program under HDFS inside the Spark cluster you deployed. Record the completion time. Now, kill one of the worker nodes immediately. You could kill one of the worker nodes by go to the *VM Instances* tab on the Cluster details page and click on the name of one of the workers. Then click on the STOP button. Record the completion time. Does the job still finish? Do you observe any difference in the completion time? Briefly explain your observations. Include your screenshot of the dataproc jobs.



**Question 6.** (2 points) Only for this question, change the replication factor of enwiki_whole.xml to 1 and repeat Question 5 without killing one of the worker nodes. Include your screenshot of the dataproc job. Do you observe any difference in the completion time? Briefly explain.

**Question 7.** (2 points) Only for this question, change the default block size in HDFS to be 64MB and repeat Question 5 without killing one of the worker nodes. Record run time, include your screenshot of the dataproc job. Is the performance getting better or worse in terms of completion time? Briefly explain.




