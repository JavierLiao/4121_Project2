# README
**Question 1**. (4 points) What is the default block size on HDFS? What is the default replication factor of HDFS on Dataproc?
default blocksize: 128mb
default replication factor: 


**Question 2**. (2 points) Use enwiki_test.xml as input and run the program locally on a Single Node cluster using 4 cores. Include your screenshot of the dataproc job. What is the completion time of the task?

<img width="1440" alt="image" src="https://user-images.githubusercontent.com/90008408/164950057-f13001ce-73ab-4431-afa1-b4614f6b0bc2.png">

The completion time is *5mins 22secs*

**Question 3.** (2 points) Use enwiki_test.xml as input and run the program under HDFS inside a 3 node cluster (2 worker nodes). Include your screenshot of the dataproc job. Is the performance getting better or worse in terms of completion time? Briefly explain.

<img width="1440" alt="image" src="https://user-images.githubusercontent.com/90008408/164950084-81e95f09-f801-4454-bd2a-5596e1e0136f.png">

The performance gets better as the completion time shortens to *3mins 20 secs*. 


**Question 4.**(2 points) For this question, change the default block size in HDFS to be 64MB and repeat Question 3. Include your screenshot of the dataproc job. Record run time, is the performance getting better or worse in terms of completion time? Briefly explain.

<img width="1440" alt="image" src="https://user-images.githubusercontent.com/90008408/164950106-4930e381-8fd0-4534-81ce-1155dde242b8.png">

**Question 5.** (2 points) Use enwiki_whole.xml as input and run the program under HDFS inside the Spark cluster you deployed. Record the completion time. Now, kill one of the worker nodes immediately. You could kill one of the worker nodes by go to the *VM Instances* tab on the Cluster details page and click on the name of one of the workers. Then click on the STOP button. Record the completion time. Does the job still finish? Do you observe any difference in the completion time? Briefly explain your observations. Include your screenshot of the dataproc jobs.

- Not kill
<img width="1440" alt="image" src="https://user-images.githubusercontent.com/90008408/164948647-bbbed67e-785e-44cd-a0ea-944f3e023241.png">
- Kill
<img width="1440" alt="image" src="https://user-images.githubusercontent.com/90008408/164956333-b98a010a-0cf4-43d3-a372-f52006c94c3d.png">


**Question 6.** (2 points) Only for this question, change the replication factor of enwiki_whole.xml to 1 and repeat Question 5 without killing one of the worker nodes. Include your screenshot of the dataproc job. Do you observe any difference in the completion time? Briefly explain.

**Question 7.** (2 points) Only for this question, change the default block size in HDFS to be 64MB and repeat Question 5 without killing one of the worker nodes. Record run time, include your screenshot of the dataproc job. Is the performance getting better or worse in terms of completion time? Briefly explain.




