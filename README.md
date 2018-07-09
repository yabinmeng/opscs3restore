# 1. Problem Overview

DataStax OpsCenter simplifies the task of backup and restore of data out of a DSE (DataStax Enterprise) clusster a lot through its out-of-the-box feature of [Backup Service](https://docs.datastax.com/en/opscenter/6.5/opsc/online_help/services/opscBackupService.html). Through this service, a user can choose to bakup DSE data to multiple locations, including AWS S3, which becomes a more and more popular choice in today's ever-increasing cloud environment.

**==Restore Challenge==**

When we use OpsCener Service to restore backup data from S3, behind the scene it utilizes the traditional Cassandra "sstableloader" utility. Simply speaking, OpsCenter server, through datatax-agent on each DSE node, fetches backup data from 
S3 bucket and once it is done, it kicks of "sstableloader" to bulk-loading data into DSE cluster. It repeats the same process until all backup data in S3 bucket has been processed.

This approach has pros an cons: 
- The biggest pro is that it can tolerate DSE topology change, which means that the backup data can be restored to 1) the same cluster without any topology change; 2) the same cluster with some topology change; or 3) a brand new cluster.
- A major downside is that it is going to consume extra disk space (and extra disk and network I/O bandwith) in order to complete the whole process. For a keyspace with replication factor N (N > 1, normally 3 or above), it causes N times of the backup data to be ingested into the cluster. Although over the time, the C* compaction process will address the issue; but still, a lot of data has been transmitted over the network and processed in the system.


# 2. Solution Overview and Usage Description

In many cases, when there is **NO DSE cluster topology change**, a much faster approach (compared with approach we discussed above) would be to
1) Simply copy the backup data to its corresponding DSE node, under the right C* keyspace/table (file system) data directory
2) Once the data is copied, either restart DSE node or run "nodetool refresh" command (no restart needed) to pick up the data-to-be-retored in DSE cluster.

The second step of this approach is very straightforward. But when it comes to the first step of fetching corresponding DSE node backup data from a S3 bucket, there is NO ready-to-use tool that can help. The goal of this code repository is to provide such a utility that can help user to fast (multi-threaded) download DSE node specific backup items from S3 to a local directory. 

## 2.1. Usage Description

**[Fast S3 Backup Data Download Utility]**

1. Download the most recent release of .jar file from [here](https://github.com/yabinmeng/opscs3restore/releases/download/1.0/DseAWSRestore-1.0-SNAPSHOT.jar)

2. Download the example configuration file (opsc_s3_config.properties) from [here](https://github.com/yabinmeng/opscs3restore/blob/master/src/main/resources/opsc_s3_config.properties)

   The example configuration file includes 4 items to configure. These items are quite straightforward and self-explanatory. Please update accordingly to your use case!
```
dse_contact_point: 127.0.0.1
local_download_home: ./s3_download_test
opsc_s3_aws_region: us-east-1
opsc_s3_bucket_name: ymeng-dse-s3-test
```
**NOTE**: Please make sure correct AWS region and S3 bucket name are entered in this conifguration file!

3. Run the program, providing the proper java options and arguments.
```
java -Daws.accessKeyId=<your_aws_access_key> -Daws.secretKey=<your_aws_secret_key> -jar ./DseAWSRestore-1.0-SNAPSHOT.jar com.dsetools.DseOpscS3Restore -l <all|DC:"<DC_name>"|>me[:"<dsenode_host_id_string>"]> -f <opsc_s3_configure.properties_full_paht> -d <max_concurrent_downloading_thread_num>
```

The program needs a few Java options and parameters to work properly:

<table>
    <thead>
        <tr>
            <th width=45%>Java Option or Parameter</th>
            <th>Description</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td> -Daws.accessKeyId=&lt; your_aws_access_key &gt; </td>
            <td rowspan=2> 
                <li> AWS access credentials to access S3 bucket. </li>
                <li> AWS default credential file (~/.aws/credentials) is also supported and takes precedence over the credential JVM options. </li>
            </td>
        </tr>
        <tr>
            <td> -Daws.secretKey=&lt; your_aws_secret_key &gt; </td>
        </tr>
        <tr> 
            <td> -l &lt; all | DC:"&lt;DC_name&gt;" | me[:"&lt;dsenode_host_id_string&gt;"] </td>
            <td> List S3 backup items on the commandline output: <br/>
                <li> all -- list the S3 backup items for all nodes in the cluster </li>
                <li> DC:"&lt;DC_name&gt;" -- list the S3 backup items of all nodes in a specified DC </li>
                <li> me[:"&lt;dsenode_host_id_string&gt;"] -- list the S3 bckup item just for 1) myself (the node that runs this program - IP matching); or 2) for any DSE node with its host ID provided as second parameter for this option. </li>
        </tr>
        <tr>
            <td> -f &lt; opsc_s3_configure.properties_full_paht &gt; </td>
            <td> The full file path of "opsc_s3_configure.properties" file. </td>
        </tr>
        <tr>
            <td> -d &lt; max_concurrent_downloading_thread_num &gt; </td>
            <td> 
                <li> <b>ONLY works with "-l me" option; which means "-l all" and "-l DC" options are just for display purpose</b> </li>
                <li> &lt; max_concurrent_downloading_thread_num &gt; represents the number of threads (Max 10) that can concurrently download S3 backup sstable sets. </li>
        </tr>
    </tbody>
</table>
</br>

## 2.2. Examples

1. List OpsCenter S3 backup items for all nodes in a cluster 
```
java -Daws.accessKeyId=<aws_accesskey_id> -Daws.secretKey=<aws_secret_key> -jar ./DseAWSRestore-1.0-SNAPSHOT.jar com.dsetools.DseOpscS3Restore -l all -f ./opsc_s3_config.properties
```

2. List OpsCenter S3 backup items for all nodes in a specific DC named 'DC1'
```
java -Daws.accessKeyId=<aws_accesskey_id> -Daws.secretKey=<aws_secret_key> -jar ./DseAWSRestore-1.0-SNAPSHOT.jar com.dsetools.DseOpscS3Restore -l DC:"DC1" -f ./opsc_s3_config.properties
```

3. List OpsCenter S3 backup items for the current node that runs this program
```
java -Daws.accessKeyId=<aws_accesskey_id> -Daws.secretKey=<aws_secret_key> -jar ./DseAWSRestore-1.0-SNAPSHOT.jar com.dsetools.DseOpscS3Restore -l me -f ./opsc_s3_config.properties
```

4. List and **Download** OpsCenter S3 backup items for a particular node, with maximum concurrent download thread to be 5. Local download directory is configured in "opsc_s3_config.properties" file.
```
java -Daws.accessKeyId=<aws_accesskey_id> -Daws.secretKey=<aws_secret_key> -jar ./DseAWSRestore-1.0-SNAPSHOT.jar com.dsetools.DseOpscS3Restore -l me:"10409aec-241c-4a79-a707-2d3e4951dbf6" -f ./opsc_s3_config.properties -d 5
```

A sample output of example 4 above is as below:
```
java -Daws.accessKeyId=<aws_accesskey_id> -Daws.secretKey=<aws_secret_key> -jar ./DseAWSRestore-1.0-SNAPSHOT.jar com.dsetools.DseOpscS3Restore -l me -f ./opsc_s3_config.properties -d 5
List and download OpsCenter S3 backup items for specified host (10409aec-241c-4a79-a707-2d3e4951dbf6) ...

Totoal SSTable S3 object number: 6
  - snapshots/10409aec-241c-4a79-a707-2d3e4951dbf6/sstables/9941b3baf0c02303123bb01d40786861-mc-1-big-CompressionInfo.db (size = 43 bytes)
  - snapshots/10409aec-241c-4a79-a707-2d3e4951dbf6/sstables/9941b3baf0c02303123bb01d40786861-mc-1-big-Data.db (size = 261 bytes)
  - snapshots/10409aec-241c-4a79-a707-2d3e4951dbf6/sstables/9941b3baf0c02303123bb01d40786861-mc-1-big-Filter.db (size = 24 bytes)
  - snapshots/10409aec-241c-4a79-a707-2d3e4951dbf6/sstables/9941b3baf0c02303123bb01d40786861-mc-1-big-Index.db (size = 123 bytes)
  - snapshots/10409aec-241c-4a79-a707-2d3e4951dbf6/sstables/9941b3baf0c02303123bb01d40786861-mc-1-big-Statistics.db (size = 4739 bytes)
  - snapshots/10409aec-241c-4a79-a707-2d3e4951dbf6/sstables/9941b3baf0c02303123bb01d40786861-mc-1-big-Summary.db (size = 92 bytes)
  Creating thread with ID 0 (6).
   - Starting thread 0 at: 2018-07-06 23:07:49
     [Thread 0] download of "snapshots/10409aec-241c-4a79-a707-2d3e4951dbf6/sstables/9941b3baf0c02303123bb01d40786861-mc-1-big-CompressionInfo.db" completed
        >>> 43 of 43 bytes transferred (100.00%)
     [Thread 0] download of "snapshots/10409aec-241c-4a79-a707-2d3e4951dbf6/sstables/9941b3baf0c02303123bb01d40786861-mc-1-big-Data.db" completed
        >>> 261 of 261 bytes transferred (100.00%)
     [Thread 0] download of "snapshots/10409aec-241c-4a79-a707-2d3e4951dbf6/sstables/9941b3baf0c02303123bb01d40786861-mc-1-big-Filter.db" completed
        >>> 24 of 24 bytes transferred (100.00%)
     [Thread 0] download of "snapshots/10409aec-241c-4a79-a707-2d3e4951dbf6/sstables/9941b3baf0c02303123bb01d40786861-mc-1-big-Index.db" completed
        >>> 123 of 123 bytes transferred (100.00%)
     [Thread 0] download of "snapshots/10409aec-241c-4a79-a707-2d3e4951dbf6/sstables/9941b3baf0c02303123bb01d40786861-mc-1-big-Statistics.db" completed
        >>> 4739 of 4739 bytes transferred (100.00%)
     [Thread 0] download of "snapshots/10409aec-241c-4a79-a707-2d3e4951dbf6/sstables/9941b3baf0c02303123bb01d40786861-mc-1-big-Summary.db" completed
        >>> 92 of 92 bytes transferred (100.00%)
   - Existing Thread 0 at 2018-07-06 23:07:50 (duration: 0 seconds): 6 of 6 s3 objects downloaded, 0 failed.


Totoal OpsCenter S3 object number: 2
 - snapshots/10409aec-241c-4a79-a707-2d3e4951dbf6/opscenter_adhoc_2018-07-02-23-45-45-UTC/backup.json (size = 1460 bytes)
   ... download complete: 1460 of 1460 bytes transferred (100.00%)
 - snapshots/10409aec-241c-4a79-a707-2d3e4951dbf6/opscenter_adhoc_2018-07-02-23-45-45-UTC/testks/schema.json (size = 1173 bytes)
   ... download complete: 1173 of 1173 bytes transferred (100.00%)
```   
