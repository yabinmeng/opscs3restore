# 1. Background and Problem Overview

DataStax OpsCenter simplifies the task of backup and restore of data out of a DSE (DataStax Enterprise) clusster a lot through its out-of-the-box feature of [Backup Service](https://docs.datastax.com/en/opscenter/6.5/opsc/online_help/services/opscBackupService.html). Through this service, a user can choose to bakup DSE data to multiple locations, including AWS S3, which becomes a more and more popular choice in today's ever-increasing cloud environment.

When backing up a DSE clsuter keyspace/table to AWS S3, the backup files are organized in S3 in the following structure.

```
mybucket/
    snapshots/
      node-id1/
        sstables/
          MyKeyspace-MyTable-ic-5-Data.db
          ...
          MyKeyspace-MyTable-ic-5-TOC.txt
          MyKeyspace-MyTable-ic-6-Data.db
          ...
        1234-ABCD-2014-10-01-01-00/
          backup.json
          MyKeyspace/schema.json
        1234-ABCD-2014-09-30-01-00/
          backup.json
          MyKeyspace/schema.json
          
       node-id2/
         sstables/
         ...
```

Please note that this is just a "virtual" structure. The actual S3 object is simply a storage blob file with certain naming convention. An example is as below fo 
```
mybucket/snapshots/node-id1/sstables/MyKeyspace-MyTable-ic-5-Data.db
```

## 1.1. Restore Challenge 

When we use OpsCener Service to restore backup data from S3, behind the scene it utilizes the traditional Cassandra "sstableloader" utility. Simply speaking, OpsCenter server, through datatax-agent on each DSE node, fetches backup data from 
S3 bucket and once it is done, it kicks of "sstableloader" to bulk-loading data into DSE cluster. It repeats the same process until all backup data in S3 bucket has been processed.

This approach has pros an cons: 
- The biggest pro is that it can tolerate DSE topology change, which means that the backup data can be restored to 1) the same cluster without any topology change; 2) the same cluster with some topology change; or 3) a brand new cluster.
- A major downside is that it is going to consume extra disk space (and extra disk and network I/O bandwith) in order to complete the whole process. For a keyspace with replication factor N (N > 1, normally 3 or above), it causes N times of the backup data to be ingested into he cluster. Although over the time, the C* compaction process will address the issue. But still, a lot of data has been transmitted over the network and processed in the system.


# 2. Solution Overview and Introduction

In many cases, when there is **NO DSE cluster topology change**, a much faster approach (compared with approach we discussed in section 1.1) would be to
1) Simply copy the backup data to its corresponding DSE node, under the right C* keyspace/table (file system) folder
2) Once the data is copied, run "nodetool refresh" command to pick up the data-to-be-retored in DSE cluster.

The second step of this approach is very straightforward. But when it comes to the first step of fetching corresponding DSE node backup data from a S3 bucket, there is NO ready-to-use tool that can help. The goal of this code repository is to provide such a utility that can help user to fast (multi-threaded) download DSE node specific backup items from S3 to a local directory. 

It also contains an example bash script file that covers the whole (2 steps) procedure of this approach - downloading backup data from S3 to a local directory; and run "nodetool refresh" to restore data into DSE cluster.

## 2.2. Usage Description

### 2.2.1. Fast S3 Backup Data Download Utility

1. Download the most recent release of .jar file from [here](https://github.com/yabinmeng/opscs3restore/releases/download/1.0/DseAWSRestore-1.0-SNAPSHOT.jar)

2. Download the example configuration file (opsc_s3_config.properties) from [here](https://github.com/yabinmeng/opscs3restore/blob/master/src/main/resources/opsc_s3_config.properties)

   The example configuration file includes 4 items to configure. These items are quite straightforward and self-explanatory!
```
dse_contact_point: 127.0.0.1
local_download_home: ./s3_download_test
opsc_s3_aws_region: us-east-1
opsc_s3_bucket_name: ymeng-dse-s3-test
```

3. Run the program 
```
java -Daws.accessKeyId=<your_aws_access_key> -Daws.secretKey=<your_aws_secret_key> -jar ./DseAWSRestore-1.0-SNAPSHOT.jar com.dsetools.DseOpscS3Restore -l <all|DC:"<DC_name>"|>me> -f <opsc_s3_configure.properties_full_paht> -d <max_concurrent_downloading_thread_num>
```

The program needs a few Java options and parameters to work properly:

<table>
    <thead>
        <tr>
            <th width=40%>Java Option or Parameter</th>
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
            <td> -l &lt; all | DC:"&lt;DC_name&gt;" | me &gt; </td>
            <td> List S3 backup items on the commandline output: <br/>
                <li> all -- list the S3 backup items for all nodes in the cluster </li>
                <li> DC:"&lt;DC_name&gt;" -- list the S3 backup items of all nodes in a specified DC </li>
                <li> me -- list the S3 bckup item just for myself (the node that runs this program) </li>
        </tr>
        <tr>
            <td> -f &lt; opsc_s3_configure.properties_full_paht &gt; </td>
            <td> The full file path of "opsc_s3_configure.properties" file. </td>
        </tr>
        <tr>
            <td> -d &lt; max_concurrent_downloading_thread_num &gt; </td>
            <td> 
                <li> ONLY works with "-l me" option; which means "-l all" and "-l DC" options are just for display purpose </li>
                <li> &lt; max_concurrent_downloading_thread_num &gt; represents the number of threads (Max 10) that can concurrently download S3 backup sstable sets. </li>
        </tr>
    </tbody>
</table>


