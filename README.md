# 1. Problem Overview

DataStax OpsCenter simplifies the task of backup and restore of data out of a DSE (DataStax Enterprise) cluster a lot through its out-of-the-box feature of [Backup Service](https://docs.datastax.com/en/opscenter/6.5/opsc/online_help/services/opscBackupService.html). Through this service, a user can choose to bakup DSE data to multiple locations, including AWS S3, which becomes a more and more popular choice in today's ever-increasing cloud environment.

**==Restore Challenge==**

When we use OpsCener Service to restore backup data from S3, behind the scene it utilizes the traditional Cassandra "sstableloader" utility. Simply speaking, OpsCenter server, through datatax-agent on each DSE node, fetches backup data from 
S3 bucket and once it is done, it kicks of "sstableloader" to bulk-loading data into DSE cluster. It repeats the same process until all backup data in S3 bucket has been processed.

This approach has pros an cons: 
- The biggest pro is that it can tolerate DSE topology change, which means that the backup data can be restored to:
  1) the same cluster without any topology change; or
  2) the same cluster with some topology change; or
  3) a brand new cluster.
- A major downside is that it is going to consume extra disk space (and extra disk and network I/O bandwith) in order to complete the whole process. For a keyspace with replication factor N (N > 1, normally 3 or above), it causes N times of the backup data to be ingested into the cluster. Although over the time, the C* compaction process will address the issue; but still, a lot of data has been transmitted over the network and processed in the system.


# 2. Solution Overview and Usage Description

In many cases, when there is **NO DSE cluster topology change**, a much faster approach (compared with approach we discussed above) would be to
1) Simply copy the backup data to its corresponding DSE node, under the right C* keyspace/table (file system) data directory
2) Once the data is copied, either restart DSE node or run "nodetool refresh" command (no restart needed) to pick up the data-to-be-retored in DSE cluster.

The second step of this approach is very straightforward. But when it comes to the first step of fetching corresponding DSE node backup data from a S3 bucket, there is NO ready-to-use tool that can help. The goal of this code repository is to provide such a utility that can help user to fast (multi-threaded) download DSE node specific backup items from S3 to a local directory. 

## 2.1. Usage Description

**[Fast S3 Backup Data Download Utility]**

1. Download the most recent release (version 2.0) of .jar file from [here](https://github.com/yabinmeng/opscs3restore/releases/download/2.0/DseAWSRestore-2.0-SNAPSHOT.jar)

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
java 
  [-Daws.accessKeyId=<your_aws_access_key>] 
  [-Daws.secretKey=<your_aws_secret_key>] 
  -jar ./DseAWSRestore-2.0-SNAPSHOT.jar com.dsetools.DseOpscS3Restore 
  -l <all|DC:"<DC_name>"|>me[:"<dsenode_host_id_string>"]> 
  -c <opsc_s3_configure.properties_full_paht> 
  -d <concurrent_downloading_thread_num> 
  -k <keyspace_name> 
  [-t <table_name>] 
  -obt <opscenter_backup_time> 
  [-cls <true|false>]
```

The program needs a few Java options and parameters to work properly:

<table>
    <thead>
        <tr>
            <th width=43%>Java Option or Parameter</th>
            <th width=43%>Description</th>
            <th width=4%>Mandatory?</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td> -Daws.accessKeyId=&lt; your_aws_access_key &gt; </td>
            <td rowspan=2> 
                <li> AWS access credentials to access S3 bucket. </li>
              <li> <b>AWS default credential file (~/.aws/credentials)</b> is also supported and <b>takes precedence</b> over the credential JVM options. </li>
            </td>
            <td rowspan=2> No </td>
        </tr>
        <tr>
            <td> -Daws.secretKey=&lt; your_aws_secret_key &gt; </td>
        </tr>
        <tr> 
            <td> -l &lt; all | DC:"&lt;DC_name&gt;" | me[:"&lt;dsenode_host_id_string&gt;"] </td>
            <td> List S3 backup items on the commandline output: <br/>
                <li> all -- list the S3 backup items for all nodes in the cluster </li>
                <li> DC:"&lt;DC_name&gt;" -- list the S3 backup items of all nodes in a specified DC </li>
                <li> me[:"&lt;dsenode_host_id_string&gt;"] -- list the S3 bckup item just for 
                   <ul> 
                      <li> myself (the node that runs this program - IP matching) </li> 
                      <li> for any DSE node with its host ID provided as second parameter for this option. </li>
                   </ul>
               </li>
             <td> Yes </td>
        </tr>
        <tr>
            <td> -c &lt; opsc_s3_configure.properties_full_paht &gt; </td>
            <td> The full file path of "opsc_s3_configure.properties" file. </td>
            <td> Yes </td>
        </tr>
        <tr>
            <td> -d &lt;max_concurrent_downloading_thread_num &gt; </td>
            <td> 
                <li> <b>ONLY works with "-l me" option; which means "-l all" and "-l DC" options are just for display purpose</b> </li>
                <li> &lt; concurrent_downloading_thread_num &gt; represents the number of threads (default 5 if not specified) that can concurrently download S3 backup sstable sets. </li>
           </td>
           <td> No </td>
        </tr>
        <tr>
           <td> -k &lt;keyspace_name&gt; </td>
           <td> Download all OpsCenter S3 backup SSTables that belong to the specified keyspace. </td>
           <td> Yes </td>
        </tr>
        <tr>
           <td> -t &lt;table_name&gt; </td>
           <td> <li> Download all OpsCenter S3 backup SSTables that belong to the specified table. </li> 
                <li> When not specified, all Cassandra tables under the specified keyspace will be downloaded. </li>
           </td>
           <td> No </td>
         </tr>
         <tr>
           <td> -obt &lt;opsCenter_backup_time&gt; </td>
            <td> OpsCenter backup time (must be in format <b>M/d/yyyy h:m a</b>) </li>
           </td>
           <td> Yes </td>
         </tr>
         <tr>
           <td> -cls &lt;true|false&gt; </td>
           <td> Whether to clear target download directory (default: false)
                When not specified, all Cassandra tables under the specified keyspace will be downloaded.
           </td>
           <td> No </td>
         </tr>
    </tbody>
</table>
</br>

## 2.2. Filter OpsCenter S3 backup SSTables by keyspace, table, and backup_time

This utility allows you to download OpsCenter s3 backup SSTables further by the following categories:
1. Cassandra keyspace name that the SSTables belong to ("-k" option, Mandtory)
2. Cassandra table name that the SSTables belong to ("-t" option, Optional)
3. OpsCenter backup time ("-obt" option, Mandatory)  

If Cassandra table name is not specified, then all SSTables belonging to all tables under the specified keyspace will be downloaded.

When specifiying OpsCenter backup time, it <b>MUST</b> be 
- In format <b>M/d/yyyy h:m a</b> (an example: 7/9/2018 3:52 PM)
- Matching the OpsCenter backup time from OpsCenter WebUI, as highlighted in the example screenshot below:
  <img src="src/main/images/Screen%20Shot%202018-07-09%20at%2022.21.18.png" width="250px"/>

## 2.3. Multi-threaded Download and Local Download Folder Structure

This utility is designed to be multi-threaded by nature to download multiple SSTable sets. When I say one SSTable set, it refers to the following files together:
* mc-<#>-big-CompresssionInfo.db
* mc-<#>-big-Data.db
* mc-<#>-big-Filter.db
* mc-<#>-big-Index.db
* mc-<#>-big-Statistics.db
* mc-<#>-big-Summary.db

**NOTE**: Currently this utility ONLY supports C* table with "mc" format (C* 3.0+/DSE 5.0/DSE5.1). It will be extended in the future to support other versions of formats.

Each thread is downloading one SSTable set. Multiple threads can download multiple sets concurrently. The maximum number threads tha can concurrently download is determined by the value of <b>-d option</b>. If this option is not specified, then the utility only lists the OpsCenter S3 backup items without actually downloading it.

When "-d <concurrent_downloading_thread_num>" option is provided, the downloaded OpsCenter S3 backup SSTables are organized locally in the following structure:

<text><b> &lt;local_download_home&gt;/snapshots/&lt;DSE_host_id&gt;/sstables/&lt;keyspace&gt;/&lt;table&gt;/mc-&lt;#&gt;-big-xxx.db </b></text>. 

An example is demonstrated below:

```
s3_download_test/
└── snapshots
    └── 53db322b-7d09-421c-b189-0d5aa9dc0e44
        ├── opscenter_adhoc_2018-07-09-15-52-06-UTC
        │   ├── backup.json
        │   ├── testks
        │   │   └── schema.json
        │   └── testks1
        │       └── schema.json
        └── sstables
            ├── testks
            │   ├── singers
            │   │   ├── mc-1-big-CompressionInfo.db
            │   │   ├── mc-1-big-Data.db
            │   │   ├── mc-1-big-Filter.db
            │   │   ├── mc-1-big-Index.db
            │   │   ├── mc-1-big-Statistics.db
            │   │   └── mc-1-big-Summary.db
            │   └── songs
            │       ├── mc-2-big-CompressionInfo.db
            │       ├── mc-2-big-Data.db
            │       ├── mc-2-big-Filter.db
            │       ├── mc-2-big-Index.db
            │       ├── mc-2-big-Statistics.db
            │       ├── mc-2-big-Summary.db
            │       ├── mc-3-big-CompressionInfo.db
            │       ├── mc-3-big-Data.db
            │       ├── mc-3-big-Filter.db
            │       ├── mc-3-big-Index.db
            │       ├── mc-3-big-Statistics.db
            │       └── mc-3-big-Summary.db
            └── testks1
                └── testtbl
                    ├── mc-1-big-CompressionInfo.db
                    ├── mc-1-big-Data.db
                    ├── mc-1-big-Filter.db
                    ├── mc-1-big-Index.db
                    ├── mc-1-big-Statistics.db
                    └── mc-1-big-Summary.db
```

The "-cls <true|false>" option controls whether to clear the local download directory before starting downloading!

## 2.4. Examples

1. List **Only** OpsCenter S3 backup items for all nodes in a cluster that belong to C* table "testks.songs" (<keyspace.table>) for the backup taken at 7/9/2018 3:52 PM
```
java 
  -Daws.accessKeyId=<aws_accesskey_id> 
  -Daws.secretKey=<aws_secret_key> 
  -jar ./DseAWSRestore-2.0-SNAPSHOT.jar com.dsetools.DseOpscS3Restore 
  -c ./opsc_s3_config.properties
  -l all 
  -k testks 
  -t songs 
  -obt "7/9/2018 3:52 PM"
```

2. List **Only** OpsCenter S3 backup items for the current node that runs this program and belong to C* keyspace "testks1" for the backup taken at 7/9/2018 3:52 PM
```
java 
  -Daws.accessKeyId=<aws_accesskey_id> 
  -Daws.secretKey=<aws_secret_key> 
  -jar ./DseAWSRestore-2.0-SNAPSHOT.jar com.dsetools.DseOpscS3Restore 
  -c ./opsc_s3_config.properties
  -l me
  -k testks1 
  -obt "7/9/2018 3:52 PM"
```

3. List and **Download** (with concurren downloading thread number 5) OpsCenter S3 backup items for a particular node that runs this program and belong to C* keyspace "testks" for the backup taken at 7/9/2018 3:52 PM. Local download directory is configured in "opsc_s3_config.properties" file and will be cleared before downloading.
```
java 
  -Daws.accessKeyId=<aws_accesskey_id> 
  -Daws.secretKey=<aws_secret_key> 
  -jar ./DseAWSRestore-2.0-SNAPSHOT.jar com.dsetools.DseOpscS3Restore 
  -c ./opsc_s3_config.properties 
  -l me:"10409aec-241c-4a79-a707-2d3e4951dbf6" 
  -d 5
  -k testks
  -obt "7/9/2018 3:52 PM"
  -cls true
```

The utility command line output for example 3 above is something like below:
```
List and download OpsCenter S3 backup items for specified host (10409aec-241c-4a79-a707-2d3e4951dbf6) ...
 - snapshots/10409aec-241c-4a79-a707-2d3e4951dbf6/opscenter_adhoc_2018-07-09-15-52-06-UTC/backup.json (size = 4086 bytes)
   ... download complete: 4086 of 4086 bytes transferred (100.00%)
 - snapshots/10409aec-241c-4a79-a707-2d3e4951dbf6/opscenter_adhoc_2018-07-09-15-52-06-UTC/testks/schema.json (size = 2039 bytes)
   ... download complete: 2039 of 2039 bytes transferred (100.00%)
 - snapshots/10409aec-241c-4a79-a707-2d3e4951dbf6/opscenter_adhoc_2018-07-09-15-52-06-UTC/testks1/schema.json (size = 1187 bytes)
   ... download complete: 1187 of 1187 bytes transferred (100.00%)

  - snapshots/10409aec-241c-4a79-a707-2d3e4951dbf6/sstables/830be0b989458a7cc65257332109d5fc-mc-1-big-CompressionInfo.db (size = 43 bytes) [keyspace: testks; table: singers]
  - snapshots/10409aec-241c-4a79-a707-2d3e4951dbf6/sstables/830be0b989458a7cc65257332109d5fc-mc-1-big-Data.db (size = 123 bytes) [keyspace: testks; table: singers]
  - snapshots/10409aec-241c-4a79-a707-2d3e4951dbf6/sstables/830be0b989458a7cc65257332109d5fc-mc-1-big-Filter.db (size = 16 bytes) [keyspace: testks; table: singers]
  - snapshots/10409aec-241c-4a79-a707-2d3e4951dbf6/sstables/830be0b989458a7cc65257332109d5fc-mc-1-big-Index.db (size = 60 bytes) [keyspace: testks; table: singers]
  - snapshots/10409aec-241c-4a79-a707-2d3e4951dbf6/sstables/830be0b989458a7cc65257332109d5fc-mc-1-big-Statistics.db (size = 4611 bytes) [keyspace: testks; table: singers]
  - snapshots/10409aec-241c-4a79-a707-2d3e4951dbf6/sstables/830be0b989458a7cc65257332109d5fc-mc-1-big-Summary.db (size = 92 bytes) [keyspace: testks; table: singers]
  Creating thread with ID 0 (6).
  - snapshots/10409aec-241c-4a79-a707-2d3e4951dbf6/sstables/d33116f6603946089a7fd536e4e5aa1f-mc-1-big-CompressionInfo.db (size = 43 bytes) [keyspace: testks; table: songs]
  - snapshots/10409aec-241c-4a79-a707-2d3e4951dbf6/sstables/d33116f6603946089a7fd536e4e5aa1f-mc-1-big-Data.db (size = 261 bytes) [keyspace: testks; table: songs]
  - snapshots/10409aec-241c-4a79-a707-2d3e4951dbf6/sstables/d33116f6603946089a7fd536e4e5aa1f-mc-1-big-Filter.db (size = 24 bytes) [keyspace: testks; table: songs]
  - snapshots/10409aec-241c-4a79-a707-2d3e4951dbf6/sstables/d33116f6603946089a7fd536e4e5aa1f-mc-1-big-Index.db (size = 123 bytes) [keyspace: testks; table: songs]
  - snapshots/10409aec-241c-4a79-a707-2d3e4951dbf6/sstables/d33116f6603946089a7fd536e4e5aa1f-mc-1-big-Statistics.db (size = 4739 bytes) [keyspace: testks; table: songs]
  - snapshots/10409aec-241c-4a79-a707-2d3e4951dbf6/sstables/d33116f6603946089a7fd536e4e5aa1f-mc-1-big-Summary.db (size = 92 bytes) [keyspace: testks; table: songs]
  Creating thread with ID 1 (6).
   - Starting thread 1 at: 2018-07-10 04:16:05
   - Starting thread 0 at: 2018-07-10 04:16:05
     [Thread 1] download of "snapshots/10409aec-241c-4a79-a707-2d3e4951dbf6/sstables/d33116f6603946089a7fd536e4e5aa1f-mc-1-big-CompressionInfo.db[keyspace: testks; table: songs]" completed
        >>> 43 of 43 bytes transferred (100.00%)
     [Thread 0] download of "snapshots/10409aec-241c-4a79-a707-2d3e4951dbf6/sstables/830be0b989458a7cc65257332109d5fc-mc-1-big-CompressionInfo.db[keyspace: testks; table: singers]" completed
        >>> 43 of 43 bytes transferred (100.00%)
     [Thread 1] download of "snapshots/10409aec-241c-4a79-a707-2d3e4951dbf6/sstables/d33116f6603946089a7fd536e4e5aa1f-mc-1-big-Data.db[keyspace: testks; table: songs]" completed
        >>> 261 of 261 bytes transferred (100.00%)
     [Thread 0] download of "snapshots/10409aec-241c-4a79-a707-2d3e4951dbf6/sstables/830be0b989458a7cc65257332109d5fc-mc-1-big-Data.db[keyspace: testks; table: singers]" completed
     [Thread 1] download of "snapshots/10409aec-241c-4a79-a707-2d3e4951dbf6/sstables/d33116f6603946089a7fd536e4e5aa1f-mc-1-big-Filter.db[keyspace: testks; table: songs]" completed
        >>> 123 of 123 bytes transferred (100.00%)
        >>> 24 of 24 bytes transferred (100.00%)
     [Thread 0] download of "snapshots/10409aec-241c-4a79-a707-2d3e4951dbf6/sstables/830be0b989458a7cc65257332109d5fc-mc-1-big-Filter.db[keyspace: testks; table: singers]" completed
        >>> 16 of 16 bytes transferred (100.00%)
     [Thread 1] download of "snapshots/10409aec-241c-4a79-a707-2d3e4951dbf6/sstables/d33116f6603946089a7fd536e4e5aa1f-mc-1-big-Index.db[keyspace: testks; table: songs]" completed
        >>> 123 of 123 bytes transferred (100.00%)
     [Thread 0] download of "snapshots/10409aec-241c-4a79-a707-2d3e4951dbf6/sstables/830be0b989458a7cc65257332109d5fc-mc-1-big-Index.db[keyspace: testks; table: singers]" completed
        >>> 60 of 60 bytes transferred (100.00%)
     [Thread 1] download of "snapshots/10409aec-241c-4a79-a707-2d3e4951dbf6/sstables/d33116f6603946089a7fd536e4e5aa1f-mc-1-big-Statistics.db[keyspace: testks; table: songs]" completed
        >>> 4739 of 4739 bytes transferred (100.00%)
     [Thread 0] download of "snapshots/10409aec-241c-4a79-a707-2d3e4951dbf6/sstables/830be0b989458a7cc65257332109d5fc-mc-1-big-Statistics.db[keyspace: testks; table: singers]" completed
        >>> 4611 of 4611 bytes transferred (100.00%)
     [Thread 1] download of "snapshots/10409aec-241c-4a79-a707-2d3e4951dbf6/sstables/d33116f6603946089a7fd536e4e5aa1f-mc-1-big-Summary.db[keyspace: testks; table: songs]" completed
        >>> 92 of 92 bytes transferred (100.00%)
   - Existing Thread 1 at 2018-07-10 04:16:06 (duration: 1 seconds): 6 of 6 s3 objects downloaded, 0 failed.
     [Thread 0] download of "snapshots/10409aec-241c-4a79-a707-2d3e4951dbf6/sstables/830be0b989458a7cc65257332109d5fc-mc-1-big-Summary.db[keyspace: testks; table: singers]" completed
        >>> 92 of 92 bytes transferred (100.00%)
   - Existing Thread 0 at 2018-07-10 04:16:06 (duration: 1 seconds): 6 of 6 s3 objects downloaded, 0 failed.
```
