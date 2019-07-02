# 1. Problem Overview

DataStax OpsCenter simplifies the task of backup and restore of data out of a DSE (DataStax Enterprise) cluster a lot through its out-of-the-box feature of [Backup Service](https://docs.datastax.com/en/opscenter/6.5/opsc/online_help/services/opscBackupService.html). Through this service, a user can choose to bakup DSE data to multiple locations, including AWS S3, which becomes a more and more popular choice in today's ever-increasing cloud environment.

**==Restore Challenge==**

When we use OpsCener Service to restore backup data from S3, behind the scenes it utilizes the traditional Cassandra "sstableloader" utility. Simply speaking, OpsCenter server, through datatax-agent on each DSE node, fetches backup data from 
S3 bucket and once it is done, it kicks of "sstableloader" to bulk-loading data into DSE cluster. It repeats the same process until all backup data in S3 bucket has been processed.

This approach has pros and cons: 
- The biggest pro is that it can tolerate DSE topology change, which means that the backup data can be restored to:
  1) the same cluster without any topology change; or
  2) the same cluster with some topology change; or
  3) a brand new cluster.
- A major downside is that it is going to consume extra disk space (and extra disk and network I/O bandwith) in order to complete the whole process. For a keyspace with replication factor N (N > 1, normally 3 or above), it causes N times of the backup data to be ingested into the cluster. Although over time, the C* compaction process will address the issue; but still, a lot of data has been transmitted over the network and processed in the system.


# 2. Solution Overview and Usage Description

In many cases, when there is **NO DSE cluster topology change**, a much faster approach (compared with approach we discussed above) would be to
1) Simply copy the backup data to its corresponding DSE node, under the right C* keyspace/table (file system) data directory
2) Once the data is copied, either restart DSE node or run "nodetool refresh" command (no restart needed) to pick up the data-to-be-retored in DSE cluster.

The second step of this approach is very straightforward. But when it comes to the first step of fetching corresponding DSE node backup data from a S3 bucket, there is NO ready-to-use tool that can help. The goal of this code repository is to provide such a utility that can help user to fast (multi-threaded) download DSE node specific backup items from S3 to a local directory. 

## 2.1. Usage Description

**[Fast S3 Backup Data Download Utility]**

1. Download the most recent release (version 3.0) of .jar file from [here](https://github.com/yabinmeng/opscs3restore/releases/download/3.0/opscs3restore-3.0-SNAPSHOT.jar)

2. Download the example configuration file (opsc_s3_config.properties) from [here](https://github.com/yabinmeng/opscs3restore/blob/master/src/main/resources/opsc_s3_config.properties)

3. Run the program, providing the proper java options and arguments.
```
java 
  [-Daws.accessKeyId=<your_aws_access_key>] 
  [-Daws.secretKey=<your_aws_secret_key>]
  [-Djavax.net.ssl.trustStore=<client_truststore>] 
  [-Djavax.net.ssl.trustStorePassword=<client_truststore_password>]
  -jar ./DseAWSRestore-2.0-SNAPSHOT.jar com.dsetools.DseOpscS3Restore 
  -l <all|DC:"<DC_name>"|>me[:"<dsenode_host_id_string>"]> 
  -c <opsc_s3_configure.properties_full_path> 
  -d <concurrent_downloading_thread_num> 
  -k <keyspace_name> 
  [-t <table_name>] 
  -obt <opscenter_backup_time> 
  [-cls <true|false>]
  [-nds <true|false>]
  [-u <cassandra_user_name>]
  [-p <cassandra_user_password>]
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
            <td> -Djavax.net.ssl.trustStore </td>
            <td> SSL/TLS client truststore file path (when DSE client-to-node SSL/TLS is enabled) </td>
            <td> No </td>
        </tr>
        <tr>           
            <td> -Djavax.net.ssl.trustStorePassword </td>
            <td> SSL/TLS client truststore password (when DSE client-to-node SSL/TLS is enabled) </td>
            <td> No </td>
        </tr>
        <tr> 
            <td> -l &lt; all | DC:"&lt;DC_name&gt;" | me[:"&lt;dsenode_host_id_string&gt;"] </td>
            <td> List OpsCenter backup SSTables on the commandline output: <br/>
                <li> all -- list OpsCenter backup SSTables for all nodes in the cluster </li>
                <li> DC:"&lt;DC_name&gt;" -- list OpsCenter backup SSTables of all nodes in a specified DC </li>
                <li> me[:"&lt;dsenode_host_id_string&gt;"] -- list OpsCenter backup SSTables just for 
                   <ul> 
                      <li> myself (the node that runs this program - IP matching) </li> 
                      <li> for any DSE node with its host ID provided as second parameter for this option. </li>
                   </ul>
               </li>
             <td> Yes </td>
        </tr>
        <tr>
            <td> -c &lt; opsc_nfs_configure.properties_full_paht &gt; </td>
            <td> The full file path of "opsc_nfs_configure.properties" file. </td>
            <td> Yes </td>
        </tr>
        <tr>
            <td> -d &lt;max_concurrent_downloading_thread_num &gt; </td>
            <td> 
                <li> <b>ONLY works with "-l me" option; which means "-l all" and "-l DC" options are just for display purpose</b> </li>
                <li> &lt; concurrent_downloading_thread_num &gt; represents the number of threads (default 5 if not specified) that can concurrently download OpsCenter backup sstable sets. </li>
            </td>
            <td> No </td>
        </tr>
        <tr>
            <td> -k &lt;keyspace_name&gt; </td>
            <td> Download all OpsCenter backup SSTables that belong to the specified keyspace. </td>
            <td> Yes </td>
        </tr>
        <tr>
            <td> -t &lt;table_name&gt; </td>
            <td> <li> Download all OpsCenter backup SSTables that belong to the specified table. </li> 
                <li> When not specified, all Cassandra tables under the specified keyspace will be downloaded. </li>
            </td>
            <td> No </td>
        </tr>
        <tr>
            <td> -obt &lt;opsCenter_backup_time&gt; </td>
            <td> OpsCenter backup time (must be in format <b>M/d/yyyy h:mm a</b>) </li>
            </td>
            <td> Yes </td>
        </tr>
        <tr>
            <td> -cls &lt;true|false&gt; </td>
            <td> Whether to clear local download home directory before downloading (default: false)
            </td>
            <td> No </td>
        </tr>
        <tr>
            <td> -nds &lt;true|false&gt; </td>
            <td> Whether NOT to maitain backup location folder structure in the local download directory (default: false)
                <li> <b>ONLY applicable when "-t" option is specified.</b> </li> 
                <li> When NOT specified or NO "-t" option is specified, backup location folder structure is always maintained under the local download directory. This is to avoid possible SSTable name conflicts among different keyspaces and/or tables.</li>
            </td>
            <td> No </td>
        </tr>
        <tr>
            <td> -u &lt;cassandra_user_name&gt; </td>
            <td> Cassandra user name (when DSE authentication is enabled) </td>
            <td> No </td>
        </tr>
        <tr>
            <td> -p &lt;cassandra_user_password&gt; </td>
            <td> Cassandra user name (when DSE authentication is enabled) </td>
            <td> No </td>
        </tr>
    </tbody>
</table>
</br>

## 2.2. Utility configuration file 

The utility configuration file includes several items to configure. 
```
dse_contact_point: <DSE_cluster_contact_point>
local_download_home: <DSE_node_local_download_home_directory>
nfs_backup_home: <absolute_path_of_NFS_backup_location>
ip_matching_nic: <NIC_name_for_IP_matching>
use_ssl: <true | false>
user_auth: <true | false>
file_size_chk: <true | false>
```
Most of these items are straightforward and I'll explain some of them a little bit more.

* "dse_contact_point": When the utility needs to check DSE cluster metadata [-l ALL, -l DC:<DC_name>, -l me (no specific "dsenode_host_id_string")], it has to connecto to the DSE cluster in order to get the information. For these cases, an actively running DSE node IP should be provided here.

* "local_download_home" and "nfs_backup_home": Please make sure using the absolute path for both the NFS backup location and the local download home directory! The Linux user that runs this utility needs to have read privilege on the NFS backup location as well as both read and write privilege on the local download directory.

* "ip_matching_nic": When use -l me (no specific "dsenode_host_id_string") option, the utility automatically finds the correct DSE node host ID through IP matching. This parameter tells the utility which NIC name to use for IP matching. 

* "use_ssl" is ONLY relevant when DSE client-to-node SSL/TLS encryption is enabled. When true, Java system properties "-Djavax.net.ssl.trustStore" and "-Djavax.net.ssl.trustStorePassword" must be provided.

* "user_auth" is ONLY relevant when DSE authentication is enabled. When true, "-u <cassandra_user_name>" and "-p <cassandra_user_password>" options must be provided.

* "file_size_chk": Whether to bypass backup file size check during the download. When setting to false (default), the utility doesn't check and display file size for each to-be-restored backup files. This can be beneficial for overall performance.

## 2.3. Filter OpsCenter S3 backup SSTables by keyspace, table, and backup_time

This utility allows you to download OpsCenter s3 backup SSTables further by the following categories:
1. Cassandra keyspace name that the SSTables belong to ("-k" option, Mandatory)
2. Cassandra table name that the SSTables belong to ("-t" option, Optional)
3. OpsCenter backup time ("-obt" option, Mandatory)  

If Cassandra table name is not specified, then all SSTables belonging to all tables under the specified keyspace will be downloaded.

When specifiying OpsCenter backup time, it <b>MUST</b> be 
- In format <b>M/d/yyyy h:m a</b> (an example: 7/9/2018 3:52 PM)
- Matching the OpsCenter backup time from OpsCenter WebUI, as highlighted in the example screenshot below:
  <img src="src/main/images/Screen%20Shot%202018-07-09%20at%2022.21.18.png" width="250px"/>

## 2.4. Multi-threaded Download and Local Download Folder Structure

This utility is designed to be multi-threaded by nature to download multiple SSTable sets. When I say one SSTable set, it refers to the following files together:
* mc-<#>-big-CompresssionInfo.db
* mc-<#>-big-Data.db
* mc-<#>-big-Filter.db
* mc-<#>-big-Index.db
* mc-<#>-big-Statistics.db
* mc-<#>-big-Summary.db

**NOTE**: the "mc" part at the beginning represents SSTable format version which correspsonds to a particular Cassandra version (such as "la", "lb", "ma", "mb", "mc", etc.). This utility supports all DSE versions (and corresponding SSTable formats). 

Each thread is downloading one SSTable set. Multiple threads can download multiple sets concurrently. The maximum number threads that can concurrently download is determined by the value of <b>-d option</b>. If this option is not specified, then the utility only lists the OpsCenter backup SSTables without actually downloading it.

When "-d <concurrent_downloading_thread_num>" option is provided, the backup SSTables files will be downloaded (from NFS backup location) to the spcified local download home directory. The following 2 options determine how the local download home directory is organized:
* The "-cls <true|false>" option controls whether to clear the local download home directory before starting downloading!
* The "-nds <true|false>" option controls whether to maintain backup location folder structure underthe local download home directory. We maintain such structure by default in order to organize the recovered SSTables by keyspaces and tables. When this option has a "true" value (don't maintain the backup location folder structure), all restored SSTables are flattened out and put directly under the local download home directory. <b>In order to avoid possible SSTable name conflict among different keyspaces and/or tables. "-nds <true|false>" option ONLY works when you specify "-t" option.</b>

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

## 2.5. Examples

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

3. List and **Download** (with concurrent downloading thread number 5) OpsCenter S3 backup items for a particular node that runs this program and belong to C* keyspace "testks" for the backup taken at 7/9/2018 3:52 PM. Local download directory is configured in "opsc_s3_config.properties" file and will be cleared before downloading.
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
