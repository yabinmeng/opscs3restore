# Overview

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

## Restore Challenge 

