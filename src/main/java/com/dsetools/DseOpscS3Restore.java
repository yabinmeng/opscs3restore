package com.dsetools;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.SdkClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.SystemPropertiesCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.*;
import com.amazonaws.services.s3.transfer.Download;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;
import com.amazonaws.services.s3.transfer.TransferProgress;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.QueryOptions;
import com.datastax.driver.dse.DseCluster;
import org.apache.commons.cli.*;
import org.apache.commons.io.FileUtils;
import org.json.simple.JSONArray;
import org.json.simple.parser.JSONParser;
import org.json.simple.JSONObject;

import java.io.*;
import java.net.*;
import java.nio.file.Files;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


class S3ObjDownloadRunnable implements  Runnable {
    private int threadID;
    private TransferManager s3TransferManager;
    private String s3BuketName;
    private String downloadHomeDir;
    private String[] s3ObjNames;
    private long[] s3ObjSizes;
    private String[] keyspaceNames;
    private String[] tableNames;
    private String[] uniquifiers;

    private DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    S3ObjDownloadRunnable( int tID,
                           TransferManager transferManager,
                           String s3bkt_name,
                           String download_dir,
                           String[] object_names,
                           long[] object_sizes,
                           String[] ks_names,
                           String[] tbl_names,
                           String[] uniq_ids) {
        assert (tID > 0);
        assert (transferManager != null);

        this.threadID = tID;
        this.s3TransferManager = transferManager;
        this.s3BuketName = s3bkt_name;
        this.downloadHomeDir = download_dir;
        this.s3ObjNames = object_names;
        this.s3ObjSizes = object_sizes;
        this.keyspaceNames = ks_names;
        this.tableNames = tbl_names;
        this.uniquifiers = uniq_ids;

        System.out.format("  Creating thread with ID %d (%d).\n", threadID, s3ObjNames.length);
    }

    @Override
    public void run() {

        LocalDateTime startTime = LocalDateTime.now();

        System.out.println("   - Starting thread " + threadID + " at: " + startTime.format(formatter));

        int downloadedS3ObjNum = 0;
        int failedS3ObjNum = 0;

        for ( int i = 0; i < s3ObjNames.length; i++ ) {
            try {
                String realSStableName = null;

                int mcFlagStartPos = s3ObjNames[i].indexOf(DseOpscS3RestoreUtils.CASSANDRA_SSTABLE_FILE_CODE);
                realSStableName = s3ObjNames[i].substring(mcFlagStartPos);

                String tmp = s3ObjNames[i].substring(0, mcFlagStartPos -1 );
                int lastPathSeperatorPos = tmp.lastIndexOf('/');

                String parentPathStr = tmp.substring(0, lastPathSeperatorPos);

                File localFile = new File(downloadHomeDir + "/" +
                    parentPathStr + "/" + keyspaceNames[i] + "/" +
                    tableNames[i] + "/" + realSStableName);

                GetObjectRequest getObjectRequest = new GetObjectRequest(s3BuketName, s3ObjNames[i]);
                Download s3Download = s3TransferManager.download(getObjectRequest, localFile);

                // wait for download to complete
                s3Download.waitForCompletion();

                downloadedS3ObjNum++;

                System.out.format("     [Thread %d] download of \"%s\" completed \n", threadID,
                    s3ObjNames[i] + "[keyspace: " + keyspaceNames[i] + "; table: " + tableNames[i] + "]");
                TransferProgress transferProgress = s3Download.getProgress();
                System.out.format("        >>> %d of %d bytes transferred (%.2f%%)\n",
                    transferProgress.getBytesTransferred(),
                    s3ObjSizes[i],
                    transferProgress.getPercentTransferred());
            }
            catch ( InterruptedException ie) {
                System.out.format("     [Thread %d] download of \"%s\" interrupted\n", threadID,
                    s3ObjNames[i] + "[keyspace: " + keyspaceNames[i] + "; table: " + tableNames[i] + "]");
                failedS3ObjNum++;
            }
            catch ( Exception ex ) {
                ex.printStackTrace();
                System.out.format("     [Thread %d] download of \"%s\" failed - unkown error\n", threadID,
                    s3ObjNames[i] + "[keyspace: " + keyspaceNames[i] + "; table: " + tableNames[i] + "]");
                ex.printStackTrace();
                failedS3ObjNum++;
            }
        }

        LocalDateTime endTime = LocalDateTime.now();

        Duration duration = Duration.between(startTime, endTime);

        System.out.format("   - Existing Thread %d at %s (duration: %d seconds): %d of %d s3 objects downloaded, %d failed.\n",
            threadID,
            endTime.format(formatter),
            duration.getSeconds(),
            downloadedS3ObjNum,
            s3ObjNames.length,
            failedS3ObjNum
        );
    }
}


public class DseOpscS3Restore {


    /**
     * Download a single S3 object to a local file.
     *
     * @param s3TransferManager
     * @param localFilePath
     * @param s3BukcetName
     * @param s3ObjKeyName
     */
    static void downloadSingleS3Obj(TransferManager s3TransferManager,
                                    String localFilePath,
                                    String s3BukcetName,
                                    String s3ObjKeyName,
                                    long s3ObjeKeySize,
                                    boolean printMsg) {
        File localFile = new File(localFilePath);
        GetObjectRequest getObjectRequest = new GetObjectRequest(s3BukcetName, s3ObjKeyName);
        Download s3Download = s3TransferManager.download(getObjectRequest, localFile);

        try {
            // wait for download to complete
            s3Download.waitForCompletion();
        }
        catch ( InterruptedException ie) {
            if (printMsg) {
                System.out.println("   ... Download of [" + s3BukcetName + "] " + s3ObjKeyName + " gets interrupted.");
            }
        }
        catch ( Exception ex ) {
            if (printMsg) {
                System.out.println("   ... Download failed - unkown error.");
            }
            ex.printStackTrace();
        }

        TransferProgress transferProgress = s3Download.getProgress();
        if (printMsg) {
            System.out.format("   ... download complete: %d of %d bytes transferred (%.2f%%)\n",
                transferProgress.getBytesTransferred(),
                s3ObjeKeySize,
                transferProgress.getPercentTransferred());
        }
    }


    private static Properties CONFIGPROP = null;

    /**
     * List (and download) Opsc S3 backup objects for a specified host
     *
     * @param dseClusterMetadata
     * @param s3Client
     * @param hostId
     * @param download
     * @param threadNum
     * @param keyspaceName
     * @param tableName
     * @param opscBckupTimeGmt
     * @param clearTargetDownDir
     */
    static void listDownloadS3ObjForHost(Metadata dseClusterMetadata,
                                         AmazonS3 s3Client,
                                         String hostId,
                                         boolean download,
                                         int threadNum,
                                         String keyspaceName,
                                         String tableName,
                                         ZonedDateTime opscBckupTimeGmt,
                                         boolean clearTargetDownDir) {
        assert (hostId != null);

        System.out.format("List" +
            (download ? " and download" : "") +
            " OpsCenter S3 backup items for specified host (%s) ...\n", hostId);

        String downloadHomeDir = CONFIGPROP.getProperty(DseOpscS3RestoreUtils.CFG_KEY_LOCAL_DOWNLOAD_HOME);

        if (download) {
            assert (threadNum > 0);

            // If non-existing, create local home directory to hold S3 download files
            try {
                File file = new File(downloadHomeDir);

                if ( Files.notExists(file.toPath()))  {
                    FileUtils.forceMkdir(file);
                }
                else {
                    if (clearTargetDownDir) {
                        FileUtils.cleanDirectory(file);
                    }
                }
            }
            catch (IOException ioe) {
                System.out.println("ERROR: failed to create download home directory for S3 objects!");
                System.exit(-10);
            }
        }

        TransferManager transferManager = transferManager = TransferManagerBuilder.standard().withS3Client(s3Client).build();

        String bktName = CONFIGPROP.getProperty(DseOpscS3RestoreUtils.CFG_KEY_OPSC_S3_BUCKET_NAME);
        String basePrefix = DseOpscS3RestoreUtils.OPSC_S3_OBJKEY_BASESTR + "/" + hostId;

        // First, check OpsCenter records matching the backup time
        String prefixString = basePrefix + "/" +
            DseOpscS3RestoreUtils.OPSC_S3_OBJKEY_OPSC_MARKER_STR + "_";

        // Download OpsCenter S3 object items
        ObjectListing objectListing = s3Client.listObjects(
            new ListObjectsRequest()
                .withBucketName(bktName)
                .withPrefix(prefixString));
        List<S3ObjectSummary>  s3ObjectSummaries = objectListing.getObjectSummaries();

        DateTimeFormatter s3OpscObjTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd-HH-mm-ss-z");
        String opscBckupTimeGmtStr = opscBckupTimeGmt.format(s3OpscObjTimeFormatter);

        Map<String, String> s3UniquifierToKsTbls = new HashMap<String, String>();

        for (S3ObjectSummary objectSummary : s3ObjectSummaries) {
            String opscObjName = objectSummary.getKey();
            String opscObjNameShortZeroSecond =
                opscObjName.substring(prefixString.length(), prefixString.length() + 16) + "-00-UTC";

            // Only deal with the S3 objects that fall in the specified OpsCenter backup time range
            if (opscBckupTimeGmtStr.equalsIgnoreCase(opscObjNameShortZeroSecond)) {
                System.out.println(" - " + objectSummary.getKey() + " (size = " + objectSummary.getSize() + " bytes)");

                // Always download "backup.json" file since it contains criticial mapping for
                // S3 uniquifier to keyspace and table
                boolean downloadForOpsc = (opscObjName.contains("backup.json") || download );

                if (downloadForOpsc) {
                    try {
                        String objKeyName = objectSummary.getKey();

                        downloadSingleS3Obj(transferManager,
                            downloadHomeDir + "/" + objKeyName,
                            bktName,
                            objKeyName,
                            objectSummary.getSize(),
                            true);

                    } catch (AmazonServiceException e) {
                        // The call was transmitted successfully, but Amazon S3 couldn't process
                        // it, so it returned an error response.
                        e.printStackTrace();
                    } catch (SdkClientException e) {
                        // Amazon S3 couldn't be contacted for a response, or the client
                        // couldn't parse the response from Amazon S3.
                        e.printStackTrace();
                    }
                }

                // Process OpsCenter backup.json file to get the Keyspace/Table/S3_Identifier mapping
                if ( opscObjName.contains("backup.json") ) {
                    // After downloaded the backup.json file, process its content to get mapping between
                    // S3 Uniquifier to Keyspace-Table.
                    String localBackupJsonFile = downloadHomeDir + "/" + opscObjName;
                    s3UniquifierToKsTbls = getS3UniquifierToKsTblMapping(localBackupJsonFile);
                }
            }
        }

        System.out.println();

        // Download SSTable S3 object items
        objectListing = s3Client.listObjects(
            new ListObjectsRequest()
                .withBucketName(bktName)
                .withPrefix(DseOpscS3RestoreUtils.OPSC_S3_OBJKEY_BASESTR + "/" + hostId + "/sstable"));

        s3ObjectSummaries = objectListing.getObjectSummaries();
        int numSstableBkupItems = 0;

        /**
         *  Start multiple threads to process data ingestion concurrently
         */
        ExecutorService executor = Executors.newFixedThreadPool(threadNum);

        // For sstable download - we use mulitple threads per sstable set. One set includes the following files:
        // > mc-<#>-big-CompresssionInfo.db
        // > mc-<#>-big-Data.db
        // > mc-<#>-big-Filter.db
        // > mc-<#>-big-Index.db
        // > mc-<#>-big-Statistics.db
        // > mc-<#>-big-Summary.db
        final int SSTABLE_SET_FILENUM = 6;

        String[] s3SstableObjKeyNames = new String[SSTABLE_SET_FILENUM];
        long[] s3SstableObjKeySizes = new long[SSTABLE_SET_FILENUM];
        String[] s3SstableKSNames = new String[SSTABLE_SET_FILENUM];
        String[] s3SstableTBLNames = new String[SSTABLE_SET_FILENUM];
        String[] s3SstableUniquifiers = new String[SSTABLE_SET_FILENUM];

        int i = 0;
        int threadId = 0;

        for (S3ObjectSummary objectSummary : s3ObjectSummaries) {

            String sstableObjName = objectSummary.getKey();
            sstableObjName = sstableObjName.substring(sstableObjName.lastIndexOf("/") + 1);

            if (s3UniquifierToKsTbls.containsKey(sstableObjName)) {
                String[] ksTblUniquifer = s3UniquifierToKsTbls.get(sstableObjName).split(":");
                String ks = ksTblUniquifer[0];
                String tbl = ksTblUniquifer[1];
                String uniquifier = ksTblUniquifer[2];

                boolean filterKsTbl = keyspaceName.equalsIgnoreCase(ks);
                if ( (tableName != null) && !tableName.isEmpty() ) {
                    filterKsTbl = filterKsTbl && tableName.equalsIgnoreCase(tbl);
                }

                if (filterKsTbl) {
                    numSstableBkupItems++;
                    System.out.println("  - " + objectSummary.getKey() + " (size = " + objectSummary.getSize()
                        + " bytes) [keyspace: " + ks + "; table: " + tbl + "]");

                    s3SstableObjKeyNames[i % SSTABLE_SET_FILENUM] = objectSummary.getKey();
                    s3SstableObjKeySizes[i % SSTABLE_SET_FILENUM] = objectSummary.getSize();
                    s3SstableKSNames[i % SSTABLE_SET_FILENUM] = ks;
                    s3SstableTBLNames[i % SSTABLE_SET_FILENUM] = tbl;
                    s3SstableUniquifiers[i % SSTABLE_SET_FILENUM] = uniquifier;

                    if ( download ) {
                        if ( (i > 0) && ((i + 1) % SSTABLE_SET_FILENUM == 0) ) {
                            Runnable worker = new S3ObjDownloadRunnable(
                                threadId,
                                transferManager,
                                bktName,
                                downloadHomeDir,
                                s3SstableObjKeyNames,
                                s3SstableObjKeySizes,
                                s3SstableKSNames,
                                s3SstableTBLNames,
                                s3SstableUniquifiers);

                            threadId++;

                            s3SstableObjKeyNames = new String[SSTABLE_SET_FILENUM];
                            s3SstableObjKeySizes = new long[SSTABLE_SET_FILENUM];
                            s3SstableKSNames = new String[SSTABLE_SET_FILENUM];
                            s3SstableTBLNames = new String[SSTABLE_SET_FILENUM];
                            s3SstableUniquifiers = new String[SSTABLE_SET_FILENUM];

                            executor.execute(worker);
                        }

                        i++;
                    }
                }
            }
        }

        if ( download && ( threadId  < (numSstableBkupItems / SSTABLE_SET_FILENUM) ) ) {
            Runnable worker = new S3ObjDownloadRunnable(
                threadId,
                transferManager,
                bktName,
                downloadHomeDir,
                s3SstableObjKeyNames,
                s3SstableObjKeySizes,
                s3SstableKSNames,
                s3SstableTBLNames,
                s3SstableUniquifiers);

            executor.execute(worker);
        }

        executor.shutdown();

        while (!executor.isTerminated()) {
        }

        if (transferManager != null) {
            transferManager.shutdownNow();
        }

        System.out.println("\n");

    }

    /**
     * Get the local host IP (non 127.0.0.1)
     *
     * @param nicName
     * @return
     */
    static String getLocalIP(String nicName) {

        String localhostIp = null;

        try {
            localhostIp = InetAddress.getLocalHost().getHostAddress();
            // Testing Purpose
            //localhostIp = "10.240.0.6";

            if ( (localhostIp != null) && (localhostIp.startsWith("127.0")) ) {

                NetworkInterface nic = NetworkInterface.getByName(nicName);
                Enumeration<InetAddress> inetAddress = nic.getInetAddresses();

                localhostIp = inetAddress.nextElement().getHostAddress();
            }
        }
        catch (Exception e) { }

        return localhostIp;
    }

    /**
     * List (and download) Opsc S3 backup objects for myself - the host that runs this program
     *
     * @param dseClusterMetadata
     * @param s3Client
     * @param download
     * @param threadNum
     * @param hostIDStr
     * @param keyspaceName
     * @param tableName
     * @param opscBckupTimeGmt
     * @param clearTargetDownDir
     */
    static void listDownloadS3ObjForMe(Metadata dseClusterMetadata,
                                       AmazonS3 s3Client,
                                       boolean download,
                                       int threadNum,
                                       String hostIDStr,
                                       String keyspaceName,
                                       String tableName,
                                       ZonedDateTime opscBckupTimeGmt,
                                       boolean clearTargetDownDir) {
        String myHostId = hostIDStr;

        if ( (hostIDStr == null) || (hostIDStr.isEmpty()) ) {

            String localhostIp = getLocalIP("eth0");

            if (localhostIp == null) {
                System.out.println("\nERROR: failed to get local host IP address!");
                return;
            }

            for (Host host : dseClusterMetadata.getAllHosts()) {
                InetAddress listen_address = host.getListenAddress();
                String listen_address_ip = (listen_address != null) ? host.getListenAddress().getHostAddress() : "";

                InetAddress broadcast_address = host.getListenAddress();
                String broadcast_address_ip = (broadcast_address != null) ? host.getBroadcastAddress().getHostAddress() : "";

                //System.out.println("listen_address: " + listen_address_ip);
                //System.out.println("broadcast_address: " + broadcast_address_ip + "\n");

                if (localhostIp.equals(listen_address_ip) || localhostIp.equals(broadcast_address_ip)) {
                    myHostId = host.getHostId().toString();
                    break;
                }
            }
        }

        if ( myHostId != null && !myHostId.isEmpty() ) {
            //System.out.println("locahost: " + myHostId);

            listDownloadS3ObjForHost(
                dseClusterMetadata,
                s3Client,
                myHostId,
                download,
                threadNum,
                keyspaceName,
                tableName,
                opscBckupTimeGmt,
                clearTargetDownDir
            );
        }
    }

    /**
     * Get mapping from "S3_uniquifier" to "keyspace:table"
     *
     * @param backupJsonFileName
     * @return
     */
    static Map<String, String> getS3UniquifierToKsTblMapping(String backupJsonFileName) {

        HashMap<String, String> s3ObjMaps = new HashMap<String, String>();

        JSONParser jsonParser = new JSONParser();

        try {
            JSONObject jsonObject = (JSONObject) jsonParser.parse(new FileReader(backupJsonFileName));
            JSONArray jsonItemArr = (JSONArray)jsonObject.get(DseOpscS3RestoreUtils.OPSC_S3_OBJKEY_SSTABLES_MARKER_STR);

            Iterator<JSONObject> iterator = jsonItemArr.iterator();

            while (iterator.hasNext()) {
                String jsonItemContent = (String) iterator.next().toJSONString();
                jsonItemContent = jsonItemContent.substring(1, jsonItemContent.length() -1 );

                String[] keyValuePairs = jsonItemContent.split(",");

                String ssTableName = "";
                String uniquifierStr = "";
                String keyspaceName = "";
                String tableName = "";

                for ( String keyValuePair :  keyValuePairs ) {
                    String key = keyValuePair.split(":")[0];
                    String value = keyValuePair.split(":")[1];

                    key = key.substring(1, key.length() - 1 );
                    value = value.substring(1, value.length() - 1);

                    if ( key.equalsIgnoreCase("uniquifier") ) {
                        uniquifierStr = value;
                    }
                    else if ( key.equalsIgnoreCase("keyspace") ) {
                        keyspaceName = value;
                    }
                    else if ( key.equalsIgnoreCase("cf") ) {
                        tableName = value;
                    }
                    else if ( key.equalsIgnoreCase("name") ) {
                        ssTableName = value;
                    }
                }

                s3ObjMaps.put(ssTableName, keyspaceName + ":" + tableName + ":" + uniquifierStr);
            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }

        return s3ObjMaps;
    }


    /**
     * List Opsc S3 backup objects for all DSE cluster hosts
     *
     * @param dseClusterMetadata
     * @param s3Client
     * @param keyspaceName
     * @param tableName
     * @param opscBckupTimeGmt
     */
    static void listS3ObjtForCluster(Metadata dseClusterMetadata,
                                     AmazonS3 s3Client,
                                     String keyspaceName,
                                     String tableName,
                                     ZonedDateTime opscBckupTimeGmt) {

        System.out.format("List OpsCenter S3 backup items for DSE cluster (%s) ...\n", dseClusterMetadata.getClusterName());

        listS3ObjForDC(dseClusterMetadata, s3Client, "", keyspaceName, tableName, opscBckupTimeGmt);
    }

    /**
     * List Opsc S3 backup objects for all hosts in a specified DC
     *
     * @param dseClusterMetadata
     * @param s3Client
     * @param dcName
     * @param keyspaceName
     * @param tableName
     * @param opscBckupTimeGmt
     */
    static void listS3ObjForDC(Metadata dseClusterMetadata,
                               AmazonS3 s3Client,
                               String dcName,
                               String keyspaceName,
                               String tableName,
                               ZonedDateTime opscBckupTimeGmt) {
        assert (CONFIGPROP != null);
        assert ( (keyspaceName != null) && !keyspaceName.isEmpty() );
        assert (opscBckupTimeGmt != null);

        Set<Host> hosts = dseClusterMetadata.getAllHosts();

        boolean dcOnly = ( (dcName != null) && !dcName.isEmpty() );
        if ( dcOnly ) {
            System.out.format("List OpsCenter S3 backup items for specified DC (%s) of DSE cluster (%s) ...\n",
                dcName,
                dseClusterMetadata.getClusterName());
        }

        // Download matching S3 backup.json file to local
        TransferManager transferManager = TransferManagerBuilder.standard().withS3Client(s3Client).build();

        for ( Host host : hosts ) {
            String dc_name = host.getDatacenter();
            String rack_name = host.getRack();
            String host_id = host.getHostId().toString();

            // If not displaying for whole cluster (dcName == null),
            // then only displaying the specified DC
            if ( !dcOnly || (dc_name.equalsIgnoreCase(dcName)) ) {
                System.out.format("  Items for Host %s (rack: %s, DC: %s) ...\n",
                    host_id, rack_name, dc_name, dseClusterMetadata.getClusterName());

                String bktName = CONFIGPROP.getProperty(DseOpscS3RestoreUtils.CFG_KEY_OPSC_S3_BUCKET_NAME);

                String basePrefix = DseOpscS3RestoreUtils.OPSC_S3_OBJKEY_BASESTR + "/" + host_id;

                // First, check OpsCenter records matching the backup time
                String prefixString = basePrefix + "/" +
                    DseOpscS3RestoreUtils.OPSC_S3_OBJKEY_OPSC_MARKER_STR + "_";

                ObjectListing opscObjListing = s3Client.listObjects(
                    new ListObjectsRequest()
                        .withBucketName(bktName)
                        .withPrefix(prefixString));

                DateTimeFormatter s3OpscObjTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd-HH-mm-ss-z");
                String opscBckupTimeGmtStr = opscBckupTimeGmt.format(s3OpscObjTimeFormatter);

                Map<String, String> s3UniquifierToKsTbls = new HashMap<String, String>();

                for (S3ObjectSummary objectSummary : opscObjListing.getObjectSummaries()) {

                    String opscObjName = objectSummary.getKey();
                    String opscObjNameShortZeroSecond =
                        opscObjName.substring(prefixString.length(), prefixString.length() + 16) + "-00-UTC";

                    // Only deal with the S3 objects that fall in the specified OpsCenter backup time range
                    if (opscBckupTimeGmtStr.equalsIgnoreCase(opscObjNameShortZeroSecond)) {
                        System.out.println("  - " + opscObjName + " (size = " + objectSummary.getSize() + " bytes)");

                        // Process OpsCenter backup.json file to get the Keyspace/Table/S3_Identifier mapping
                        if ( opscObjName.contains("backup.json") ) {

                            String downloadHomeDir = CONFIGPROP.getProperty(DseOpscS3RestoreUtils.CFG_KEY_LOCAL_DOWNLOAD_HOME);

                            // If non-existing, create local home directory to hold S3 download files
                            try {
                                File file = new File(downloadHomeDir);

                                if ( Files.notExists(file.toPath()))  {
                                    FileUtils.forceMkdir(file);
                                }
                            }
                            catch (IOException ioe) {
                                System.out.println("ERROR: failed to create download home directory for S3 objects!");
                                System.exit(-10);
                            }

                            String localBackupJsonFile = downloadHomeDir + "/" + opscObjName;
                            downloadSingleS3Obj(transferManager,
                                localBackupJsonFile,
                                bktName,
                                opscObjName,
                                objectSummary.getSize(),
                                false );

                            // After downloaded the backup.json file, process its content to get mapping between
                            // S3 Uniquifier to Keyspace-Table.
                            s3UniquifierToKsTbls = getS3UniquifierToKsTblMapping(localBackupJsonFile);
                        }
                    }
                }

                // Second, check SSTables records matching the backup time, keyspace, and table
                prefixString = basePrefix + "/" +
                    DseOpscS3RestoreUtils.OPSC_S3_OBJKEY_SSTABLES_MARKER_STR;

                ObjectListing sstableObjListing = s3Client.listObjects(
                    new ListObjectsRequest()
                        .withBucketName(bktName)
                        .withPrefix(prefixString));

                for (S3ObjectSummary objectSummary : sstableObjListing.getObjectSummaries()) {
                    String sstableObjName = objectSummary.getKey();
                    sstableObjName = sstableObjName.substring(sstableObjName.lastIndexOf("/") + 1);

                    if (s3UniquifierToKsTbls.containsKey(sstableObjName)) {
                        String[] ksTblUniquifer = s3UniquifierToKsTbls.get(sstableObjName).split(":");
                        String ks = ksTblUniquifer[0];
                        String tbl = ksTblUniquifer[1];

                        boolean filterKsTbl = keyspaceName.equalsIgnoreCase(ks);
                        if ( (tableName != null) && !tableName.isEmpty() ) {
                            filterKsTbl = filterKsTbl && tableName.equalsIgnoreCase(tbl);
                        }

                        if (filterKsTbl) {
                            System.out.println("  - " + objectSummary.getKey() + " (size = " + objectSummary.getSize()
                                + " bytes) [keyspace: " + ks + "; table: " + tbl + "]");
                        }
                    }
                }
            }

            System.out.println();
        }

        if (transferManager != null) {
            transferManager.shutdownNow();
        }
    }


    /**
     *  Define Command Line Arguments
     */
    static Options options = new Options();

    static {
        Option helpOption = new Option(
            DseOpscS3RestoreUtils.CMD_OPTION_HELP_SHORT,
            DseOpscS3RestoreUtils.CMD_OPTION_HELP_LONG,
            false,
            "Displays this help message.");
        Option listOption = new Option(
            DseOpscS3RestoreUtils.CMD_OPTION_LIST_SHORT,
            DseOpscS3RestoreUtils.CMD_OPTION_LIST_LONG,
            true,
            "List OpsCenter S3 backup items (all | DC:\"<dc_name>\" | me[:\"<host_id_string>\"]).");
        Option downloadOption = new Option(
            DseOpscS3RestoreUtils.CMD_OPTION_DOWNLOAD_SHORT,
            DseOpscS3RestoreUtils.CMD_OPTION_DOWNLOAD_LONG,
            true,
            "Download OpsCenter S3 bakcup items to local directory (only applies to \"list me\" case");
        Option fileOption = new Option(
            DseOpscS3RestoreUtils.CMD_OPTION_CFG_SHORT,
            DseOpscS3RestoreUtils.CMD_OPTION_CFG_LONG,
            true,
            "Configuration properties file path");
        Option keyspaceOption = new Option(
            DseOpscS3RestoreUtils.CMD_OPTION_KEYSPACE_SHORT,
            DseOpscS3RestoreUtils.CMD_OPTION_KEYSPACE_LONG,
            true,
            "Keyspace name to be restored");
        Option tableOption = new Option(
            DseOpscS3RestoreUtils.CMD_OPTION_TABLE_SHORT,
            DseOpscS3RestoreUtils.CMD_OPTION_TABLE_LONG,
            true,
            "Table name to be restored");
        Option opscBkupTimeOPtion = new Option(
            DseOpscS3RestoreUtils.CMD_OPTION_BACKUPTIME_SHORT,
            DseOpscS3RestoreUtils.CMD_OPTION_BACKUPTIME_LONG,
            true,
            "OpsCetner backup datetime");
        Option clsTargetDirOPtion = new Option(
            DseOpscS3RestoreUtils.CMD_OPTION_CLSDOWNDIR_SHORT,
            DseOpscS3RestoreUtils.CMD_OPTION_CLSDOWNDIR_LONG,
            true,
            "OpsCetner backup datetime");

        options.addOption(helpOption);
        options.addOption(listOption);
        options.addOption(fileOption);
        options.addOption(downloadOption);
        options.addOption(keyspaceOption);
        options.addOption(tableOption);
        options.addOption(opscBkupTimeOPtion);
        options.addOption(clsTargetDirOPtion);
    }

    /**
     * Print out usage info. and exit
     */
    static void usageAndExit(int errorCode) {

        System.out.println();

        PrintWriter errWriter = new PrintWriter(System.out, true);

        try {
            HelpFormatter formatter = new HelpFormatter();

            formatter.printHelp(errWriter, 150, "DseOpscS3Restore",
                String.format("\nDseOpscS3Restore Options:"),
                options, 2, 1, "", true);

            System.out.println();
            System.out.println();
        }
        catch ( Exception e ) {
            e.printStackTrace();
        }
        finally
        {
            System.exit(errorCode);
        }
    }
    static void usageAndExit() {
        usageAndExit(0);
    }


    /**
     * Main function - java program entry point
     *
     * @param args
     */
    public static void main(String[] args) {

        /**
         *  Parsing commandline parameters (starts) ---->
         */

        DefaultParser parser = new DefaultParser();
        CommandLine cmd = null;

        try {
            cmd = parser.parse(options, args);
        }
        catch (ParseException e) {
            System.err.format("\nERROR: Failure parsing argument inputs: %s.\n", e.getMessage());
            usageAndExit(-10);
        }

        // Print help message
        if ( cmd.hasOption(DseOpscS3RestoreUtils.CMD_OPTION_HELP_SHORT) ) {
            usageAndExit();
        }

        // "-c" option (Configuration File) is a must!
        String cfgFilePath = cmd.getOptionValue(DseOpscS3RestoreUtils.CMD_OPTION_CFG_SHORT);
        if ( (cfgFilePath == null) || cfgFilePath.isEmpty() ) {
            System.err.println("\nERROR: Please specify a valid configuration file path as the \"-" +
                DseOpscS3RestoreUtils.CMD_OPTION_CFG_SHORT + " option value.\n");
            usageAndExit(-20);
        }

        // "-l" option (ALL | DC:"<DC_Name>" | me[:"<C*_node_host_id>" is a must!
        boolean listCluster = false;
        boolean listDC = false;
        boolean listMe = false;

        String dcNameToList = "";
        String myHostID = "";

        String lOptVal = cmd.getOptionValue(DseOpscS3RestoreUtils.CMD_OPTION_LIST_SHORT);
        if ( (lOptVal == null) || lOptVal.isEmpty() ) {
            System.out.println("\nERROR: Please specify proper value for \"-" +
                DseOpscS3RestoreUtils.CMD_OPTION_LIST_SHORT + "\" option -- " +
                DseOpscS3RestoreUtils.CMD_OPTION_LIST_ALL + " | " +
                DseOpscS3RestoreUtils.CMD_OPTION_LIST_DC + ":\"<DC_Name>\" | " +
                DseOpscS3RestoreUtils.CMD_OPTION_LIST_ME + "[:\"<host_id>\"].\n");
            usageAndExit(-30);
        }

        if ( lOptVal.equalsIgnoreCase(DseOpscS3RestoreUtils.CMD_OPTION_LIST_ALL) ) {
            listCluster = true;
        }
        else if ( lOptVal.toUpperCase().startsWith(DseOpscS3RestoreUtils.CMD_OPTION_LIST_DC) ) {
            listDC = true;

            String[] strSplits = lOptVal.split(":");
            if (strSplits.length != 2) {
                System.out.println("\nERROR: Please specify proper value for \"-" +
                    DseOpscS3RestoreUtils.CMD_OPTION_LIST_SHORT + " " + DseOpscS3RestoreUtils.CMD_OPTION_LIST_DC +
                    "\" option -- DC:\"<DC_Name>\".\n");
                usageAndExit(-40);
            }
            else {
                dcNameToList = strSplits[1];
            }
        }
        else if ( lOptVal.toUpperCase().startsWith(DseOpscS3RestoreUtils.CMD_OPTION_LIST_ME) ) {
            listMe = true;

            String[] strSplits = lOptVal.split(":");
            if ( lOptVal.contains(":") && (strSplits.length != 2) ) {
                System.out.println("\nERROR: Please specify proper value for \"-" +
                    DseOpscS3RestoreUtils.CMD_OPTION_LIST_SHORT + " " + DseOpscS3RestoreUtils.CMD_OPTION_LIST_ME +
                    "\" option -- [:\"<specified_host_id_string>\"].\n");
                usageAndExit(-50);
            }
            else if ( lOptVal.contains(":") ) {
                myHostID = lOptVal.split(":")[1];
            }
        }
        else {
            System.out.println("\nERROR: Please specify proper value for \"-" +
                DseOpscS3RestoreUtils.CMD_OPTION_LIST_SHORT + "\" option -- " +
                DseOpscS3RestoreUtils.CMD_OPTION_LIST_ALL + " | " +
                DseOpscS3RestoreUtils.CMD_OPTION_LIST_DC + ":\"<DC_Name>\" | " +
                DseOpscS3RestoreUtils.CMD_OPTION_LIST_ME + "[:\"<host_id>\"].\n");
            usageAndExit(-60);
        }

        // Download option ONLY works for "-l me" option! If "-d" option value is not specified, use the default value
        boolean downloadS3Obj = false;
        int downloadS3ObjThreadNum = DseOpscS3RestoreUtils.DOWNLOAD_THREAD_POOL_SIZE;

        if ( cmd.hasOption(DseOpscS3RestoreUtils.CMD_OPTION_DOWNLOAD_SHORT) ) {
            downloadS3Obj = true;

            String dOptVal = cmd.getOptionValue(DseOpscS3RestoreUtils.CMD_OPTION_DOWNLOAD_SHORT);
            if ( (dOptVal != null) && (!dOptVal.isEmpty()) ) {
                try {
                    downloadS3ObjThreadNum = Integer.parseInt(dOptVal);
                }
                catch (NumberFormatException nfe) {
                    System.out.println("\nWARN: Incorrect \"-" + DseOpscS3RestoreUtils.CMD_OPTION_DOWNLOAD_SHORT +
                        "\" option value -- must be a positive integer! Using default value (" +
                        DseOpscS3RestoreUtils.DOWNLOAD_THREAD_POOL_SIZE + ").\n");
                }
            }
        }

        // "-k" option (Keyspace name) is a must
        String keyspaceName = cmd.getOptionValue(DseOpscS3RestoreUtils.CMD_OPTION_KEYSPACE_SHORT);
        if ( (keyspaceName == null) || keyspaceName.isEmpty() ) {
            System.out.println("\nERROR: Please specify proper keypsace name as the \"-" +
                DseOpscS3RestoreUtils.CMD_OPTION_KEYSPACE_SHORT + "\" option value.\n");
            usageAndExit(-70);
        }

        // "-t" option (Table name) is optional. If not specified, all Tables of the specified keyspaces will be processed.
        String tableName = cmd.getOptionValue(DseOpscS3RestoreUtils.CMD_OPTION_TABLE_SHORT);

        // "-obt" option is a must
        // OpsCenter Backup Date Time String (Can get  from OpsCenter Backup Service Window)
        String obtOptOptValue = cmd.getOptionValue(DseOpscS3RestoreUtils.CMD_OPTION_BACKUPTIME_SHORT);

        if ( (obtOptOptValue == null) || (obtOptOptValue.isEmpty()) ) {
            System.out.println("\nERROR: Please specify proper OpsCenter S3 backup time string (M/d/yyyy h:m a) as the \"-" +
                DseOpscS3RestoreUtils.CMD_OPTION_BACKUPTIME_SHORT + "\" option value.");
            usageAndExit(-80);
        }

        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("M/d/yyyy h:m a");
        ZonedDateTime opscBackupTime_gmt = null;
        try {
            LocalDateTime ldt = LocalDateTime.parse(obtOptOptValue, formatter);

            ZoneId gmtZoneId = ZoneId.of("UTC");
            opscBackupTime_gmt = ldt.atZone(gmtZoneId);
        }
        catch (DateTimeParseException dte) {
            dte.printStackTrace();

            System.out.println("\nERROR: Please specify correct time string format (M/d/yyyy h:m a) for \"-" +
                DseOpscS3RestoreUtils.CMD_OPTION_BACKUPTIME_SHORT + "\" option.");
            //usageAndExit(-80);
        }

        // "-cls" option is optional
        boolean clearTargetDownDir = false;
        String clsOptOptValue = cmd.getOptionValue(DseOpscS3RestoreUtils.CMD_OPTION_CLSDOWNDIR_SHORT);

        if ( (clsOptOptValue != null) && (!clsOptOptValue.isEmpty()) ) {
            try {
                clearTargetDownDir = Boolean.parseBoolean(clsOptOptValue);
                clearTargetDownDir = true;
            }
            catch (NumberFormatException nfe) {
            }
        }


        /**
         *  Parsing commandline parameters (ends)  <----
         */



        /**
         * Load configuration files
         */

        CONFIGPROP = DseOpscS3RestoreUtils.LoadConfigFile(cfgFilePath);
        if (CONFIGPROP == null) {
            System.exit(-50);
        }

        /**
         * Verify AWS credential
         */

        AWSCredentials credentials = null;

        // Check AWS credential from default credential profile file
        try {
            credentials = new ProfileCredentialsProvider("default").getCredentials();
        }
        catch ( Exception e) {

        }

        // When failed, check whether AWS credential is provided from Java system properties:
        // -Daws.accessKeyId=<AWS_access_key>, and
        // -Daws.secretKey=<AWS_secret_key>
        if (credentials == null) {
            try {
                credentials = new SystemPropertiesCredentialsProvider().getCredentials();
            }
            catch ( Exception e ) {

            }
        }

        if (credentials == null) {
            System.err.println("\nERROR: Failed to set up AWS S3 connection! Please check provided AWS access key and secret key!");
            usageAndExit(-50);
        }

        /**
         * Set up AWS S3 connection
         */
        AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
            .withCredentials(new AWSStaticCredentialsProvider(credentials))
            .withRegion(CONFIGPROP.getProperty(DseOpscS3RestoreUtils.CFG_KEY_OPSC_S3_AWS_REGION))
            .build();

        /*
        // Test S3 connection - list all S3 buckets under the provided AWS account
        System.out.println("Listing buckets");
        for (Bucket bucket : s3Client.listBuckets()) {
            System.out.println(" - " + bucket.getName());
        }
        System.out.println();
        */


        /**
         * Get Dse cluster metadata
         */
        QueryOptions queryOptions = new QueryOptions();
        queryOptions.setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);

        DseCluster dseCluster = DseCluster.builder()
            .addContactPoint(CONFIGPROP.getProperty(DseOpscS3RestoreUtils.CFG_KEY_CONTACT_POINT))
            .withQueryOptions(queryOptions)
            .build();

        // dseCluster.connect();    /* NO NEED for acutal connection */
        Metadata dseClusterMetadata = dseCluster.getMetadata();

        // List Opsc S3 backup items for all Dse Cluster hosts
        if ( listCluster ) {
            listS3ObjtForCluster(dseClusterMetadata, s3Client, keyspaceName, tableName, opscBackupTime_gmt);
        }
        // List Opsc S3 backup items for all hosts in a specified DC of the Dse cluster
        else if ( listDC ) {
            listS3ObjForDC(dseClusterMetadata, s3Client, dcNameToList, keyspaceName, tableName, opscBackupTime_gmt);
        }
        // List (and download) Opsc S3 backup items for myself (the host that runs this program)
        else if ( listMe ) {
            listDownloadS3ObjForMe(
                dseClusterMetadata,
                s3Client,
                downloadS3Obj,
                downloadS3ObjThreadNum,
                myHostID,
                keyspaceName,
                tableName,
                opscBackupTime_gmt,
                clearTargetDownDir);
        }

        if (s3Client != null) {
            s3Client.shutdown();
        }

        System.exit(0);
    }
}