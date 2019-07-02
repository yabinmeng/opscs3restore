package com.dsetools;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
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
import com.datastax.driver.core.*;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.dse.DseCluster;
import org.apache.commons.cli.*;
import org.apache.commons.io.FileUtils;
import org.json.simple.JSONArray;
import org.json.simple.parser.JSONParser;
import org.json.simple.JSONObject;

import java.io.*;
import java.net.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.*;
import java.time.Duration;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


class S3ObjDownloadRunnable implements  Runnable {
    private int threadID;
    private TransferManager s3TransferManager;
    private boolean fileSizeChk;
    private String s3BuketName;
    private String downloadHomeDir;
    private String[] s3ObjNames;
    private long[] s3ObjSizes;
    private String[] keyspaceNames;
    private String[] tableNames;
    private String[] sstableVersions;
    private boolean noTargetDirStruct;

    private DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    S3ObjDownloadRunnable( int tID,
                           TransferManager transferManager,
                           boolean file_size_chk,
                           String s3bkt_name,
                           String download_dir,
                           String[] object_names,
                           long[] object_sizes,
                           String[] ks_names,
                           String[] tbl_names,
                           String[] sstable_versions,
                           boolean no_dir_struct) {
        assert (tID > 0);
        assert (transferManager != null);

        this.threadID = tID;
        this.s3TransferManager = transferManager;
        this.fileSizeChk = file_size_chk;
        this.s3BuketName = s3bkt_name;
        this.downloadHomeDir = download_dir;
        this.s3ObjNames = object_names;
        this.s3ObjSizes = object_sizes;
        this.keyspaceNames = ks_names;
        this.tableNames = tbl_names;
        this.sstableVersions = sstable_versions;
        this.noTargetDirStruct = no_dir_struct;

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
                int sstblVersionStartPos = s3ObjNames[i].indexOf(sstableVersions[i]);
                String realSStableName = s3ObjNames[i].substring(sstblVersionStartPos);

                String tmp = s3ObjNames[i].substring(0, sstblVersionStartPos -1 );
                int lastPathSeperatorPos = tmp.lastIndexOf('/');

                String parentPathStr = tmp.substring(0, lastPathSeperatorPos);
                parentPathStr = parentPathStr.substring(parentPathStr.indexOf(DseOpscS3RestoreUtils.OPSC_OBJKEY_BASESTR));

                File localFile = new File(downloadHomeDir + "/" +
                    ( noTargetDirStruct ? "" :
                        (parentPathStr + "/" + keyspaceNames[i] + "/" + tableNames[i] + "/") ) +
                    realSStableName );


                GetObjectRequest getObjectRequest = new GetObjectRequest(s3BuketName, s3ObjNames[i]);

                Download s3Download = s3TransferManager.download(getObjectRequest, localFile);

                // wait for download to complete
                s3Download.waitForCompletion();

                downloadedS3ObjNum++;

                System.out.format("     [Thread %d] download of \"%s\" completed \n", threadID,
                    s3ObjNames[i] + "[keyspace: " + keyspaceNames[i] + "; table: " + tableNames[i] + "]");

                if (fileSizeChk) {
                    TransferProgress transferProgress = s3Download.getProgress();
                    System.out.format("        >>> %d of %d bytes transferred (%.2f%%)\n",
                        transferProgress.getBytesTransferred(),
                        s3ObjSizes[i],
                        transferProgress.getPercentTransferred());
                }
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

    private static Properties CONFIGPROP = null;

    /**
     * Get the full file path of the "backup.json" file that corresponds
     * to the specified DSE Host ID and OpsCenter backup time
     *
     * @param s3Client
     * @param hostId
     * @param opscBckupTimeGmt
     * @return
     */
    static S3ObjectSummary getMyBackupJson(AmazonS3 s3Client,
                                           String hostId,
                                           ZonedDateTime opscBckupTimeGmt)
    {
        DateTimeFormatter opscObjTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd-HH-mm-ss-z");
        String opscBckupTimeGmtStr = opscBckupTimeGmt.format(opscObjTimeFormatter);

        String bktName = CONFIGPROP.getProperty(DseOpscS3RestoreUtils.CFG_KEY_OPSC_S3_BUCKET_NAME);
        String basePrefix = DseOpscS3RestoreUtils.OPSC_OBJKEY_BASESTR + "/" + hostId;
        String opscPrefixString = basePrefix + "/" +
            DseOpscS3RestoreUtils.OPSC_OBJKEY_OPSC_MARKER_STR + "_";

        ObjectListing objectListing = s3Client.listObjects(
            new ListObjectsRequest()
                .withBucketName(bktName)
                .withPrefix(opscPrefixString));

        List<S3ObjectSummary>  s3ObjectSummaries = objectListing.getObjectSummaries();

        S3ObjectSummary backupJsonS3ObjeSummary = null;
        for (S3ObjectSummary objectSummary : s3ObjectSummaries) {

            String opscObjName = objectSummary.getKey();
            String backupTimeShortZeroSecondStr =
                opscObjName.substring(opscPrefixString.length(), opscPrefixString.length() + 16) + "-00-UTC";

            if (opscBckupTimeGmtStr.equalsIgnoreCase(backupTimeShortZeroSecondStr)) {
                backupJsonS3ObjeSummary = objectSummary;
                break;
            }
        }

        return backupJsonS3ObjeSummary;
    }


    /**
     *  Get file size of a file
     *
     * @param s3Client
     * @param s3ObjKeyName
     * @return
     */
    static long getS3FileSize(AmazonS3 s3Client, String s3BucketName, String s3ObjKeyName) {
        long size = -1;

        ObjectListing objectListing = s3Client.listObjects(
            new ListObjectsRequest()
                .withBucketName(s3BucketName)
                .withPrefix(s3ObjKeyName));

        List<S3ObjectSummary>  s3ObjectSummaries = objectListing.getObjectSummaries();

        if (! s3ObjectSummaries.isEmpty()) {
            assert (s3ObjectSummaries.size() == 1);
            size = s3ObjectSummaries.get(0).getSize();
        }

        return size;
    }

    /**
     * Get mapping from "unique_identifier" to "keyspace:table"
     *
     * @param backupJsonFileName
     * @return
     */
    static Map<String, String> getOpscUniquifierToKsTblMapping(String backupJsonFileName) {

        LinkedHashMap<String, String> opscObjMaps = new LinkedHashMap<String, String>();

        JSONParser jsonParser = new JSONParser();

        try {
            JSONObject jsonObject = (JSONObject) jsonParser.parse(new FileReader(backupJsonFileName));
            JSONArray jsonItemArr = (JSONArray)jsonObject.get(DseOpscS3RestoreUtils.OPSC_OBJKEY_SSTABLES_MARKER_STR);

            Iterator iterator = jsonItemArr.iterator();

            while (iterator.hasNext()) {
                String jsonItemContent = (String) ((JSONObject)iterator.next()).toJSONString();
                jsonItemContent = jsonItemContent.substring(1, jsonItemContent.length() -1 );

                String[] keyValuePairs = jsonItemContent.split(",");

                String ssTableName = "";
                String uniquifierStr = "";
                String keyspaceName = "";
                String tableName = "";
                String ssTableVersion = "";

                for ( String keyValuePair :  keyValuePairs ) {
                    String key = keyValuePair.split(":")[0];
                    String value = keyValuePair.split(":")[1];

                    key = key.substring(1, key.length() - 1 );
                    value = value.substring(1, value.length() - 1);

                    if ( key.equalsIgnoreCase("uniquifier") ) {
                        uniquifierStr = value;
                    }
                    else if ( key.equalsIgnoreCase("version") ) {
                        ssTableVersion = value;
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

                opscObjMaps.put(ssTableName, keyspaceName + ":" + tableName + ":" + uniquifierStr + ":" + ssTableVersion);
            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }

        return opscObjMaps;
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
     * Find my DSE host ID by checking with DSE cluster
     *
     * @param dseClusterMetadata
     * @return
     */
    static String findMyHostID(Metadata dseClusterMetadata) {
        assert (dseClusterMetadata != null);

        String myHostId = null;

        String localhostIp = getLocalIP(CONFIGPROP.getProperty(DseOpscS3RestoreUtils.CFG_KEY_IP_MATCHING_NIC));

        if (localhostIp == null) {
            System.out.println("\nERROR: failed to get local host IP address!");
        }
        else {
            boolean foundMatchingHost = false;
            for (Host host : dseClusterMetadata.getAllHosts()) {
                InetAddress listen_address = host.getListenAddress();
                String listen_address_ip = (listen_address != null) ? listen_address.getHostAddress() : "";

                InetAddress broadcast_address = host.getBroadcastAddress();
                String broadcast_address_ip = (broadcast_address != null) ? broadcast_address.getHostAddress() : "";

                //System.out.println("listen_address: " + listen_address_ip);
                //System.out.println("broadcast_address: " + broadcast_address_ip + "\n");

                if (localhostIp.equals(listen_address_ip) || localhostIp.equals(broadcast_address_ip)) {
                    myHostId = host.getHostId().toString();
                    foundMatchingHost = true;
                    break;
                }
            }

            if (!foundMatchingHost) {
                System.out.format("\nERROR: Failed to match my DSE host address by IP (NIC Name: %s; NIC IP: %s)!\n",
                    CONFIGPROP.getProperty(DseOpscS3RestoreUtils.CFG_KEY_IP_MATCHING_NIC),
                    localhostIp);
            }
        }

        return myHostId;
    }
    

    /**
     * Download a single S3 object to a local file.
     *
     * @param s3TransferManager
     * @param localFilePath
     * @param s3BukcetName
     * @param s3ObjKeyName
     * @param s3ObjeKeySize
     * @param file_size_chk
     * @param printMsg
     */
    static void downloadSingleS3Obj(TransferManager s3TransferManager,
                                    String localFilePath,
                                    String s3BukcetName,
                                    String s3ObjKeyName,
                                    long s3ObjeKeySize,
                                    boolean file_size_chk,
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
                System.out.println("   ... Download failed - unknown error.");
            }
            ex.printStackTrace();
        }

        if (file_size_chk) {
            TransferProgress transferProgress = s3Download.getProgress();
            if (printMsg) {
                System.out.format("   ... download complete: %d of %d bytes transferred (%.2f%%)\n",
                    transferProgress.getBytesTransferred(),
                    s3ObjeKeySize,
                    transferProgress.getPercentTransferred());
            }
        }
    }


    /**
     * List (and download) Opsc S3 backup objects for a specified host
     *
     * @param fileSizeChk
     * @param s3Client
     * @param hostId
     * @param download
     * @param threadNum
     * @param keyspaceName
     * @param tableName
     * @param opscBckupTimeGmt
     * @param clearTargetDownDir
     * @param noTargetDirStruct
     */
    static void listDownloadS3ObjForHost(boolean fileSizeChk,
                                         AmazonS3 s3Client,
                                         String hostId,
                                         boolean download,
                                         int threadNum,
                                         String keyspaceName,
                                         String tableName,
                                         ZonedDateTime opscBckupTimeGmt,
                                         boolean clearTargetDownDir,
                                         boolean noTargetDirStruct) {
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


        // First, check OpsCenter records matching the backup time
        TransferManager transferManager =
            TransferManagerBuilder.standard().withS3Client(s3Client).build();

        String bktName = CONFIGPROP.getProperty(DseOpscS3RestoreUtils.CFG_KEY_OPSC_S3_BUCKET_NAME);

        Map<String, String> opscUniquifierToKsTbls = new HashMap<String, String>();

        S3ObjectSummary backupJsonS3ObjSummary = getMyBackupJson(s3Client, hostId, opscBckupTimeGmt);

        if (backupJsonS3ObjSummary == null) {
            DateTimeFormatter opscObjTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd-HH-mm-ss-z");
            String opscBckupTimeGmtStr = opscBckupTimeGmt.format(opscObjTimeFormatter);

            System.out.format("ERROR: Failed to find %s file for host (%s) at backup time (%s)\n",
                DseOpscS3RestoreUtils.OPSC_BKUP_METADATA_FILE,
                hostId,
                opscBckupTimeGmtStr);

            return;
        }
        else {
            // download backup.json file from S3
            String objKeyName = backupJsonS3ObjSummary.getKey();
            String localBackupJsonFile = downloadHomeDir + "/" + objKeyName;

            boolean downloadSucceed = false;

            try {
                downloadSingleS3Obj(transferManager,
                    localBackupJsonFile,
                    bktName,
                    objKeyName,
                    backupJsonS3ObjSummary.getSize(),
                    fileSizeChk,
                    true);

                downloadSucceed = true;

            } catch (AmazonServiceException e) {
                // The call was transmitted successfully, but Amazon S3 couldn't process
                // it, so it returned an error response.
                e.printStackTrace();
            } catch (SdkClientException e) {
                // Amazon S3 couldn't be contacted for a response, or the client
                // couldn't parse the response from Amazon S3.
                e.printStackTrace();
            }

            // processing backup.json metadata when it is successfully downloaded from S3
            if (downloadSucceed) {
                opscUniquifierToKsTbls =
                    getOpscUniquifierToKsTblMapping(localBackupJsonFile);
            }
        }


        if ( opscUniquifierToKsTbls.isEmpty() ) {
            System.out.println("ERROR: Failed to get backup SSTable file list from " +
                DseOpscS3RestoreUtils.OPSC_BKUP_METADATA_FILE + " file!");
            return;
        }


        // Download SSTable S3 object items
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
        String[] s3SstableVersions = new String[SSTABLE_SET_FILENUM];

        int i = 0;
        int threadId = 0;

        String sstablePrefixString =
            DseOpscS3RestoreUtils.OPSC_OBJKEY_BASESTR + "/" +
                hostId + "/" +
                DseOpscS3RestoreUtils.OPSC_OBJKEY_SSTABLES_MARKER_STR;


        for ( String sstableObjName : opscUniquifierToKsTbls.keySet() )  {

            String opscObjName = sstablePrefixString + "/" + sstableObjName;

            String[] ksTblUniquifer = opscUniquifierToKsTbls.get(sstableObjName).split(":");
            String ks = ksTblUniquifer[0];
            String tbl = ksTblUniquifer[1];
            String version = ksTblUniquifer[3];

            boolean filterKsTbl = keyspaceName.equalsIgnoreCase(ks);
            if ((tableName != null) && !tableName.isEmpty()) {
                filterKsTbl = filterKsTbl && tableName.equalsIgnoreCase(tbl);
            }

            if (filterKsTbl) {
                numSstableBkupItems++;

                long opscObjSize = -1;
                if (fileSizeChk) {
                    opscObjSize = getS3FileSize(s3Client, bktName, opscObjName);
                }

                System.out.println("  - [" + bktName + "] " + opscObjName +
                    ( !fileSizeChk ? "" : (" (size = " + opscObjSize + " bytes)") ) +
                    " [keyspace: " + ks + "; table: " + tbl + "]");

                s3SstableObjKeyNames[i % SSTABLE_SET_FILENUM] = opscObjName;
                s3SstableObjKeySizes[i % SSTABLE_SET_FILENUM] = opscObjSize;
                s3SstableKSNames[i % SSTABLE_SET_FILENUM] = ks;
                s3SstableTBLNames[i % SSTABLE_SET_FILENUM] = tbl;
                s3SstableVersions[i % SSTABLE_SET_FILENUM] = version;

                if (download) {
                    if ((i > 0) && ((i + 1) % SSTABLE_SET_FILENUM == 0)) {
                        Runnable worker = new S3ObjDownloadRunnable(
                            threadId,
                            transferManager,
                            fileSizeChk,
                            bktName,
                            downloadHomeDir,
                            s3SstableObjKeyNames,
                            s3SstableObjKeySizes,
                            s3SstableKSNames,
                            s3SstableTBLNames,
                            s3SstableVersions,
                            noTargetDirStruct);

                        threadId++;

                        s3SstableObjKeyNames = new String[SSTABLE_SET_FILENUM];
                        s3SstableObjKeySizes = new long[SSTABLE_SET_FILENUM];
                        s3SstableKSNames = new String[SSTABLE_SET_FILENUM];
                        s3SstableTBLNames = new String[SSTABLE_SET_FILENUM];
                        s3SstableVersions[i % SSTABLE_SET_FILENUM] = version;

                        executor.execute(worker);
                    }

                    i++;
                }
            }
        }

        if ( download && ( threadId  < (numSstableBkupItems / SSTABLE_SET_FILENUM) ) ) {
            Runnable worker = new S3ObjDownloadRunnable(
                threadId,
                transferManager,
                fileSizeChk,
                bktName,
                downloadHomeDir,
                s3SstableObjKeyNames,
                s3SstableObjKeySizes,
                s3SstableKSNames,
                s3SstableTBLNames,
                s3SstableVersions,
                noTargetDirStruct);

            executor.execute(worker);
        }

        executor.shutdown();

        while (!executor.isTerminated()) {
        }

        if (numSstableBkupItems == 0) {
            System.out.println("  - Found no matching backup records for the specified conditions!.");
        }

        if (transferManager != null) {
            transferManager.shutdownNow();
        }

        System.out.println("\n");

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
                                       boolean fileSizeChk,
                                       AmazonS3 s3Client,
                                       boolean download,
                                       int threadNum,
                                       String hostIDStr,
                                       String keyspaceName,
                                       String tableName,
                                       ZonedDateTime opscBckupTimeGmt,
                                       boolean clearTargetDownDir,
                                       boolean noTargetDirStruct) {
        String myHostId = hostIDStr;

        // When not providing DSE host ID explicitly, find it by checking with DSE cluster
        if ( (hostIDStr == null) || (hostIDStr.isEmpty()) ) {
            myHostId = findMyHostID(dseClusterMetadata);
        }

        if ( myHostId != null && !myHostId.isEmpty() ) {
            listDownloadS3ObjForHost(
                fileSizeChk,
                s3Client,
                myHostId,
                download,
                threadNum,
                keyspaceName,
                tableName,
                opscBckupTimeGmt,
                clearTargetDownDir,
                noTargetDirStruct
            );
        }
    }


    /**
     * List Opsc S3 backup objects for all DSE cluster hosts
     *
     * @param dseClusterMetadata
     * @param fileSizeChk
     * @param s3Client
     * @param keyspaceName
     * @param tableName
     * @param opscBckupTimeGmt
     */
    static void listS3ObjtForCluster(Metadata dseClusterMetadata,
                                     boolean fileSizeChk,
                                     AmazonS3 s3Client,
                                     String keyspaceName,
                                     String tableName,
                                     ZonedDateTime opscBckupTimeGmt) {

        System.out.format("List OpsCenter S3 backup items for DSE cluster (%s) ...\n", dseClusterMetadata.getClusterName());

        listS3ObjForDC(dseClusterMetadata, fileSizeChk, s3Client, "", keyspaceName, tableName, opscBckupTimeGmt);
    }

    /**
     * List Opsc S3 backup objects for all hosts in a specified DC
     *
     * @param dseClusterMetadata
     * @param fileSizeChk
     * @param s3Client
     * @param dcName
     * @param keyspaceName
     * @param tableName
     * @param opscBckupTimeGmt
     */
    static void listS3ObjForDC(Metadata dseClusterMetadata,
                               boolean fileSizeChk,
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
        TransferManager transferManager =
            TransferManagerBuilder.standard().withS3Client(s3Client).build();

        String bktName = CONFIGPROP.getProperty(DseOpscS3RestoreUtils.CFG_KEY_OPSC_S3_BUCKET_NAME);

        for ( Host host : hosts ) {
            int numSstableBkupItems = 0;

            String dc_name = host.getDatacenter();
            String rack_name = host.getRack();
            String host_id = host.getHostId().toString();

            // If not displaying for whole cluster (dcName == null),
            // then only displaying the specified DC
            if ( !dcOnly || (dc_name.equalsIgnoreCase(dcName)) ) {
                System.out.format("  Items for Host %s (rack: %s, DC: %s) ...\n",
                    host_id, rack_name, dc_name, dseClusterMetadata.getClusterName());


                // First. get the backup.json file corresponds to the specified host and backup time

                Map<String, String> opscUniquifierToKsTbls = new HashMap<>();

                S3ObjectSummary backupJsonS3ObjSummary = getMyBackupJson(s3Client, host_id, opscBckupTimeGmt);

                if (backupJsonS3ObjSummary == null) {
                    DateTimeFormatter opscObjTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd-HH-mm-ss-z");
                    String opscBckupTimeGmtStr = opscBckupTimeGmt.format(opscObjTimeFormatter);

                    System.out.format("ERROR: Failed to find %s file for host (%s) at backup time (%s)\n",
                        DseOpscS3RestoreUtils.OPSC_BKUP_METADATA_FILE,
                        host_id,
                        opscBckupTimeGmtStr);

                    return;
                }
                else {
                    // download backup.json file from S3
                    String objKeyName = backupJsonS3ObjSummary.getKey();
                    String localBackupJsonFile =
                        CONFIGPROP.getProperty(DseOpscS3RestoreUtils.CFG_KEY_LOCAL_DOWNLOAD_HOME) +
                        "/" +
                        objKeyName;

                    boolean downloadSucceed = false;

                    try {
                        downloadSingleS3Obj(transferManager,
                            localBackupJsonFile,
                            bktName,
                            objKeyName,
                            backupJsonS3ObjSummary.getSize(),
                            fileSizeChk,
                            true);

                        downloadSucceed = true;

                    } catch (AmazonServiceException e) {
                        // The call was transmitted successfully, but Amazon S3 couldn't process
                        // it, so it returned an error response.
                        e.printStackTrace();
                    } catch (SdkClientException e) {
                        // Amazon S3 couldn't be contacted for a response, or the client
                        // couldn't parse the response from Amazon S3.
                        e.printStackTrace();
                    }

                    // processing backup.json metadata when it is successfully downloaded from S3
                    if (downloadSucceed) {
                        opscUniquifierToKsTbls =
                            getOpscUniquifierToKsTblMapping(localBackupJsonFile);
                    }
                }


                if ( opscUniquifierToKsTbls.isEmpty() ) {
                    System.out.println("ERROR: Failed to get backup SSTable file list from " +
                        DseOpscS3RestoreUtils.OPSC_BKUP_METADATA_FILE + " file!");
                    return;
                }


                // Second, check SSTables records matching the backup time, keyspace, and table

                String sstablePrefixString =
                    CONFIGPROP.get(DseOpscS3RestoreUtils.CFG_KEY_OPSC_S3_BUCKET_NAME) + "/" +
                        DseOpscS3RestoreUtils.OPSC_OBJKEY_BASESTR + "/" +
                        host_id + "/" +
                        DseOpscS3RestoreUtils.OPSC_OBJKEY_SSTABLES_MARKER_STR;

                for ( String sstableObjName : opscUniquifierToKsTbls.keySet() )  {

                    String opscObjName = sstablePrefixString + "/" + sstableObjName;

                    if (opscUniquifierToKsTbls.containsKey(sstableObjName)) {
                        String[] ksTblUniquifer = opscUniquifierToKsTbls.get(sstableObjName).split(":");
                        String ks = ksTblUniquifer[0];
                        String tbl = ksTblUniquifer[1];

                        boolean filterKsTbl = keyspaceName.equalsIgnoreCase(ks);
                        if ((tableName != null) && !tableName.isEmpty()) {
                            filterKsTbl = filterKsTbl && tableName.equalsIgnoreCase(tbl);
                        }

                        if (filterKsTbl) {
                            numSstableBkupItems++;

                            long opscObjSize = -1;
                            if (fileSizeChk) {
                                opscObjSize = getS3FileSize(s3Client, bktName, opscObjName);
                            }

                            System.out.println("  - " + opscObjName +
                                ( !fileSizeChk ? "" : (" (size = " + opscObjSize + " bytes)") ) +
                                " [keyspace: " + ks + "; table: " + tbl + "]");
                        }
                    }
                }

                if (numSstableBkupItems == 0) {
                    System.out.println("  - Found no matching backup records for the specified conditions!.");
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
            "List OpsCenter backup items (all | DC:\"<dc_name>\" | me[:\"<host_id_string>\"]).");
        Option downloadOption = new Option(
            DseOpscS3RestoreUtils.CMD_OPTION_DOWNLOAD_SHORT,
            DseOpscS3RestoreUtils.CMD_OPTION_DOWNLOAD_LONG,
            true,
            "Download OpsCenter bakcup items to local directory (only applies to \"list me\" case");
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
        Option opscBkupTimeOption = new Option(
            DseOpscS3RestoreUtils.CMD_OPTION_BACKUPTIME_SHORT,
            DseOpscS3RestoreUtils.CMD_OPTION_BACKUPTIME_LONG,
            true,
            "OpsCetner backup datetime");
        Option clsTargetDirOption = new Option(
            DseOpscS3RestoreUtils.CMD_OPTION_CLSDOWNDIR_SHORT,
            DseOpscS3RestoreUtils.CMD_OPTION_CLSDOWNDIR_LONG,
            true,
            "Clear existing download directory content");
        Option noDirStructOption = new Option(
            DseOpscS3RestoreUtils.CMD_OPTION_NODIR_SHORT,
            DseOpscS3RestoreUtils.CMD_OPTION_NODIR_LONG,
            true,
            "Don't maintain keyspace/table backup data directory structure");
        Option userOption = new Option(
            DseOpscS3RestoreUtils.CMD_OPTION_USER_SHORT,
            DseOpscS3RestoreUtils.CMD_OPTION_USER_LONG,
            true,
            "Cassandra user name");
        Option passwdOption = new Option(
            DseOpscS3RestoreUtils.CMD_OPTION_PWD_SHORT,
            DseOpscS3RestoreUtils.CMD_OPTION_PWD_LONG,
            true,
            "Cassandra user password");
        Option debugOption = new Option(
            DseOpscS3RestoreUtils.CMD_OPTION_DEBUG_SHORT,
            DseOpscS3RestoreUtils.CMD_OPTION_DEBUG_LONG,
            false,
            "Debug output");

        options.addOption(helpOption);
        options.addOption(listOption);
        options.addOption(fileOption);
        options.addOption(downloadOption);
        options.addOption(keyspaceOption);
        options.addOption(tableOption);
        options.addOption(opscBkupTimeOption);
        options.addOption(clsTargetDirOption);
        options.addOption(noDirStructOption);
        options.addOption(userOption);
        options.addOption(passwdOption);
        options.addOption(debugOption);
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
            usageAndExit(10);
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
            usageAndExit(20);
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
            usageAndExit(30);
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
                usageAndExit(40);
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
                usageAndExit(50);
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
            usageAndExit(60);
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
            usageAndExit(70);
        }

        // "-t" option (Table name) is optional. If not specified, all Tables of the specified keyspaces will be processed.
        String tableName = cmd.getOptionValue(DseOpscS3RestoreUtils.CMD_OPTION_TABLE_SHORT);

        // "-obt" option is a must
        // OpsCenter Backup Date Time String (Can get  from OpsCenter Backup Service Window)
        String obtOptOptValue = cmd.getOptionValue(DseOpscS3RestoreUtils.CMD_OPTION_BACKUPTIME_SHORT);

        if ( (obtOptOptValue == null) || (obtOptOptValue.isEmpty()) ) {
            System.out.println("\nERROR: Please specify proper OpsCenter backup time string (M/d/yyyy h:mm a) as the \"-" +
                DseOpscS3RestoreUtils.CMD_OPTION_BACKUPTIME_SHORT + "\" option value.");
            usageAndExit(80);
        }

        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("M/d/yyyy h:mm a");
        ZonedDateTime opscBackupTime_gmt = null;
        try {
            LocalDateTime ldt = LocalDateTime.parse(obtOptOptValue, formatter);

            ZoneId gmtZoneId = ZoneId.of("UTC");
            opscBackupTime_gmt = ldt.atZone(gmtZoneId);
        }
        catch (DateTimeParseException dte) {
            dte.printStackTrace();

            System.out.println("\nERROR: Please specify correct time string format (M/d/yyyy h:mm a) for \"-" +
                DseOpscS3RestoreUtils.CMD_OPTION_BACKUPTIME_SHORT + "\" option.");
            usageAndExit(90);
        }

        // "-cls" option is optional
        boolean clearTargetDownDir = false;
        String clsOptOptValue = cmd.getOptionValue(DseOpscS3RestoreUtils.CMD_OPTION_CLSDOWNDIR_SHORT);

        if ( (clsOptOptValue != null) && (!clsOptOptValue.isEmpty()) ) {
            try {
                clearTargetDownDir = Boolean.parseBoolean(clsOptOptValue);
            }
            catch (NumberFormatException nfe) {
            }
        }

        // "-nds" option is optional.
        // ONLY works when "-t"/"--table" option is provided;
        //    Otherwise, target directory structure is automatically maintained.
        boolean noTargetDirStruct = false;
        String ndsOptOptValue = cmd.getOptionValue(DseOpscS3RestoreUtils.CMD_OPTION_NODIR_SHORT);

        if ( (ndsOptOptValue != null) && (!ndsOptOptValue.isEmpty()) ) {
            try {
                noTargetDirStruct = Boolean.parseBoolean(ndsOptOptValue);

                if  ( (tableName == null) || tableName.isEmpty() ) {
                    noTargetDirStruct = false;
                }
            }
            catch (NumberFormatException nfe) {
            }
        }

        // "-u" and "-p" option is optional.
        String userName = cmd.getOptionValue(DseOpscS3RestoreUtils.CMD_OPTION_USER_SHORT);
        String passWord = cmd.getOptionValue(DseOpscS3RestoreUtils.CMD_OPTION_PWD_SHORT);


        /**
         *  Parsing commandline parameters (ends)  <----
         */



        /**
         * Load configuration files
         */

        CONFIGPROP = DseOpscS3RestoreUtils.LoadConfigFile(cfgFilePath);
        if (CONFIGPROP == null) {
            usageAndExit(100);
        }

        // Check whether "use_ssl" config file parameter is true (default false).
        // - If so, java system properties "-Djavax.net.ssl.trustStore" and "-Djavax.net.ssl.trustStorePassword" must be set.
        boolean useSsl = false;
        String useSslStr = CONFIGPROP.getProperty(DseOpscS3RestoreUtils.CFG_KEY_USE_SSL);
        if ( (useSslStr != null) && !(useSslStr.isEmpty()) ) {
            useSsl = Boolean.parseBoolean(useSslStr);
        }

        if (useSsl) {
            String trustStoreProp = System.getProperty(DseOpscS3RestoreUtils.JAVA_SSL_TRUSTSTORE_PROP);
            String trustStorePassProp = System.getProperty(DseOpscS3RestoreUtils.JAVA_SSL_TRUSTSTORE_PASS_PROP);

            if ( (trustStoreProp == null) || trustStoreProp.isEmpty() ||
                (trustStorePassProp == null) || trustStorePassProp.isEmpty()) {
                System.out.format("\nERROR: Must provide SSL system properties (-D%s and -D%s) when " +
                        "configuration file parameter \"%s\" is set true!\n",
                    DseOpscS3RestoreUtils.JAVA_SSL_TRUSTSTORE_PROP,
                    DseOpscS3RestoreUtils.JAVA_SSL_TRUSTSTORE_PASS_PROP,
                    DseOpscS3RestoreUtils.CFG_KEY_USE_SSL);
                usageAndExit(104);
            }
        }

        // Check whether "user_auth" config file parameter is true (default false).
        // - If so, command line parameter "-u (--user)" and "-p (--password)" must be set.
        boolean userAuth = false;
        String userAuthStr = CONFIGPROP.getProperty(DseOpscS3RestoreUtils.CFG_KEY_USE_SSL);
        if ( (userAuthStr != null) && !(userAuthStr.isEmpty()) ) {
            userAuth = Boolean.parseBoolean(useSslStr);
        }

        if (userAuth) {
            if ( (userName == null) || userName.isEmpty() ||
                (passWord == null) || passWord.isEmpty() ) {
                System.out.println("\nERROR: Must provide user name and password when configuration file parameter \"" +
                    DseOpscS3RestoreUtils.CFG_KEY_USER_AUTH + "\" is set true!");
                usageAndExit(105);
            }
        }

        // Check whether "file_size_mon" config file parameter is true (default false).
        boolean fileSizeChk = false;
        String fileSizeMonStr = CONFIGPROP.getProperty(DseOpscS3RestoreUtils.CFG_KEY_FILE_SIZE_CHK);
        if ( (fileSizeMonStr != null) && !(fileSizeMonStr.isEmpty()) ) {
            fileSizeChk = Boolean.parseBoolean(fileSizeMonStr);
        }

        /**
         * If the specified download home directory already exists,
         *      check if it is reachable and write-able; otherwise, error-out.
         *
         * If the specified download home directory doesn't exist, do nothing here
         */
        Path localDownloadHomePath = Paths.get(CONFIGPROP.getProperty(DseOpscS3RestoreUtils.CFG_KEY_LOCAL_DOWNLOAD_HOME));
        if ( Files.exists(localDownloadHomePath) &&
            ( !Files.isDirectory(localDownloadHomePath)  ||
                !( localDownloadHomePath.toFile().canRead() &&
                    localDownloadHomePath.toFile().canWrite() &&
                    localDownloadHomePath.toFile().canExecute()
                )
            )
        )
        {
            System.out.println("\nERROR: [Config File] Specified local download directory is not correct (doesn't exist, non-directory, or no Read/Write privilege)!");
            usageAndExit(120);
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
            .withClientConfiguration(new ClientConfiguration().withProtocol(Protocol.HTTP))
            .withCredentials(new AWSStaticCredentialsProvider(credentials))
            .withRegion(CONFIGPROP.getProperty(DseOpscS3RestoreUtils.CFG_KEY_OPSC_S3_AWS_REGION))
            .build();


        /**
         * Check if S3 bucket is reachable! Otherwise, list files under it.
         */
        Path s3BucketName = Paths.get(CONFIGPROP.getProperty(DseOpscS3RestoreUtils.CFG_KEY_OPSC_S3_BUCKET_NAME));
        /**
         * Check access privilege of S3 bucket (needs to be able to read!)

         if ( Files.notExists(nfsBackupHomePath) ||
         !Files.isDirectory(nfsBackupHomePath)  ||
         !(nfsBackupHomePath.toFile().canRead() && nfsBackupHomePath.toFile().canExecute()) ) {
         System.out.println("\nERROR: [Config File] Specified S3 OpsCenter backup directory is not correct (doesn't exist, non-directory, or no READ privilege)!");
         usageAndExit(110);
         }
         */


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


        // dseCluster.connect();    /* NO NEED for acutal connection */
        Metadata dseClusterMetadata = null;

        // Do NOT check cluster metadata for "-l me:<dse_host_id>" option
        boolean checkDseMetadata = listCluster || listDC || (listMe && ((myHostID == null) || myHostID.isEmpty()) );
        if (checkDseMetadata) {

            try {
                DseCluster.Builder clusterBuilder = DseCluster.builder()
                    .addContactPoint(CONFIGPROP.getProperty(DseOpscS3RestoreUtils.CFG_KEY_CONTACT_POINT))
                    .withQueryOptions(queryOptions);


                if (useSsl) {
                    clusterBuilder.withSSL();
                }

                if (userAuth) {
                    AuthProvider authProvider = new PlainTextAuthProvider(userName, passWord);
                    clusterBuilder.withAuthProvider(authProvider);
                }

                DseCluster dseCluster = clusterBuilder.build();
                dseClusterMetadata = dseCluster.getMetadata();
            }
            catch (NoHostAvailableException nhae) {
                System.out.println("\nERROR: Failed to check DSE cluster metadata. " +
                    "Please check DSE cluster status and/or connection requirements (e.g. SSL/TLS, username/password)!");
                usageAndExit(120);
            }
            catch (Exception e) {
                System.out.println("\nERROR: Unknown error when checking DSE cluster metadata!");
                e.printStackTrace();
                usageAndExit(125);
            }
        }


        // List Opsc S3 backup items for all Dse Cluster hosts
        if ( listCluster ) {
            listS3ObjtForCluster(
                dseClusterMetadata,
                fileSizeChk,
                s3Client,
                keyspaceName,
                tableName,
                opscBackupTime_gmt);
        }
        // List OpsCenter backup SSTables for all hosts in a specified DC of the Dse cluster
        else if ( listDC ) {
            listS3ObjForDC(
                dseClusterMetadata,
                fileSizeChk,
                s3Client,
                dcNameToList,
                keyspaceName,
                tableName,
                opscBackupTime_gmt);
        }
        // List (and download) Opsc S3 backup items for myself (the host that runs this program)
        else if ( listMe ) {
            listDownloadS3ObjForMe(
                dseClusterMetadata,
                fileSizeChk,
                s3Client,
                downloadS3Obj,
                downloadS3ObjThreadNum,
                myHostID,
                keyspaceName,
                tableName,
                opscBackupTime_gmt,
                clearTargetDownDir,
                noTargetDirStruct );
        }

        if (s3Client != null) {
            s3Client.shutdown();
        }

        System.exit(0);
    }
}