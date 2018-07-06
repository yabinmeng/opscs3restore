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
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.QueryOptions;
import com.datastax.driver.dse.DseCluster;
import org.apache.commons.cli.*;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.*;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
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

    private DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    S3ObjDownloadRunnable( int tID,
                                   TransferManager transferManager,
                                   String s3bkt_name,
                                   String download_dir,
                                   String[] object_names,
                                   long[] object_sizes ) {
        assert (tID > 0);
        assert (transferManager != null);

        this.threadID = tID;
        this.s3TransferManager = transferManager;
        this.s3BuketName = s3bkt_name;
        this.downloadHomeDir = download_dir;
        this.s3ObjNames = object_names;
        this.s3ObjSizes = object_sizes;

        System.out.format(" Creating thread with ID %d.\n", threadID);
    }

    @Override
    public void run() {

        LocalDateTime startTime = LocalDateTime.now();

        System.out.println(" ... Starting thread " + threadID + " at: " + startTime.format(formatter));

        int downloadedS3ObjNum = 0;
        int failedS3ObjNum = 0;

        for ( int i = 0; i < s3ObjNames.length; i++ ) {
            try {
                File localFile = new File(downloadHomeDir + "/" + s3ObjNames[i]);
                GetObjectRequest getObjectRequest = new GetObjectRequest(s3BuketName, s3ObjNames[i]);
                s3TransferManager.download(getObjectRequest, localFile);
                downloadedS3ObjNum++;
            }
            catch ( Exception ex ) {
                failedS3ObjNum++;
            }

            System.out.format("     [Thread %d] download of \"%s\" complete\n", threadID, s3ObjNames[i]);
        }

        LocalDateTime endTime = LocalDateTime.now();

        Duration duration = Duration.between(startTime, endTime);

        System.out.format(" ... Existing Thread %d at %s (duration: %d seconds): %d of %d s3 objects downloaded, %d failed.\n",
            threadID,
            endTime.format(formatter),
            duration.getSeconds(),
            downloadedS3ObjNum,
            s3ObjNames.length,
            failedS3ObjNum);
    }
}


public class DseOpscS3Restore {

    private static Properties CONFIGPROP = null;
    private static int  THREAD_POOL_SIZE = 10;

    /**
     * List (and download) Opsc S3 backup objects for a specified host
     *
     * @param dseClusterMetadata
     * @param s3Client
     * @param hostId
     * @param download
     */
    static void listDownloadS3ObjForHost(Metadata dseClusterMetadata,
                                         AmazonS3 s3Client,
                                         String hostId,
                                         boolean download,
                                         int threadNum) {
        assert (hostId != null);

        System.out.format("List" +
            (download ? " and download" : "") +
            " OpsCenter S3 backup items for specified host (%s) ...\n", hostId);

        String downloadHomeDir = CONFIGPROP.getProperty(DseOpscS3RestoreUtils.CFG_KEY_LOCAL_DOWNLOAD_HOME);

        if (download) {
            assert (threadNum > 0);

            // Creating local home directory to hold S3 download files
            // Clean up contents in the home directory, if any!
            try {
                File file = new File(downloadHomeDir);

                FileUtils.forceMkdir(file);
                FileUtils.cleanDirectory(file);
            }
            catch (IOException ioe) {
                System.out.println("ERROR: failed to create download home directory for S3 objects!");
                System.exit(-10);
            }
        }

        TransferManager transferManager = TransferManagerBuilder.standard().withS3Client(s3Client).build();
        String bktName = CONFIGPROP.getProperty(DseOpscS3RestoreUtils.CFG_KEY_OPSC_S3_BUCKET_NAME);


        // Download OpsCenter S3 object items
        ObjectListing objectListing = s3Client.listObjects(
            new ListObjectsRequest()
                .withBucketName(bktName)
                .withPrefix(DseOpscS3RestoreUtils.OPSC_S3_OBJKEY_BASESTR + "/" + hostId + "/opscenter"));

        List<S3ObjectSummary> s3ObjectSummaries = objectListing.getObjectSummaries();
        int numOpscBkupItems = s3ObjectSummaries.size();

        System.out.println("\nTotoal OpsCenter S3 object number: " + numOpscBkupItems);

        for (S3ObjectSummary objectSummary : s3ObjectSummaries) {
            System.out.println(" - " + objectSummary.getKey() + " (size = " + objectSummary.getSize() + " bytes)");

            if ( download ) {
                try {
                    String objKeyName = objectSummary.getKey();
                    File localFile = new File(downloadHomeDir + "/" + objKeyName);
                    GetObjectRequest getObjectRequest = new GetObjectRequest(bktName, objKeyName);
                    transferManager.download(getObjectRequest, localFile);

                } catch (AmazonServiceException e) {
                    // The call was transmitted successfully, but Amazon S3 couldn't process
                    // it, so it returned an error response.
                    e.printStackTrace();
                } catch (SdkClientException e) {
                    // Amazon S3 couldn't be contacted for a response, or the client
                    // couldn't parse the response from Amazon S3.
                    e.printStackTrace();
                }

                System.out.println("   ... Download complete");
            }
        }

        // Download OpsCenter S3 object items
        objectListing = s3Client.listObjects(
            new ListObjectsRequest()
                .withBucketName(bktName)
                .withPrefix(DseOpscS3RestoreUtils.OPSC_S3_OBJKEY_BASESTR + "/" + hostId + "/sstable"));

        s3ObjectSummaries = objectListing.getObjectSummaries();
        int numSstableBkupItems = s3ObjectSummaries.size();

        System.out.println("\nTotoal SSTable S3 object number: " + numSstableBkupItems);

        /**
         *  Start multiple threads to process data ingestion concurrently
         */
        ExecutorService executor = Executors.newFixedThreadPool(THREAD_POOL_SIZE);

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

        int i = 0;
        int threadId = 0;

        for (S3ObjectSummary objectSummary : s3ObjectSummaries) {
            System.out.println(" - " + objectSummary.getKey() + " (size = " + objectSummary.getSize() + " bytes)");

            if ( download ) {
                try {
                    s3SstableObjKeyNames[i % SSTABLE_SET_FILENUM] = objectSummary.getKey();
                    s3SstableObjKeySizes[i % SSTABLE_SET_FILENUM] = objectSummary.getSize();

                    if ( (i+1) % SSTABLE_SET_FILENUM == 0 ) {
                        threadId++;

                        Runnable worker = new S3ObjDownloadRunnable(
                            threadId,
                            transferManager,
                            bktName,
                            downloadHomeDir,
                            s3SstableObjKeyNames,
                            s3SstableObjKeySizes );

                        executor.execute(worker);
                    }

                    i++;

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
        }

        executor.shutdown();

        while (!executor.isTerminated()) {
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
     */
    static void listDownloadS3ObjForMe(Metadata dseClusterMetadata,
                                       AmazonS3 s3Client,
                                       boolean download,
                                       int threadNum) {
        String myHostId = "";

        String localhostIp = getLocalIP("eth0");

        if (localhostIp == null) {
            System.out.println("\nERROR: failed to get local host IP address!");
            return;
        }

        System.out.println("locahost: " + localhostIp);

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

        if ( myHostId != null && !myHostId.isEmpty() ) {
            listDownloadS3ObjForHost(dseClusterMetadata, s3Client, myHostId, download, threadNum);
        }
    }

    /**
     * List Opsc S3 backup objects for all DSE cluster hosts
     *
     * @param dseClusterMetadata
     * @param s3Client
     */
    static void listS3ObjtForCluster(Metadata dseClusterMetadata, AmazonS3 s3Client) {

        System.out.format("List OpsCenter S3 backup items for DSE cluster (%s) ...\n", dseClusterMetadata.getClusterName());

        listS3ObjForDC(dseClusterMetadata, s3Client, "");
    }

    /**
     * List Opsc S3 backup objects for all hosts in a specified DC
     *
     * @param dseClusterMetadata
     * @param s3Client
     * @param dcName
     */
    static void listS3ObjForDC(Metadata dseClusterMetadata, AmazonS3 s3Client, String dcName) {
        assert (CONFIGPROP != null);

        Set<Host> hosts = dseClusterMetadata.getAllHosts();

        boolean dcOnly = ( (dcName != null) && !dcName.isEmpty() );
        if ( dcOnly ) {
            System.out.format("List OpsCenter S3 backup items for specified DC (%s) of DSE cluster (%s) ...\n",
                dcName,
                dseClusterMetadata.getClusterName());
        }

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
                ObjectListing objectListing = s3Client.listObjects(
                    new ListObjectsRequest()
                        .withBucketName(bktName)
                        .withPrefix(DseOpscS3RestoreUtils.OPSC_S3_OBJKEY_BASESTR + "/" + host_id));

                for (S3ObjectSummary objectSummary : objectListing.getObjectSummaries()) {
                    System.out.println("  - " + objectSummary.getKey() + " (size = " + objectSummary.getSize() + " bytes)");
                }

                System.out.println("\n");
            }
        }
    }

    static Options options = new Options();

    static {
        Option helpOption = new Option("h", "help",
            false,
            "Displays this help message.");
        Option fileOption = new Option("f", "config",
            true,
            "Configure file path");
        Option listOption = new Option("l", "list",
            true,
            "List OpsCenter S3 backup items (all| DC:\"<dc_name>\" | me).");
        Option downloadOption = new Option("d", "download",
            true,
            "Download OpsCenter S3 bakcup items to local directory (only applies to \"list me\" case");
        Option configFileOption = new Option("f", "config",
            true,
            "Configuration properties file path");

        options.addOption(helpOption);
        options.addOption(listOption);
        options.addOption(fileOption);
        options.addOption(downloadOption);
        options.addOption(configFileOption);
    }

    /**
     * Print out usage info. and exit
     */
    static void usageAndExit(int errorCode) {

        PrintWriter errWriter = new PrintWriter(System.out, true);

        try {
            HelpFormatter formatter = new HelpFormatter();

            formatter.printHelp(errWriter, 120, "DseOpscS3Restore",
                String.format("%DseOpscS3Restore Options:"),
                options, 2, 1, "", true);

            System.out.println();
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
        if ( cmd.hasOption("h") ) {
            usageAndExit();
        }

        if ( !( cmd.hasOption("l") && cmd.hasOption("f") ) ) {
            System.err.println("\nERROR: \"-f (--config)\" and \"-l (--list)\" options are mandatory!");
            usageAndExit(-20);
        }

        String cfgFilePath = "";

        if ( cmd.hasOption("f") ) {
            cfgFilePath = cmd.getOptionValue("f");

            if ( cfgFilePath.isEmpty() ) {
                System.err.println("\nERROR: Please specify a valid configuration file path for \"f (config)\" option!");
                usageAndExit(-30);
            }
        }

        boolean listCluster = false;
        boolean listDC = false;
        boolean listMe = false;

        String dcNameToList = "";

        if ( cmd.hasOption("l") ) {
            String lOptVal = cmd.getOptionValue("l");

            if ( lOptVal.equalsIgnoreCase("all") ) {
                listCluster = true;
            }
            else if ( lOptVal.startsWith("DC") ) {
                listDC = true;
                dcNameToList = lOptVal.split(":")[1];
            }
            else if ( lOptVal.equalsIgnoreCase("me") ) {
                listMe = true;
            }
        }

        boolean downloadS3Obj = false;
        int downloadS3ObjThreadNum = THREAD_POOL_SIZE;

        if ( cmd.hasOption("d") ) {
            downloadS3Obj = true;

            String dOptVal = cmd.getOptionValue("d");

            try {
                downloadS3ObjThreadNum = Integer.parseInt(dOptVal);
            }
            catch (NumberFormatException nfe) {
                System.out.println("WARN: Incorrect \"-d\" option value - must be a positive integer! Using default value (" +
                    THREAD_POOL_SIZE + ")");
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
            System.exit(-40);
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
            listS3ObjtForCluster(dseClusterMetadata, s3Client);
        }
        // List Opsc S3 backup items for all hosts in a specified DC of the Dse cluster
        else if ( listDC ) {
            listS3ObjForDC(dseClusterMetadata, s3Client, dcNameToList);
        }
        // List (and download) Opsc S3 backup items for myself (the host that runs this program)
        else if ( listMe ) {
            listDownloadS3ObjForMe(dseClusterMetadata, s3Client, downloadS3Obj, downloadS3ObjThreadNum);
        }

        System.exit(0);
    }
}