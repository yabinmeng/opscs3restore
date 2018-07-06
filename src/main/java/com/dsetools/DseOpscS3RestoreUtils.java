package com.dsetools;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class DseOpscS3RestoreUtils {

    // Key string in Yaml config file
    static String CFG_KEY_CONTACT_POINT = "dse_contact_point";
    static String CFG_KEY_LOCAL_DOWNLOAD_HOME = "local_download_home";
    static String CFG_KEY_OPSC_S3_AWS_REGION = "opsc_s3_aws_region";
    static String CFG_KEY_OPSC_S3_BUCKET_NAME = "opsc_s3_bucket_name";

    static String OPSC_S3_OBJKEY_BASESTR = "snapshots";

    static String CASSANDRA_SSTABLE_FILE_CODE = "mc";


    static Properties LoadConfigFile(String configFilePath) {

        Properties configProps = new Properties();

        InputStream inputStream = null;

        try {
            inputStream = new FileInputStream(configFilePath);
            configProps.load(inputStream);
        }
        catch (IOException ioe) {
            System.err.format("ERROR: failed to read/process configuration file (%s)\n.", configFilePath);
            ioe.printStackTrace();
        }

        return configProps;
    }
}