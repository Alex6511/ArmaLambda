import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.event.S3EventNotification;
import com.amazonaws.services.s3.model.*;
import org.apache.commons.io.FilenameUtils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;


public class Main implements
        RequestHandler<S3Event, String> {

    public static void main(String[] args) {
        // write your code here
    }

    public String handleRequest(S3Event s3Event, Context context) {
        byte[] buffer = new byte[1024];
        try {
            for (S3EventNotification.S3EventNotificationRecord record : s3Event.getRecords()) {
                String srcBucket = record.getS3().getBucket().getName();
                String dstBucket = "stage1.alex6511.com";

                // Object key may have spaces or unicode non-ASCII characters.
                String srcKey = record.getS3().getObject().getKey()
                        .replace('+', ' ');
                srcKey = URLDecoder.decode(srcKey, "UTF-8");

                // Detect file type
                Matcher matcher = Pattern.compile(".*\\.([^\\.]*)").matcher(srcKey);
                if (!matcher.matches()) {
                    System.out.println("Unable to detect file type for key " + srcKey);
                    return "";
                }
                String extension = matcher.group(1).toLowerCase();
                if (!"zip".equals(extension)) {
                    System.out.println("Skipping non-zip file " + srcKey + " with extension " + extension);
                    return "";
                }
                System.out.println("Extracting zip file " + srcBucket + "/" + srcKey);

                // Download the zip from S3 into a stream
                AmazonS3 s3Client = new AmazonS3Client();
                S3Object s3Object = s3Client.getObject(new GetObjectRequest(srcBucket, srcKey));
                ZipInputStream zis = new ZipInputStream(s3Object.getObjectContent());
                ZipEntry entry = zis.getNextEntry();

                ArrayList<String> files = new ArrayList<String>();

                while (entry != null) {
                    String fileName = entry.getName();
                    files.add(fileName);
                    String mimeType = FileMimeType.fromExtension(FilenameUtils.getExtension(fileName)).mimeType();
                    System.out.println("Extracting " + fileName + ", compressed: " + entry.getCompressedSize() + " bytes, extracted: " + entry.getSize() + " bytes, mimetype: " + mimeType);
                    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
                    int len;
                    while ((len = zis.read(buffer)) > 0) {
                        outputStream.write(buffer, 0, len);
                    }
                    InputStream is = new ByteArrayInputStream(outputStream.toByteArray());
                    ObjectMetadata meta = new ObjectMetadata();
                    meta.setContentLength(outputStream.size());
                    meta.setContentType(mimeType);
//                    s3Client.putObject(dstBucket, FilenameUtils.getFullPath(srcKey) + fileName, is, meta);
                    is.close();
                    outputStream.close();
                    entry = zis.getNextEntry();
                }
                ArrayList<String> requiredFiles = new ArrayList<String>();
                requiredFiles.add("cfg/ACE_Settings.hpp");
                requiredFiles.add("cfg/CfgEventHandlers.hpp");
                requiredFiles.add("cfg/Params.hpp");
                requiredFiles.add("init.sqf");
                requiredFiles.add("mission.sqm"); // Because I don't want people uploading their trash
                requiredFiles.add("CfgLoadouts.hpp");
                requiredFiles.add("description.ext");
                for (String file : files) {
                    System.out.println(file);
                }
                if (!files.contains("cleanup.bat") && files.containsAll(requiredFiles)) {
                    CopyObjectRequest copyObjRequest = new CopyObjectRequest(
                            srcBucket, srcKey, dstBucket, srcKey);
                    s3Client.copyObject(copyObjRequest);
                    System.out.println("Uploaded Mission to Success Bucket");
                } else {
                    CopyObjectRequest copyObjRequest = new CopyObjectRequest(
                            srcBucket, srcKey, "error.alex6511.com", srcKey);
                    s3Client.copyObject(copyObjRequest);
                    System.out.println("Mission Failed Autotest");
                }
                zis.closeEntry();
                zis.close();

                //delete zip file when done
                System.out.println("Deleting zip file " + srcBucket + "/" + srcKey + "...");
                s3Client.deleteObject(new DeleteObjectRequest(srcBucket, srcKey));
                System.out.println("Done deleting");
            }
            return ":ok_hand:";
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
