import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class Sampling {

    public static AmazonS3 s3client;

    public static void main(String[] args) {
        String accessKey = System.getenv("access_key");
        String secretKey = System.getenv("secret_key");
        String bucketName = System.getenv("bucket");
        String pathWithinBucket = System.getenv("key");

        // AWS setting up credentials for API calls */
        AWSCredentials credentials = new BasicAWSCredentials(accessKey, secretKey);
        AWSStaticCredentialsProvider statics = new AWSStaticCredentialsProvider(credentials);

        s3client = AmazonS3ClientBuilder.standard()
                .withCredentials(statics)
                .withRegion(Regions.EU_CENTRAL_1)
                .build();

        // Initializing Spark Session
        SparkSession spark = SparkSession.builder()
                .config("fs.s3a.endpoint", "s3.eu-central-1.amazonaws.com")
                .config("fs.s3a.access.key", System.getenv("access_key"))
                .config("fs.s3a.secret.key", System.getenv("secret_key"))
                .appName("test").master("local[*]").getOrCreate();

        // Listing all objects within a specific S3 path
        ObjectListing objList = s3client.listObjects(bucketName, pathWithinBucket);

        List<S3ObjectSummary> summaries = objList.getObjectSummaries();
        while (objList.isTruncated()) {
            objList = s3client.listNextBatchOfObjects(objList);
            summaries.addAll(objList.getObjectSummaries());
        }

        // Removing _SUCCESS from the partitions list
        List<S3ObjectSummary> successData = summaries
                .stream().filter(e -> e.getKey().contains("_SUCCESS")).collect(Collectors.toList());
        summaries.removeAll(successData);

        // Total number of partitions without success
        int numPartitions = summaries.size();

        // A 'mapped' value for numPartitionsToCalculateAvg depending on numPartitions value
        double numPartitionsToCalculateAvg = numPartitions <= 100 ?
                Math.ceil(Math.log(numPartitions + 1) / Math.log(3))
                :
                Math.ceil(3 + (((numPartitions / 100.0) * 2) - 1));

        // We generate `numberPartitionsToCalculateAvg` random integers
        List<Integer> randomInts = new ArrayList<>();
        while (randomInts.size() < numPartitionsToCalculateAvg) {
            int dub = (int) Math.floor(Math.random() * numPartitions);

            if (!randomInts.contains(dub)) randomInts.add(dub);
        }

        // We find the average of all these random partitions
        Optional<Long> optAvgRandom = randomInts
                .stream()
                .map(part -> {
                    String partBucket = summaries.get(part).getBucketName();
                    String partKey = summaries.get(part).getKey();

                    return spark.read().parquet(String.format("s3a://%s/%s", partBucket, partKey));
                })
                .map(Dataset::count)
                .reduce(Long::sum);

        // If there is a number, find the average, otherwise set it to -1
        double avgRandom = optAvgRandom.map(aLong -> aLong / (double) randomInts.size()).orElse(-1.0);

        // Now, we have an assumed count which is not exact but approximate to the whole dataset count
        double assumedCount = avgRandom * numPartitions;

        // We calculate a fraction
        double fractionToUse = findFraction(0.00001, assumedCount);

        // Read the path with the calculated fraction
        String target = String.format("s3a://%s/%s", bucketName, pathWithinBucket);
        spark.read().parquet(target).sample(fractionToUse).show();
    }

    public static double findFraction(double fraction, double totalCount) {
        // Round the number
        long rounded = Math.round(fraction * totalCount);

        // Get the length of the rounded number
        long length = String.valueOf(rounded).length();

        // If number has three digits, return the fraction, otherwise keep going deeper
        if (length >= 3) return fraction;

        return findFraction(fraction * 10, totalCount);
    }
}