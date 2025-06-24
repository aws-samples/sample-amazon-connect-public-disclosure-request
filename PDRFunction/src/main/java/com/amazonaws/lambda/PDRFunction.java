package com.amazonaws.lambda;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.services.bedrockruntime.BedrockRuntimeClient;
import software.amazon.awssdk.services.bedrockruntime.model.*;
import software.amazon.awssdk.services.connect.ConnectClient;
import software.amazon.awssdk.services.connect.model.*;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.presigner.S3Presigner;
import software.amazon.awssdk.services.s3.presigner.model.GetObjectPresignRequest;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringWriter;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

public class PDRFunction implements RequestHandler<Object, String> {
    public S3Presigner s3PreSigner;
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private LambdaLogger logger;
    private static String AWS_Account_Id = "";

    // Environment Variables
    // PDR DestinationBucket
    private final String destinationBucket = System.getenv()
                                                   .getOrDefault("DestinationBucket", "");
    // Amazon Connect InstanceId
    private final String instanceId = System.getenv()
                                            .getOrDefault("Instance_Id", "");
    // S3 PreSigned URL Expiration in seconds
    private final int presignedUrlExpiration = Integer.parseInt(System.getenv()
                                                                      .getOrDefault("PresignedUrlExpiration", "86400"));
    // Bedrock Model ID (default to Claude 3 Sonnet)
    private final String bedrockModelId = System.getenv()
                                                .getOrDefault("BedrockModelId", "anthropic.claude-3-5-sonnet-20241022-v2:0");

    // Bedrock Socket Timeout in Secs
    private final String bedrockTimeoutInSecs = System.getenv()
                                                      .getOrDefault("BedrockTimeoutInSecs", "600");

    // AWS Services
    private final ConnectClient connectClient = ConnectClient.builder()
                                                             .build();
    private final S3Client s3Client = S3Client.builder()
                                              .build();
    private final BedrockRuntimeClient bedrockClient = BedrockRuntimeClient.builder()
                                                                           .httpClientBuilder(ApacheHttpClient.builder()
                                                                                                              .connectionTimeout(Duration.ofSeconds(30))
                                                                                                              .socketTimeout(Duration.ofSeconds(Long.parseLong(bedrockTimeoutInSecs))))
                                                                           .build();

    /**
     * Main entry point for the Lambda function
     * Processes events from S3 or EventBridge and handles public disclosure requests
     *
     * @param event   The Lambda event object
     * @param context The Lambda context
     * @return A response string indicating success or failure
     */
    @Override
    public String handleRequest(Object event, Context context) {
        // Initialize logger first before any logging calls
        logger = context.getLogger();
        s3PreSigner = S3Presigner.create();
        AWS_Account_Id = getAccountId(context);

        logln("Event received");

        String response = "200 OK";
        JsonNode eventJSONMap;
        String bucket = "", key = "";

        // Get the S3 bucket and key from the event
        try {
            String eventString = objectMapper.writeValueAsString(event);
            eventJSONMap = objectMapper.readTree(eventString);
            // If the Event is from S3, else its assumed it's from EventBridge
            if (eventJSONMap.has("Records")) {
                bucket = eventJSONMap.path("Records")
                                     .get(0)
                                     .path("s3")
                                     .path("bucket")
                                     .path("name")
                                     .asText();
                key = eventJSONMap.path("Records")
                                  .get(0)
                                  .path("s3")
                                  .path("object")
                                  .path("key")
                                  .asText();
            }
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }

        // Process both Queue and PhoneNumber Quick Connect Type, using the CSV file from Amazon S3
        if (!bucket.isEmpty() && !key.isEmpty()) {
            try {
                // Read CSV file from S3
                List<PDRCSVRecord> records = readCsvFromS3(bucket, key);

                List<Map<String, String>> contactIdPrefixList = getContactIdDetails(records, instanceId);

                List<PDRCSVRecord> pdrcsvRecords = processContactFiles(contactIdPrefixList);

                // Close S3 PreSigner
                s3PreSigner.close();

                String csvContent = generateCSV(pdrcsvRecords);

                String csvFileName = "PDR_" + LocalDateTime.now()
                                                           .truncatedTo(ChronoUnit.SECONDS) + ".csv";

                csvFileName = csvFileName.replaceAll(":", "-");

                String csvKey = "PDR/" + csvFileName;

                s3Client.putObject(builder -> builder.bucket(destinationBucket)
                                                     .key(csvKey)
                                                     .contentType("text/csv")
                                                     .expectedBucketOwner(AWS_Account_Id)
                                                     .build(),
                        RequestBody.fromString(csvContent, StandardCharsets.UTF_8));


            } catch (Exception e) {
                logln("Error: " + e.getMessage());
                response = "500 Internal Server Error";
            }

        }
        return response;
    }

    /**
     * Generates a CSV string from a list of PDRCSVRecord objects using Apache Commons CSV
     *
     * @param pdrcsvRecords The list of PDRCSVRecord objects
     * @return A CSV string representation of the records
     */
    private String generateCSV(List<PDRCSVRecord> pdrcsvRecords) {
        StringWriter stringWriter = new StringWriter();
        try (CSVPrinter csvPrinter = new CSVPrinter(stringWriter, CSVFormat.DEFAULT
                .builder()
                .setHeader("ContactId", "Channel", "FileType", "S3PreSignedURL")
                .build())) {

            for (PDRCSVRecord record : pdrcsvRecords) {
                csvPrinter.printRecord(
                        record.contactId(),
                        record.channel(),
                        record.fileType(),
                        record.s3PreSignedURL()
                );
            }
        } catch (IOException e) {
            logln("Error generating CSV: " + e.getMessage());
        }

        return stringWriter.toString();
    }

    /**
     * Processes contact files from S3 based on the provided contact ID details
     *
     * @param contactIdDetails A list of maps containing contact IDs and their S3 paths
     * @return A list of PDRCSVRecord objects with file information and presigned URLs
     */
    private List<PDRCSVRecord> processContactFiles(List<Map<String, String>> contactIdDetails) {
        List<PDRCSVRecord> pdrcsvRecords = new ArrayList<>();

        try {
            // Process each entry in the contactIdDetails
            for (Map<String, String> contactMap : contactIdDetails) {
                // Each entry in contactIdDetails is a map of contactId -> s3Path
                for (Map.Entry<String, String> entry : contactMap.entrySet()) {
                    String contactId = entry.getKey();
                    String s3Path = entry.getValue();

                    logln("Processing files for Contact ID: " + contactId);
                    logln("S3 Path: " + s3Path);
                    String[] parts = s3Path.split("/", 2);
                    logln("Bucket: " + parts[0]);
                    logln("Prefix: " + parts[1]);

                    // List objects in the S3 bucket with the given prefix
                    ListObjectsV2Request listObjectsRequest =
                            ListObjectsV2Request.builder()
                                                .bucket(parts[0])
                                                .prefix(parts[1])
                                                .build();

                    ListObjectsV2Response listObjectsResponse;
                    do {
                        listObjectsResponse = s3Client.listObjectsV2(listObjectsRequest);

                        listObjectsResponse.contents()
                                           .forEach(s3Object -> {
                                               String channel = "CHAT";
                                               if (s3Object.key()
                                                           .contains("Voice")) {
                                                   channel = "VOICE";
                                               }
                                               if (s3Object.key()
                                                           .endsWith(".wav")) {
                                                   pdrcsvRecords.add(new PDRCSVRecord(contactId, channel, "RECORDING", generatePresignedUrl(parts[0], s3Object.key())));
                                               } else if (s3Object.key()
                                                                  .endsWith(".json")) {
                                                   // For JSON files, process with Bedrock LLM and generate a human-readable transcript
                                                   try {
                                                       // Process JSON transcript with Bedrock LLM
                                                       String jsonContent = readJsonFromS3(parts[0], s3Object.key());
                                                       String humanReadableTranscript = generateHumanReadableTranscript(jsonContent);

                                                       // Save human-readable transcript to S3
                                                       String humanReadableKey = s3Object.key()
                                                                                         .replace(".json", "_TRANSCRIPT.txt");
                                                       uploadToS3(parts[0], humanReadableKey, humanReadableTranscript);

                                                       // Generate presigned URL for the human-readable transcript
                                                       String humanReadablePresignedUrl = generatePresignedUrl(parts[0], humanReadableKey);
                                                       pdrcsvRecords.add(new PDRCSVRecord(contactId, channel, "TRANSCRIPT", humanReadablePresignedUrl));
                                                   } catch (Exception e) {
                                                       logln("Error processing JSON transcript: " + e.getMessage());
                                                       // Fall back to just providing the JSON file
                                                       pdrcsvRecords.add(new PDRCSVRecord(contactId, channel, "TRANSCRIPT", generatePresignedUrl(parts[0], s3Object.key())));
                                                   }
                                               }
                                           });

                        listObjectsRequest = listObjectsRequest.toBuilder()
                                                               .continuationToken(listObjectsResponse.nextContinuationToken())
                                                               .build();
                    } while (listObjectsResponse.isTruncated());
                }
            }
        } catch (Exception e) {
            logln("Error processing contact files: " + e.getMessage());
        }
        return pdrcsvRecords;
    }

    /**
     * Reads a JSON file from S3 and returns its content as a string
     *
     * @param bucket The S3 bucket name
     * @param key    The S3 object key
     * @return The JSON content as a string
     * @throws IOException If there's an error reading the file
     */
    // import java.util.concurrent.atomic.AtomicInteger;
    // AtomicInteger is used to safely count the number of lines read
    private String readJsonFromS3(String bucket, String key) throws IOException {
        GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                                                            .bucket(bucket)
                                                            .key(key)
                                                            .expectedBucketOwner(AWS_Account_Id)
                                                            .build();

        ResponseInputStream<GetObjectResponse> s3Object = s3Client.getObject(getObjectRequest);

        StringBuilder jsonContent = new StringBuilder();
        AtomicInteger lineCount = new AtomicInteger(0);
        try (BufferedReader br = new BufferedReader(new InputStreamReader(s3Object, StandardCharsets.UTF_8))) {
            String line;
            while ((line = br.readLine()) != null && lineCount.incrementAndGet() <= 10000) {
                jsonContent.append(line);
            }
        }

        return jsonContent.toString();
    }

    /**
     * Generates a human-readable transcript from JSON content using Bedrock LLM
     *
     * @param jsonContent The JSON transcript content
     * @return A human-readable transcript with AGENT and CUSTOMER keys
     * @throws JsonProcessingException If there's an error processing the JSON
     */
    private String generateHumanReadableTranscript(String jsonContent) throws JsonProcessingException {
        // Parse the JSON content
        JsonNode jsonNode = objectMapper.readTree(jsonContent);

        // Create a prompt for Bedrock LLM
        String prompt = createBedrockPrompt(jsonNode);

        // Call Bedrock LLM
        return invokeBedrockModel(prompt);
    }

    /**
     * Creates a prompt for Bedrock LLM based on the JSON transcript
     *
     * @param jsonNode The JSON transcript as a JsonNode
     * @return A prompt for Bedrock LLM
     */
    private String createBedrockPrompt(JsonNode jsonNode) {
        return "Human: I have a JSON transcript from an Amazon Connect conversation. " +
                "Please convert it into a human-readable format with AGENT and CUSTOMER as the keys. " +
                "Here is the JSON transcript:\n\n" +
                jsonNode.toString() +
                "\n\nAssistant:";
    }

    /**
     * Invokes Bedrock LLM with the given prompt using the Bedrock Converse API format
     *
     * @param prompt The prompt for Bedrock LLM
     * @return The response from Bedrock LLM
     */
    private String invokeBedrockModel(String prompt) {
        try {

            ConverseRequest converseRequest = ConverseRequest.builder()
                                                             .modelId(bedrockModelId)
                                                             .messages(
                                                                     Message.builder()
                                                                            .role(ConversationRole.USER)
                                                                            .content(ContentBlock.builder()
                                                                                                 .text(prompt)
                                                                                                 .build())
                                                                            .build())
                                                             .inferenceConfig(InferenceConfiguration.builder()
                                                                                                    .temperature(0f)
                                                                                                    .build())
                                                             .build();

            ConverseResponse converseResponse = bedrockClient.converse(converseRequest);

            return converseResponse.output()
                                   .message()
                                   .content()
                                   .getFirst()
                                   .text();

        } catch (Exception e) {
            logln("Error invoking Bedrock model: " + e.getMessage());
            return "Error: " + e.getMessage();
        }
    }

    /**
     * Uploads content to S3
     *
     * @param bucket  The S3 bucket name
     * @param key     The S3 object key
     * @param content The content to upload
     */
    private void uploadToS3(final String bucket, final String key, final String content) {
        try {
            s3Client.putObject(PutObjectRequest.builder()
                                               .bucket(bucket)
                                               .key(key)
                                               .contentType("text/plain")
                                               .expectedBucketOwner(AWS_Account_Id)
                                               .build(),
                    RequestBody.fromString(content, StandardCharsets.UTF_8));

            logln("Successfully uploaded human-readable transcript to S3: " + bucket + "/" + key);
        } catch (Exception e) {
            logln("Error uploading to S3: " + e.getMessage());
            throw new RuntimeException("Failed to upload to S3", e);
        }
    }

    /**
     * Record to hold CSV row data for public disclosure requests
     *
     * @param contactId      The contact ID
     * @param channel        The contact channel (VOICE, CHAT)
     * @param fileType       The type of file (RECORDING, TRANSCRIPT)
     * @param s3PreSignedURL The presigned URL for accessing the file
     */
    record PDRCSVRecord(String contactId, String channel, String fileType, String s3PreSignedURL) {
    }

    /**
     * Reads a CSV file from S3 and converts it to a list of PDRCSVRecord objects
     *
     * @param bucket The S3 bucket name
     * @param key    The S3 object key
     * @return A list of PDRCSVRecord objects
     * @throws IOException If there's an error reading the file
     */
    private List<PDRCSVRecord> readCsvFromS3(String bucket, String key) throws IOException {
        List<PDRCSVRecord> records = new ArrayList<>();

        // To handle special characters in the S3 key
        String decodedKey = URLDecoder.decode(key, StandardCharsets.UTF_8);

        GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                                                            .bucket(bucket)
                                                            .key(decodedKey)
                                                            .build();

        ResponseInputStream<GetObjectResponse> s3Object = s3Client.getObject(getObjectRequest);

        try (BufferedReader br = new BufferedReader(new InputStreamReader(
                s3Object))) {

            // Skip header row
            br.readLine();

            // Read data rows
            String line;
            int lineCount = 0;
            final int MAX_LINES = 1000000; // Set a reasonable limit

            while ((line = br.readLine()) != null && lineCount < MAX_LINES) {
                String[] values = line.split(",");
                if (values.length >= 1) {
                    records.add(new PDRCSVRecord(
                            values[0].trim(), // contactIds
                            "", // channel
                            "", // fileType
                            ""  // s3PreSignedURL
                    ));
                }
                lineCount++;
            }

            if (lineCount >= MAX_LINES) {
                throw new IOException("Exceeded maximum number of lines allowed");
            }
        }

        return records;
    }

    /**
     * Generates an S3 prefix based on the contact ID and contact timestamp
     *
     * @param contactId The contact ID
     * @param contact   The Contact object containing contact details
     * @return The S3 prefix in the format year/month/day/contactId
     */
    private static String getS3Prefix(String contactId, Contact contact) {
        Instant contactTimestamp = contact.initiationTimestamp();
        LocalDateTime contactLocalDateTime = LocalDateTime.ofInstant(contactTimestamp, ZoneId.systemDefault());

        String year = String.valueOf(contactLocalDateTime.getYear());
        String month = String.format("%02d", contactLocalDateTime.getMonthValue());
        String day = String.format("%02d", contactLocalDateTime.getDayOfMonth());

        return year + "/" + month + "/" + day + "/" + contactId;
    }

    // amazonq-ignore-next-line

    /**
     * Retrieves the S3 bucket name from the Connect instance storage configuration
     *
     * @param instanceId          The Connect instance ID
     * @param storageResourceType The type of storage resource (e.g., CALL_RECORDINGS, CHAT_TRANSCRIPTS)
     * @return The S3 bucket name or an error message if not found
     */
    private String getStorageS3BucketName(String instanceId, InstanceStorageResourceType storageResourceType) {
        ListInstanceStorageConfigsRequest storageConfigsRequest_CR = ListInstanceStorageConfigsRequest.builder()
                                                                                                      .instanceId(instanceId)
                                                                                                      .resourceType(storageResourceType)
                                                                                                      .build();
        ListInstanceStorageConfigsResponse storageConfigsResponse = connectClient.listInstanceStorageConfigs(storageConfigsRequest_CR);

        List<InstanceStorageConfig> storageConfigs = storageConfigsResponse.storageConfigs();
        for (InstanceStorageConfig storageConfig : storageConfigs) {
            logln("Storage config: " + storageConfig.storageTypeAsString());
            if ("S3".equalsIgnoreCase(storageConfig.storageTypeAsString())) {
                return storageConfig.s3Config()
                                    .bucketName();
            }
        }
        return "No S3 Bucket Found, please check the Storage Config";
    }

    /**
     * Generates a presigned URL for an S3 object
     *
     * @param bucketName The S3 bucket name
     * @param objectKey  The S3 object key
     * @return A presigned URL for accessing the S3 object
     * @throws RuntimeException If there's an error generating the presigned URL
     */
    private String generatePresignedUrl(String bucketName, String objectKey) {
        try {
            // Create a GetObjectRequest to be pre-signed
            GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                                                                .bucket(bucketName)
                                                                .key(objectKey)
                                                                .build();

            // Create a GetObjectPresignRequest to specify the signature duration
            GetObjectPresignRequest getObjectPresignRequest = GetObjectPresignRequest.builder()
                                                                                     .signatureDuration(Duration.ofDays(7)) // URL expires in 7 days
                                                                                     .getObjectRequest(getObjectRequest)
                                                                                     .build();

            // Generate the presigned URL
            return s3PreSigner.presignGetObject(getObjectPresignRequest)
                              .url()
                              .toString();
        } catch (Exception e) {
            throw new RuntimeException("Failed to generate presigned URL", e);
        }
    }

    /**
     * Retrieves contact details from Amazon Connect and maps them to S3 paths
     *
     * @param contactIdList A list of PDRCSVRecord objects containing contact IDs
     * @param instanceId    The Connect instance ID
     * @return A list of maps containing contact IDs and their S3 paths
     */
    private List<Map<String, String>> getContactIdDetails(List<PDRCSVRecord> contactIdList, String instanceId) {
        try {
            logln("Getting Connect Instance Storage details in instance: " + instanceId);

            String s3BucketName_CallRecordings, s3BucketName_ChatTranscripts, s3BucketName_same = "";
            String callRecordings_Prefix = "/Analysis/Voice/Redacted/", chatTranscripts_Prefix = "/Analysis/Chat/Redacted/";
            boolean sameBucket = false;

            // Create a Map to store ContactId and its Prefix details
            Map<String, String> s3PrefixMap = new HashMap<>();

            s3BucketName_CallRecordings = getStorageS3BucketName(instanceId, InstanceStorageResourceType.CALL_RECORDINGS);
            s3BucketName_ChatTranscripts = getStorageS3BucketName(instanceId, InstanceStorageResourceType.CHAT_TRANSCRIPTS);

            logln("s3BucketName_CallRecordings = " + s3BucketName_CallRecordings);
            logln("s3BucketName_ChatTranscripts = " + s3BucketName_ChatTranscripts);

            if (s3BucketName_CallRecordings.equalsIgnoreCase(s3BucketName_ChatTranscripts)) {
                sameBucket = true;
                s3BucketName_same = s3BucketName_CallRecordings;
            }

            for (PDRCSVRecord record : contactIdList) {
                String contactId = record.contactId();
                logln("Contact ID: " + contactId);
                DescribeContactResponse describeContactResponse = connectClient.describeContact(DescribeContactRequest.builder()
                                                                                                                      .contactId(contactId)
                                                                                                                      .instanceId(instanceId)
                                                                                                                      .build());
                Contact contact = describeContactResponse.contact();

                String s3Prefix = getS3Prefix(contactId, contact);
                if (sameBucket) {
                    if (contact.channelAsString()
                               .equalsIgnoreCase("Chat")) {
                        s3PrefixMap.put(contactId, s3BucketName_same + chatTranscripts_Prefix + s3Prefix);
                    } else if (contact.channelAsString()
                                      .equalsIgnoreCase("Voice")) {
                        s3PrefixMap.put(contactId, s3BucketName_same + callRecordings_Prefix + s3Prefix);
                    }
                } else {
                    if (contact.channelAsString()
                               .equalsIgnoreCase("Chat")) {
                        s3PrefixMap.put(contactId, s3BucketName_ChatTranscripts + chatTranscripts_Prefix + s3Prefix);
                    } else if (contact.channelAsString()
                                      .equalsIgnoreCase("Voice")) {
                        s3PrefixMap.put(contactId, s3BucketName_CallRecordings + callRecordings_Prefix + s3Prefix);
                    }
                }
            }
            logln("s3PrefixMap = " + s3PrefixMap);
            return List.of(s3PrefixMap);

        } catch (Exception e) {
            logln("Error getting contact details: " + e.getMessage());

            // Return error as JSON
            Map<String, String> errorMap = new HashMap<>();
            errorMap.put("error", "Failed to retrieve contact details: " + e.getMessage());
            return List.of(errorMap);
        }
    }

    /**
     * Helper method to log a message followed by a newline
     *
     * @param message The message to log
     */
    private void logln(String message) {
        logger.log(message + "\n");
    }

    String getAccountId(Context context) {
        String arn = context.getInvokedFunctionArn();
        // ARN format: arn:aws:lambda:region:account-id:function:function-name
        String[] arnParts = arn.split(":");
        return arnParts[4]; // The account ID is the 5th element (index 4)
    }

}
