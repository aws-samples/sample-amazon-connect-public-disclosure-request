/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: MIT-0
 */

package com.amazonaws.cdk;

import io.github.cdklabs.cdknag.NagPackSuppression;
import io.github.cdklabs.cdknag.NagSuppressions;
import software.amazon.awscdk.*;
import software.amazon.awscdk.services.dynamodb.*;
import software.amazon.awscdk.services.iam.*;
import software.amazon.awscdk.services.kms.Key;
import software.amazon.awscdk.services.lambda.*;
import software.amazon.awscdk.services.lambda.Runtime;
import software.amazon.awscdk.services.logs.LogGroup;
import software.amazon.awscdk.services.logs.RetentionDays;
import software.amazon.awscdk.services.s3.BlockPublicAccess;
import software.amazon.awscdk.services.s3.Bucket;
import software.amazon.awscdk.services.s3.BucketEncryption;
import software.amazon.awscdk.services.s3.notifications.LambdaDestination;
import software.amazon.awscdk.services.ssm.ParameterDataType;
import software.amazon.awscdk.services.ssm.StringParameter;
import software.constructs.Construct;

import java.util.List;
import java.util.Map;

public class InfraStack extends Stack {
    public InfraStack(final Construct scope, final String id) {
        this(scope, id, null);
    }

    public InfraStack(final Construct scope, final String id, final StackProps props) {
        super(scope, id, props);

        // Access the Context file to get values for Parameters
        String instanceId = this.getNode()
                                .tryGetContext("InstanceId")
                                .toString();
        String connectStorageBucket = this.getNode()
                                          .tryGetContext("ConnectStorageBucket")
                                          .toString();
        String bedrockModelId = this.getNode()
                                    .tryGetContext("BedrockModelId")
                                    .toString();


        Key loggingBucketKey = Key.Builder.create(this, "LoggingBucketKey")
                                          .enableKeyRotation(true)
                                          .pendingWindow(Duration.days(7))
                                          .removalPolicy(RemovalPolicy.DESTROY)
                                          .build();

        Bucket loggingBucket = Bucket.Builder.create(this, "LoggingBucket")
                                             .enforceSsl(true)
                                             .encryption(BucketEncryption.KMS)
                                             .encryptionKey(loggingBucketKey)
                                             .blockPublicAccess(BlockPublicAccess.BLOCK_ALL)
                                             .versioned(true)
                                             .removalPolicy(RemovalPolicy.DESTROY)
                                             .autoDeleteObjects(true)
                                             .build();

        Key sourceBucketKey = Key.Builder.create(this, "SourceBucketKey")
                                         .enableKeyRotation(true)
                                         .pendingWindow(Duration.days(7))
                                         .removalPolicy(RemovalPolicy.DESTROY)
                                         .build();

        Bucket sourceBucket = Bucket.Builder.create(this, "SourceBucket")
                                            .enforceSsl(true)
                                            .versioned(true)
                                            .encryption(BucketEncryption.KMS)
                                            .encryptionKey(sourceBucketKey)
                                            .blockPublicAccess(BlockPublicAccess.BLOCK_ALL)
                                            .serverAccessLogsBucket(loggingBucket)
                                            .serverAccessLogsPrefix("sourceBucket/")
                                            .removalPolicy(RemovalPolicy.DESTROY)
                                            .autoDeleteObjects(true)
                                            .build();

        Key destinationBucketKey = Key.Builder.create(this, "DestinationBucketKey")
                                              .enableKeyRotation(true)
                                              .pendingWindow(Duration.days(7))
                                              .removalPolicy(RemovalPolicy.DESTROY)
                                              .build();

        Bucket destinationBucket = Bucket.Builder.create(this, "DestinationBucket")
                                                 .enforceSsl(true)
                                                 .versioned(true)
                                                 .encryption(BucketEncryption.KMS)
                                                 .encryptionKey(destinationBucketKey)
                                                 .blockPublicAccess(BlockPublicAccess.BLOCK_ALL)
                                                 .serverAccessLogsBucket(loggingBucket)
                                                 .serverAccessLogsPrefix("destinationBucket/")
                                                 .removalPolicy(RemovalPolicy.DESTROY)
                                                 .autoDeleteObjects(true)
                                                 .build();

        // Create an IAM role for the Lambda function
        Role lambdaRole = Role.Builder.create(this, "PDRLambdaRole")
                                      .assumedBy(new ServicePrincipal("lambda.amazonaws.com"))
                                      .build();

        // Create a policy statement for Amazon Bedrock - restrict to specific model
        PolicyStatement bedrockStatement = PolicyStatement.Builder.create()
                                                                  .effect(Effect.ALLOW)
                                                                  .actions(List.of("bedrock:InvokeModel"))
                                                                  .resources(List.of("arn:" + getPartition() + ":bedrock:" + getRegion() + "::foundation-model/" + bedrockModelId))
                                                                  .build();

        // Create a policy statement for Amazon Connect
        PolicyStatement connectStatement = PolicyStatement.Builder.create()
                                                                  .effect(Effect.ALLOW)
                                                                  .actions(List.of(
                                                                          "connect:DescribeContact",
                                                                          "connect:ListInstanceStorageConfigs"
                                                                  ))
                                                                  .resources(List.of(
                                                                          "arn:" + getPartition() + ":connect:" + getRegion() + ":" + getAccount() + ":instance/" + instanceId,
                                                                          "arn:" + getPartition() + ":connect:" + getRegion() + ":" + getAccount() + ":instance/" + instanceId + "/*"
                                                                  ))
                                                                  .build();

        // Create a policy statement for Amazon S3 Connect Storage Bucket
        PolicyStatement s3Statement = PolicyStatement.Builder.create()
                                                             .effect(Effect.ALLOW)
                                                             .actions(List.of(
                                                                     "s3:ListBucket",
                                                                     "s3:GetObject",
                                                                     "s3:PutObject"
                                                             ))
                                                             .resources(List.of(
                                                                     "arn:" + getPartition() + ":s3:::" + connectStorageBucket,
                                                                     "arn:" + getPartition() + ":s3:::" + connectStorageBucket + "/*"
                                                             ))
                                                             .build();

        lambdaRole.addToPolicy(bedrockStatement);
        lambdaRole.addToPolicy(connectStatement);
        lambdaRole.addToPolicy(s3Statement);

        // Create the Lambda function first to get its name for the log group
        Function pdrFunction = Function.Builder.create(this, "PDRFunction")
                                               .runtime(Runtime.JAVA_21)
                                               .architecture(Architecture.X86_64)
                                               .handler("com.amazonaws.lambda.PDRFunction")
                                               .memorySize(1024)
                                               .timeout(Duration.minutes(10))
                                               .code(Code.fromAsset("../assets/PDRFunction.jar"))
                                               .environment(Map.of(
                                                       "DestinationBucket", destinationBucket.getBucketName(),
                                                       "Instance_Id", instanceId,
                                                       "PresignedUrlExpiration", "86400",
                                                       "BedrockModelId", bedrockModelId,
                                                       "ConnectStorageBucket", connectStorageBucket,
                                                       "BedrockTimeoutInSecs", "600"
                                               ))
                                               .role(lambdaRole)
                                               .build();

        // Create the CloudWatch Log Group explicitly with retention policy
        LogGroup pdrFunctionLogGroup = LogGroup.Builder.create(this, "PDRFunctionLogGroup")
                                                       .logGroupName("/aws/lambda/" + pdrFunction.getFunctionName())
                                                       .retention(RetentionDays.ONE_MONTH)
                                                       .removalPolicy(RemovalPolicy.DESTROY)
                                                       .build();

        // Create a policy statement for CloudWatch Logs - specific to the PDR function log group
        PolicyStatement logsStatement = PolicyStatement.Builder.create()
                                                               .effect(Effect.ALLOW)
                                                               .actions(List.of("logs:CreateLogStream", "logs:PutLogEvents"))
                                                               .resources(List.of(pdrFunctionLogGroup.getLogGroupArn() + ":*"))
                                                               .build();

        pdrFunction.getRole()
                   .attachInlinePolicy(Policy.Builder.create(this, "LogsPolicy")
                                                     .document(PolicyDocument.Builder.create()
                                                                                     .statements(List.of(logsStatement))
                                                                                     .build())
                                                     .build());

        // Add Object Created Notification to Source Bucket
        LambdaDestination lambdaDestination = new LambdaDestination(pdrFunction);
        sourceBucket.addObjectCreatedNotification(lambdaDestination);

        // AWS Lambda Execution Permission
        sourceBucket.grantRead(pdrFunction);
        destinationBucket.grantWrite(pdrFunction);

        // Output the source bucket name
        CfnOutput.Builder.create(this, "SourceBucketName")
                         .description("The name of the S3 bucket where PDR CSV files should be uploaded")
                         .value(sourceBucket.getBucketName())
                         .build();

        // Output the destination bucket name
        CfnOutput.Builder.create(this, "DestinationBucketName")
                         .description("The name of the S3 bucket where PDR results will be stored")
                         .value(destinationBucket.getBucketName())
                         .build();

        // Output the PDRFunction
        CfnOutput.Builder.create(this, "PDRFunctionName")
                         .description("The name of the Lambda function")
                         .value(pdrFunction.getFunctionArn())
                         .build();

        // CDK NAG Suppression
        NagSuppressions.addResourceSuppressionsByPath(this, "/InfraStack/BucketNotificationsHandler050a0587b7544547bf325f094a3db834/Role/Resource",
                List.of(NagPackSuppression.builder()
                                          .id("AwsSolutions-IAM4")
                                          .reason("Internal CDK lambda needed to apply bucket notification configurations")
                                          .appliesTo(List.of("Policy::arn:<AWS::Partition>:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"))
                                          .build(),
                        NagPackSuppression.builder()
                                          .id("AwsSolutions-IAM5")
                                          .reason("Internal CDK lambda needed to apply bucket notification configurations")
                                          .appliesTo(List.of("Resource::*"))
                                          .build()));

        NagSuppressions.addResourceSuppressionsByPath(this, "/InfraStack/BucketNotificationsHandler050a0587b7544547bf325f094a3db834/Role/DefaultPolicy/Resource",
                List.of(NagPackSuppression.builder()
                                          .id("AwsSolutions-IAM5")
                                          .reason("Internal CDK construct requires wildcard permissions to configure bucket notifications")
                                          .appliesTo(List.of("Resource::*"))
                                          .build()));

        NagSuppressions.addResourceSuppressionsByPath(this, "/InfraStack/PDRLambdaRole/DefaultPolicy/Resource",
                List.of(
                        NagPackSuppression.builder()
                                          .id("AwsSolutions-IAM5")
                                          .reason("S3 operations require object-level access with wildcard")
                                          .appliesTo(List.of(
                                                  "Resource::arn:<AWS::Partition>:s3:::" + connectStorageBucket + "/*",
                                                  "Resource::<SourceBucketDDD2130A.Arn>/*",
                                                  "Resource::<DestinationBucket4BECDB47.Arn>/*",
                                                  "Action::s3:GetBucket*",
                                                  "Action::s3:GetObject*",
                                                  "Action::s3:List*",
                                                  "Action::s3:Abort*",
                                                  "Action::s3:DeleteObject*"
                                          ))
                                          .build(),
                        NagPackSuppression.builder()
                                          .id("AwsSolutions-IAM5")
                                          .reason("KMS operations for S3 encryption require these action patterns")
                                          .appliesTo(List.of(
                                                  "Action::kms:GenerateDataKey*",
                                                  "Action::kms:ReEncrypt*"
                                          ))
                                          .build(),
                        NagPackSuppression.builder()
                                          .id("AwsSolutions-IAM5")
                                          .reason("KMS operations for Connect storage bucket encryption require wildcard resource access")
                                          .appliesTo(List.of("Resource::*"))
                                          .build(),
                        NagPackSuppression.builder()
                                          .id("AwsSolutions-IAM5")
                                          .reason("Connect instance operations require wildcard access to contacts and storage configurations within the specific instance")
                                          .appliesTo(List.of("Resource::arn:<AWS::Partition>:connect:<AWS::Region>:<AWS::AccountId>:instance/" + instanceId + "/*"))
                                          .build()
                ));

        NagSuppressions.addResourceSuppressionsByPath(this, "/InfraStack/LogsPolicy/Resource",
                List.of(NagPackSuppression.builder()
                                          .id("AwsSolutions-IAM5")
                                          .reason("Lambda logging requires wildcard for log streams within the specific log group")
                                          .appliesTo(List.of("Resource::<PDRFunctionLogGroup44723FF1.Arn>:*"))
                                          .build()));
    }
}