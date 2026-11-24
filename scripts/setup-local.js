// Creates the bucket, queue and DLQ in the configured endpoint (intended for the local moto emulator).
// For real AWS follow the README instead: this script would create billable resources.
import {
  S3Client, CreateBucketCommand, PutBucketEncryptionCommand, PutBucketLifecycleConfigurationCommand,
  PutPublicAccessBlockCommand,
} from '@aws-sdk/client-s3';
import { SQSClient, CreateQueueCommand, GetQueueAttributesCommand } from '@aws-sdk/client-sqs';
import { loadConfig } from '../src/config/env.js';

const config = loadConfig();
if (!config.aws.endpoint) {
  console.error('AWS_ENDPOINT_URL is not set. Refusing to create resources in real AWS from this script.');
  process.exit(1);
}

const s3 = new S3Client({ region: config.aws.region, endpoint: config.aws.endpoint, forcePathStyle: true });
const sqs = new SQSClient({ region: config.aws.region, endpoint: config.aws.endpoint });
const bucket = config.s3.bucket;
const name = (url) => url.split('/').pop();

async function tolerate(label, fn) {
  try { await fn(); console.log(`ok   ${label}`); } catch (e) { console.warn(`warn ${label}: ${e.name}`); }
}

await tolerate(`bucket ${bucket}`, () => s3.send(new CreateBucketCommand({ Bucket: bucket })));
await tolerate('block public access', () => s3.send(new PutPublicAccessBlockCommand({
  Bucket: bucket,
  PublicAccessBlockConfiguration: { BlockPublicAcls: true, IgnorePublicAcls: true, BlockPublicPolicy: true, RestrictPublicBuckets: true },
})));
await tolerate('default encryption', () => s3.send(new PutBucketEncryptionCommand({
  Bucket: bucket,
  ServerSideEncryptionConfiguration: { Rules: [{ ApplyServerSideEncryptionByDefault: { SSEAlgorithm: 'AES256' } }] },
})));
await tolerate('abort incomplete multipart uploads after 1 day', () => s3.send(new PutBucketLifecycleConfigurationCommand({
  Bucket: bucket,
  LifecycleConfiguration: { Rules: [{ ID: 'abort-incomplete-mpu', Status: 'Enabled', Filter: { Prefix: '' }, AbortIncompleteMultipartUpload: { DaysAfterInitiation: 1 } }] },
})));

const dlq = await sqs.send(new CreateQueueCommand({ QueueName: name(config.sqs.dlqUrl) }));
const { Attributes } = await sqs.send(new GetQueueAttributesCommand({ QueueUrl: dlq.QueueUrl, AttributeNames: ['QueueArn'] }));
const main = await sqs.send(new CreateQueueCommand({
  QueueName: name(config.sqs.queueUrl),
  Attributes: {
    VisibilityTimeout: String(config.sqs.visibilityTimeoutSeconds),
    RedrivePolicy: JSON.stringify({ deadLetterTargetArn: Attributes.QueueArn, maxReceiveCount: config.sqs.maxReceiveCount }),
  },
}));
console.log(`ok   queue ${main.QueueUrl}`);
console.log(`ok   dlq   ${dlq.QueueUrl}`);
console.log('Make sure SQS_QUEUE_URL / SQS_DLQ_URL in your environment match the URLs above.');
