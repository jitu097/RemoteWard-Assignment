import {
  SQSClient,
  SendMessageCommand,
  ReceiveMessageCommand,
  DeleteMessageCommand,
} from '@aws-sdk/client-sqs';

export function createSqsClient(config) {
  return new SQSClient({ region: config.aws.region, endpoint: config.aws.endpoint });
}

export function createSqsService({ client, queueUrl, visibilityTimeoutSeconds }) {
  return {
    /** Publishes an event (metadata only) as the message body. */
    async publish(event) {
      const res = await client.send(new SendMessageCommand({ QueueUrl: queueUrl, MessageBody: JSON.stringify(event) }));
      return res.MessageId;
    },

    /** Long-polls (max 20 s) for up to `max` messages. */
    async receive({ max, waitSeconds = 20, abortSignal }) {
      const res = await client.send(
        new ReceiveMessageCommand({
          QueueUrl: queueUrl,
          MaxNumberOfMessages: max,
          WaitTimeSeconds: Math.min(waitSeconds, 20),
          VisibilityTimeout: visibilityTimeoutSeconds,
          AttributeNames: ['ApproximateReceiveCount'],
        }),
        { abortSignal },
      );
      return res.Messages ?? [];
    },

    async delete(receiptHandle) {
      await client.send(new DeleteMessageCommand({ QueueUrl: queueUrl, ReceiptHandle: receiptHandle }));
    },
  };
}
