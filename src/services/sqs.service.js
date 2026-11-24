import {
  SQSClient,
  SendMessageCommand,
  ReceiveMessageCommand,
  DeleteMessageCommand,
  ChangeMessageVisibilityCommand,
} from '@aws-sdk/client-sqs';

export function createSqsClient(config) {
  return new SQSClient({ region: config.aws.region, endpoint: config.aws.endpoint });
}

export function createSqsService({ client, queueUrl, dlqUrl, visibilityTimeoutSeconds }) {
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
          MessageSystemAttributeNames: ['ApproximateReceiveCount'],
        }),
        { abortSignal },
      );
      return res.Messages ?? [];
    },

    async delete(receiptHandle) {
      await client.send(new DeleteMessageCommand({ QueueUrl: queueUrl, ReceiptHandle: receiptHandle }));
    },

    /** Restarts the visibility timeout so a long-running job is not redelivered to another worker. */
    async extendVisibility(receiptHandle) {
      await client.send(
        new ChangeMessageVisibilityCommand({
          QueueUrl: queueUrl,
          ReceiptHandle: receiptHandle,
          VisibilityTimeout: visibilityTimeoutSeconds,
        }),
      );
    },

    /** Copies a message that can never succeed straight to the dead-letter queue, with the reason attached. */
    async sendToDlq(body, reason) {
      await client.send(
        new SendMessageCommand({
          QueueUrl: dlqUrl,
          MessageBody: body,
          MessageAttributes: { failureReason: { DataType: 'String', StringValue: String(reason).slice(0, 500) } },
        }),
      );
    },
  };
}
