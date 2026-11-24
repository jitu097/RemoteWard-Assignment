import { S3Client, GetObjectCommand } from '@aws-sdk/client-s3';
import { Upload } from '@aws-sdk/lib-storage';

export function createS3Client(config) {
  return new S3Client({
    region: config.aws.region,
    endpoint: config.aws.endpoint,
    forcePathStyle: config.aws.forcePathStyle,
  });
}

export function createS3Service({ client, bucket }) {
  return {
    /**
     * Streams `body` to S3 using multipart upload for large objects (5 MiB parts, 4 in flight,
     * so memory use is bounded to roughly 20 MiB per upload). Aborts the multipart upload on failure.
     */
    async upload({ key, body, contentType }) {
      const upload = new Upload({
        client,
        queueSize: 4,
        partSize: 5 * 1024 * 1024,
        leavePartsOnError: false,
        params: { Bucket: bucket, Key: key, Body: body, ContentType: contentType, ServerSideEncryption: 'AES256' },
      });
      try {
        await upload.done();
      } catch (err) {
        await upload.abort().catch(() => {});
        throw err;
      }
    },

    /** Returns the object body as a Node readable stream. */
    async download({ bucket: b = bucket, key }) {
      const res = await client.send(new GetObjectCommand({ Bucket: b, Key: key }));
      return res.Body;
    },
  };
}
