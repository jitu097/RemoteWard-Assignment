import { AppError, errors } from '../../utils/errors.js';
import { isJobId } from '../../utils/events.js';

export function createJobsController({ jobs }) {
  async function loadJob(req) {
    const { jobId } = req.params;
    if (!isJobId(jobId)) throw errors.badRequest('INVALID_JOB_ID', 'jobId must be a UUID');
    let job;
    try {
      job = await jobs.get(jobId);
    } catch {
      throw errors.unavailable('JOB_STORE_UNAVAILABLE', 'Job store is temporarily unavailable');
    }
    if (!job) throw errors.notFound('JOB_NOT_FOUND', 'Job not found (it may have expired)');
    return job;
  }

  return {
    async getJob(req, res) {
      const job = await loadJob(req);
      const { jobId, status, file, attempts, error, createdAt, updatedAt, completedAt } = job;
      res.json({
        success: true,
        data: {
          jobId, status, fileName: file.originalName, attempts, createdAt, updatedAt,
          ...(completedAt && { completedAt }),
          ...(error && { error }),
          ...(status === 'completed' && { resultUrl: `/api/v1/jobs/${jobId}/result` }),
        },
      });
    },

    async getResult(req, res) {
      const job = await loadJob(req);
      if (job.status !== 'completed') {
        throw new AppError(409, 'RESULT_NOT_READY', `Job is ${job.status}; no result available`);
      }
      let result;
      try {
        result = await jobs.getResult(job.jobId);
      } catch {
        throw errors.unavailable('JOB_STORE_UNAVAILABLE', 'Job store is temporarily unavailable');
      }
      if (!result) throw new AppError(410, 'RESULT_EXPIRED', 'Result has expired and is no longer available');
      res.json({ success: true, data: { jobId: job.jobId, status: job.status, result } });
    },
  };
}
