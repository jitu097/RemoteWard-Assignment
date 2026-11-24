import { Router } from 'express';
import { createUploadController } from '../controllers/uploads.controller.js';
import { createJobsController } from '../controllers/jobs.controller.js';

export function createRoutes({ limiters, ...deps }) {
  const router = Router();
  const jobsController = createJobsController(deps);

  router.post('/uploads', limiters.upload, createUploadController(deps));
  router.get('/jobs/:jobId', limiters.read, jobsController.getJob);
  router.get('/jobs/:jobId/result', limiters.read, jobsController.getResult);
  return router;
}
