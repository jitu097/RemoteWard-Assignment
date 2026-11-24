import { Router } from 'express';
import { createUploadController } from '../controllers/uploads.controller.js';
import { createJobsController } from '../controllers/jobs.controller.js';

export function createRoutes(deps) {
  const router = Router();
  const jobsController = createJobsController(deps);

  router.post('/uploads', createUploadController(deps));
  router.get('/jobs/:jobId', jobsController.getJob);
  router.get('/jobs/:jobId/result', jobsController.getResult);
  return router;
}
