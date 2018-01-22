package org.rabix.backend.tes.service.impl;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang.NotImplementedException;
import org.rabix.backend.api.WorkerService;
import org.rabix.backend.api.callback.WorkerStatusCallback;
import org.rabix.backend.api.callback.WorkerStatusCallbackException;
import org.rabix.backend.api.engine.EngineStub;
import org.rabix.backend.api.engine.EngineStubLocal;
import org.rabix.backend.model.RemoteTask;
import org.rabix.backend.service.RemoteServiceException;
import org.rabix.backend.service.RemoteStorageService;
import org.rabix.bindings.Bindings;
import org.rabix.bindings.BindingsFactory;
import org.rabix.bindings.model.Job;
import org.rabix.bindings.model.Job.JobStatus;
import org.rabix.common.helper.ChecksumHelper.HashAlgorithm;
import org.rabix.common.logging.VerboseLogger;
import org.rabix.transport.backend.Backend;
import org.rabix.transport.backend.impl.BackendLocal;
import org.rabix.transport.mechanism.TransportPluginException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.BindingAnnotation;
import com.google.inject.Inject;

public abstract class LocalWorkerServiceImpl implements WorkerService {

  private final static Logger logger = LoggerFactory.getLogger(LocalWorkerServiceImpl.class);

  @BindingAnnotation
  @Target({java.lang.annotation.ElementType.FIELD, java.lang.annotation.ElementType.PARAMETER, java.lang.annotation.ElementType.METHOD})
  @Retention(java.lang.annotation.RetentionPolicy.RUNTIME)
  public static @interface RemoteWorker {
  }

  @Inject
  private RemoteStorageService storage;

  private Set<Future<WorkPair>> pendingResults = Collections.newSetFromMap(new ConcurrentHashMap<Future<WorkPair>, Boolean>());

  private ScheduledExecutorService scheduledTaskChecker = Executors.newScheduledThreadPool(1);
  private java.util.concurrent.ExecutorService taskPoolExecutor = Executors.newFixedThreadPool(10);

  private EngineStub<?, ?, ?> engineStub;

  @Inject
  private Configuration configuration;
  @Inject
  private WorkerStatusCallback statusCallback;

  @Inject
  private TaskCallableFactory factory;


  private void success(Job job, RemoteTask tesJob) {
    job = Job.cloneWithStatus(job, JobStatus.COMPLETED);
    try {
      Bindings bindings = BindingsFactory.create(job);
      Path dir = storage.localDir(job);
      if (!bindings.isSelfExecutable(job)) {
        URI uri = tesJob.getOutputLocation();
        Path outDir = Paths.get(uri);
        dir = outDir;
      }
      job = bindings.postprocess(job, dir, HashAlgorithm.SHA1, (String path, Map<String, Object> config) -> path);
    } catch (Exception e) {
      logger.error("Couldn't process job", e);
      job = Job.cloneWithStatus(job, JobStatus.FAILED);
    }
    try {
      job = statusCallback.onJobCompleted(job);
    } catch (WorkerStatusCallbackException e1) {
      logger.warn("Failed to execute statusCallback: {}", e1);
    }
    engineStub.send(job);
  }


  void fail(Job job, RemoteTask task) {
    job = Job.cloneWithStatus(job, JobStatus.FAILED);
    job = Job.cloneWithMessage(job, task.getError());
    try {
      job = statusCallback.onJobFailed(job);
    } catch (WorkerStatusCallbackException e) {
      logger.warn("Failed to execute statusCallback: {}", e);
    }
    engineStub.send(job);
  }

  @Override
  public void start(Backend backend) {
    try {
      switch (backend.getType()) {
        case LOCAL:
          engineStub = new EngineStubLocal((BackendLocal) backend, this, configuration);
          break;
        default:
          throw new TransportPluginException("Backend " + backend.getType() + " is not supported.");
      }
      engineStub.start();
    } catch (TransportPluginException e) {
      logger.error("Failed to initialize Executor", e);
      throw new RuntimeException("Failed to initialize Executor", e);
    }

    this.scheduledTaskChecker.scheduleAtFixedRate(new Runnable() {
      @Override
      public void run() {
        for (Iterator<Future<WorkPair>> iterator = pendingResults.iterator(); iterator.hasNext();) {
          Future<WorkPair> pending = (Future<WorkPair>) iterator.next();
          if (pending.isDone()) {
            try {
              WorkPair pair = pending.get();
              if (pair.task.isSuccess()) {
                success(pair.job, pair.task);
              } else {
                fail(pair.job, pair.task);
              }
              iterator.remove();
            } catch (InterruptedException | ExecutionException e) {
              logger.error("Failed to retrieve TESTask", e);
              handleException(e);
              iterator.remove();
            }
          }
        }
      }

      /**
       * Basic exception handling
       */
      private void handleException(Exception e) {
        Throwable cause = e.getCause();
        if (cause != null) {
          if (cause.getClass().equals(RemoteServiceException.class)) {
            VerboseLogger.log("Failed to communicate with service");
            System.exit(-10);
          }
        }
      }
    }, 0, 300, TimeUnit.MILLISECONDS);
  }

  public void submit(Job job, UUID contextId) {
    pendingResults.add(taskPoolExecutor.submit(factory.get(job)));
  }

  @Override
  public void cancel(List<UUID> ids, UUID contextId) {
    throw new NotImplementedException("This method is not implemented");
  }

  @Override
  public void freeResources(UUID rootId, Map<String, Object> config) {
    throw new NotImplementedException("This method is not implemented");
  }

  @Override
  public void shutdown(Boolean stopEverything) {
    throw new NotImplementedException("This method is not implemented");
  }

  @Override
  public boolean isRunning(UUID id, UUID contextId) {
    throw new NotImplementedException("This method is not implemented");
  }

  @Override
  public Map<String, Object> getResult(UUID id, UUID contextId) {
    throw new NotImplementedException("This method is not implemented");
  }

  @Override
  public boolean isStopped() {
    throw new NotImplementedException("This method is not implemented");
  }

  @Override
  public JobStatus findStatus(UUID id, UUID contextId) {
    throw new NotImplementedException("This method is not implemented");
  }
}
