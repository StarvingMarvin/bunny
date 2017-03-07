package org.rabix.engine.processor.handler.impl;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.rabix.bindings.BindingException;
import org.rabix.bindings.model.Job;
import org.rabix.bindings.model.Job.JobStatus;
import org.rabix.bindings.model.LinkMerge;
import org.rabix.bindings.model.dag.DAGContainer;
import org.rabix.bindings.model.dag.DAGLink;
import org.rabix.bindings.model.dag.DAGLinkPort;
import org.rabix.bindings.model.dag.DAGLinkPort.LinkPortType;
import org.rabix.bindings.model.dag.DAGNode;
import org.rabix.common.helper.InternalSchemaHelper;
import org.rabix.common.logging.DebugAppender;
import org.rabix.engine.JobHelper;
import org.rabix.engine.db.AppDB;
import org.rabix.engine.db.DAGNodeDB;
import org.rabix.engine.event.Event;
import org.rabix.engine.event.impl.ContextStatusEvent;
import org.rabix.engine.event.impl.InputUpdateEvent;
import org.rabix.engine.event.impl.JobStatusEvent;
import org.rabix.engine.event.impl.OutputUpdateEvent;
import org.rabix.engine.model.ContextRecord;
import org.rabix.engine.model.ContextRecord.ContextStatus;
import org.rabix.engine.model.JobRecord;
import org.rabix.engine.model.JobRecord.PortCounter;
import org.rabix.engine.model.LinkRecord;
import org.rabix.engine.model.VariableRecord;
import org.rabix.engine.processor.EventProcessor;
import org.rabix.engine.processor.handler.EventHandler;
import org.rabix.engine.processor.handler.EventHandlerException;
import org.rabix.engine.repository.JobRepository;
import org.rabix.engine.service.CacheService;
import org.rabix.engine.service.ContextRecordService;
import org.rabix.engine.service.JobRecordService;
import org.rabix.engine.service.JobService;
import org.rabix.engine.service.LinkRecordService;
import org.rabix.engine.service.VariableRecordService;
import org.rabix.engine.service.impl.JobRecordServiceImpl.JobState;
import org.rabix.engine.validator.JobStateValidationException;
import org.rabix.engine.validator.JobStateValidator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Inject;

public class JobStatusEventHandler implements EventHandler<JobStatusEvent> {

  private final Logger logger = LoggerFactory.getLogger(JobStatusEventHandler.class);
  
  private final DAGNodeDB dagNodeDB;
  private final AppDB appDB;
  private final ScatterHandler scatterHelper;
  private final EventProcessor eventProcessor;
  
  private final JobRecordService jobRecordService;
  private final LinkRecordService linkRecordService;
  private final VariableRecordService variableRecordService;
  private final ContextRecordService contextRecordService;
  
  private final JobRepository jobRepository;
  private final JobService jobService;
  private final CacheService cacheService;

  @Inject
  public JobStatusEventHandler(final DAGNodeDB dagNodeDB, final AppDB appDB, final JobRecordService jobRecordService,
      final LinkRecordService linkRecordService, final VariableRecordService variableRecordService,
      final ContextRecordService contextRecordService, final EventProcessor eventProcessor,
      final ScatterHandler scatterHelper, final JobRepository jobRepository, final CacheService cacheService, final JobService jobService) {
    this.dagNodeDB = dagNodeDB;
    this.scatterHelper = scatterHelper;
    this.eventProcessor = eventProcessor;
    this.jobRecordService = jobRecordService;
    this.linkRecordService = linkRecordService;
    this.contextRecordService = contextRecordService;
    this.variableRecordService = variableRecordService;
    this.appDB = appDB;
    this.jobService = jobService;
    
    this.cacheService = cacheService;
    this.jobRepository = jobRepository;
  }

  @Override
  public void handle(JobStatusEvent event) throws EventHandlerException {
    JobRecord jobRecord = jobRecordService.find(event.getJobId(), event.getContextId());
    if (jobRecord == null) {
      logger.info("Possible stale message. Job {} for root {} doesn't exist.", event.getJobId(), event.getContextId());
      return;
    }
    try {
      JobStateValidator.checkState(jobRecord, event.getState());
    } catch (JobStateValidationException e) {
      logger.warn("Cannot transition from state " + jobRecord.getState() + " to " + event.getState());
      return;
    }
    
    switch (event.getState()) {
    case READY:
      jobRecord.setState(JobState.READY);
      jobRecordService.update(jobRecord);
      
      ready(jobRecord, event);
      
      if (!jobRecord.isContainer() && !jobRecord.isScatterWrapper()) {
        Job job = null;
        try {
          job = JobHelper.createReadyJob(jobRecord, JobStatus.READY, jobRecordService, variableRecordService, linkRecordService, contextRecordService, dagNodeDB, appDB);
          if (!job.getName().equals(InternalSchemaHelper.ROOT_NAME)) {
            jobRepository.insert(job, event.getEventGroupId(), event.getProducedByNode());
          } else {
            jobRepository.update(job);
          }
        } catch (BindingException e1) {
          // FIXME: is this really safe to ignore?
          logger.info("Failed to create job", e1);
        }
      }
        else {
          Job containerJob = null;
          try {
            containerJob = JobHelper.createJob(jobRecord, JobStatus.READY, jobRecordService, variableRecordService, linkRecordService, contextRecordService,
                dagNodeDB, appDB, false);
          } catch (BindingException e) {
            throw new EventHandlerException("Failed to call onReady callback for Job " + containerJob, e);
          }
          jobService.handleJobContainerReady(containerJob);

        }
      break;
    case RUNNING:
      jobRecord.setState(JobState.RUNNING);
      jobRecordService.update(jobRecord);
      break;
    case COMPLETED:
      jobRecord.setState(JobState.READY);
      jobRecordService.update(jobRecord);
      
      if (jobRecord.isRoot()) {
        try {
          if(!jobRecord.isContainer()) {
            // if root is CommandLineTool create OutputUpdateEvents
            for (PortCounter portCounter : jobRecord.getOutputCounters()) {
              Object output = event.getResult().get(portCounter.getPort());
              eventProcessor.send(new OutputUpdateEvent(jobRecord.getRootId(), jobRecord.getId(), portCounter.getPort(), output, 1, event.getEventGroupId(), event.getProducedByNode()));
            }
          }
          eventProcessor.send(new ContextStatusEvent(event.getContextId(), ContextStatus.COMPLETED));
          Job rootJob = JobHelper.createRootJob(jobRecord, JobStatus.COMPLETED, jobRecordService, variableRecordService, linkRecordService, contextRecordService, dagNodeDB, appDB, event.getResult());
          jobService.handleJobRootCompleted(rootJob);
          deleteRecords(rootJob.getId());
          cacheService.remove(jobRecord.getRootId());
        } catch (Exception e) {
          throw new EventHandlerException("Failed to call onRootCompleted callback for Job " + jobRecord.getRootId(), e);
        }
      } else {
        try {
          Job completedJob = JobHelper.createCompletedJob(jobRecord, JobStatus.COMPLETED, jobRecordService, variableRecordService, linkRecordService, contextRecordService, dagNodeDB, appDB);
          jobService.handleJobCompleted(completedJob);
        } catch (BindingException e) {
          logger.error("Failed to create completed Job " + jobRecord.getId(), e);
        }
        for (PortCounter portCounter : jobRecord.getOutputCounters()) {
          Object output = event.getResult().get(portCounter.getPort());
          eventProcessor.addToQueue(new OutputUpdateEvent(jobRecord.getRootId(), jobRecord.getId(), portCounter.getPort(), output, 1, event.getEventGroupId(), event.getProducedByNode()));
        }
      }
      break;
    case ABORTED:
      Set<JobState> jobRecordStatuses = new HashSet<>();
      jobRecordStatuses.add(JobState.PENDING);
      jobRecordStatuses.add(JobState.READY);
      jobRecordStatuses.add(JobState.RUNNING);

      List<JobRecord> records = jobRecordService.find(jobRecord.getRootId(), jobRecordStatuses);
      for (JobRecord record : records) {
        record.setState(JobState.ABORTED);
        jobRecordService.update(record);
      }
      
      ContextRecord contextRecord = contextRecordService.find(jobRecord.getRootId());
      contextRecord.setStatus(ContextStatus.ABORTED);
      contextRecordService.update(contextRecord);
      break;
    case FAILED:
      jobRecord.setState(JobState.READY);
      jobRecordService.update(jobRecord);
      
      if (jobRecord.isRoot()) {
        try {
          Job rootJob = JobHelper.createRootJob(jobRecord, JobStatus.FAILED, jobRecordService, variableRecordService, linkRecordService, contextRecordService, dagNodeDB, appDB, null);
          jobService.handleJobRootFailed(rootJob);
          
          eventProcessor.send(new ContextStatusEvent(event.getContextId(), ContextStatus.FAILED));
          cacheService.remove(jobRecord.getRootId());
        } catch (Exception e) {
          throw new EventHandlerException("Failed to call onRootFailed callback for Job " + jobRecord.getRootId(), e);
        }
      } else {
        try {
          Job failedJob = JobHelper.createCompletedJob(jobRecord, JobStatus.FAILED, jobRecordService, variableRecordService, linkRecordService, contextRecordService, dagNodeDB, appDB);
          jobService.handleJobFailed(failedJob);
          
          eventProcessor.send(new JobStatusEvent(InternalSchemaHelper.ROOT_NAME, event.getContextId(), JobState.FAILED, null, event.getEventGroupId(), event.getProducedByNode()));
        } catch (Exception e) {
          throw new EventHandlerException("Failed to call onFailed callback for Job " + jobRecord.getId(), e);
        }
      }
      break;
    default:
      break;
    }
  }
  
  private void deleteRecords(UUID rootId) {
    contextRecordService.delete(rootId);
  }
  
  /**
   * Job is ready
   */
  public void ready(JobRecord job, Event event) throws EventHandlerException {
    job.setState(JobState.READY);
    
    UUID rootId = event.getContextId();
    DAGNode node = dagNodeDB.get(InternalSchemaHelper.normalizeId(job.getId()), rootId, job.getDagHash());

    DebugAppender readyJobLogging = new DebugAppender(logger);
    readyJobLogging.append(" --- JobRecord ").append(job.getId()).append(" is ready.").append(" Job isBlocking=").append(job.isBlocking()).append("\n");
    for (PortCounter portCounter : job.getInputCounters()) {
      readyJobLogging.append(" --- Input port ").append(portCounter.getPort()).append(", isScatter=").append(portCounter.isScatter()).append(", isBlocking ").append(job.isInputPortBlocking(node, portCounter.getPort())).append("\n");
    }
    readyJobLogging.append(" --- All scatter ports ").append(job.getScatterPorts()).append("\n");
    logger.debug(readyJobLogging.toString());
    
    if (!job.isScattered() && job.getScatterPorts().size() > 0) {
      job.setState(JobState.RUNNING);
      
      for (String port : job.getScatterPorts()) {
        VariableRecord variable = variableRecordService.find(job.getId(), port, LinkPortType.INPUT, rootId);
        scatterHelper.scatterPort(job, event, port, variableRecordService.getValue(variable), 1, null, false, false);
      }
    } else if (job.isContainer()) {
      job.setState(JobState.RUNNING);

      DAGContainer containerNode;
      if (job.isScattered()) {
        containerNode = (DAGContainer) node;
      } else {
        containerNode = (DAGContainer) node;
      }
      rollOutContainer(job, containerNode, rootId);

      List<LinkRecord> containerLinks = linkRecordService.findBySourceAndSourceType(job.getId(), LinkPortType.INPUT, rootId);
      if (containerLinks.isEmpty()) {
        Set<String> immediateReadyNodeIds = findImmediateReadyNodes(containerNode);
        for (String readyNodeId : immediateReadyNodeIds) {
          JobRecord childJobRecord = jobRecordService.find(readyNodeId, rootId);
          if(childJobRecord.isContainer() || childJobRecord.isScatterWrapper()) {
            ready(childJobRecord, event);  
          }
          else {
            JobStatusEvent jobStatusEvent = new JobStatusEvent(childJobRecord.getId(), rootId, JobState.READY, null, event.getEventGroupId(), event.getProducedByNode());
            eventProcessor.send(jobStatusEvent);
          }
        }
      } else {
        for (LinkRecord link : containerLinks) {
          VariableRecord sourceVariable = variableRecordService.find(link.getSourceJobId(), link.getSourceJobPort(), LinkPortType.INPUT, rootId);
          VariableRecord destinationVariable = variableRecordService.find(link.getDestinationJobId(), link.getDestinationJobPort(), LinkPortType.INPUT, rootId);
          if(destinationVariable == null) {
            VariableRecord stepVariable = new VariableRecord(rootId, link.getDestinationJobId(), sourceVariable.getPortId(), LinkPortType.INPUT, variableRecordService.getValue(sourceVariable), null);
            variableRecordService.create(stepVariable);
          }
          Event updateEvent = new InputUpdateEvent(rootId, link.getDestinationJobId(), link.getDestinationJobPort(), variableRecordService.getValue(sourceVariable), link.getPosition(), event.getEventGroupId(), event.getProducedByNode());
          eventProcessor.send(updateEvent);
        }
      }
    }
  }
  
  private Set<String> findImmediateReadyNodes(DAGNode node) {
    if (node instanceof DAGContainer) {
      Set<String> nodesWithoutDestination = new HashSet<>();
      for (DAGNode child : ((DAGContainer) node).getChildren()) {
        nodesWithoutDestination.add(child.getId());
      }
      
      for (DAGLink link : ((DAGContainer) node).getLinks()) {
        nodesWithoutDestination.remove(link.getDestination().getDagNodeId());
      }
      return nodesWithoutDestination;
    }
    return Collections.<String>emptySet();
  }
  
  /**
   * Unwraps {@link DAGContainer}
   */
  private void rollOutContainer(JobRecord job, DAGContainer containerNode, UUID contextId) {
    for (DAGNode node : containerNode.getChildren()) {
      String newJobId = InternalSchemaHelper.concatenateIds(job.getId(), InternalSchemaHelper.getLastPart(node.getId()));
      
      JobRecord childJob = scatterHelper.createJobRecord(newJobId, job.getExternalId(), node, false, contextId, job.getDagHash());
      jobRecordService.create(childJob);

      DebugAppender childJobLogBuilder = new DebugAppender(logger);
      childJobLogBuilder.append("\n -- JobRecord ", newJobId, ", isBlocking ", childJob.isBlocking(), "\n");


      for (DAGLinkPort port : node.getInputPorts()) {
        if (port.getTransform() != null) {
          childJob.setBlocking(true);
        }
        VariableRecord childVariable = new VariableRecord(contextId, newJobId, port.getId(), LinkPortType.INPUT, port.getDefaultValue(), node.getLinkMerge(port.getId(), port.getType()));
        childJobLogBuilder.append(" -- Input port ", port.getId(), ", isScatter ", port.isScatter(), "\n");
        variableRecordService.create(childVariable);
      }

      for (DAGLinkPort port : node.getOutputPorts()) {
        childJobLogBuilder.append(" -- Output port ", port.getId(), ", isScatter ", port.isScatter(), "\n");
        VariableRecord childVariable = new VariableRecord(contextId, newJobId, port.getId(), LinkPortType.OUTPUT, null, node.getLinkMerge(port.getId(), port.getType()));
        variableRecordService.create(childVariable);
      }
      logger.debug(childJobLogBuilder.toString());

    }
    for (DAGLink link : containerNode.getLinks()) {
      String originalJobID = InternalSchemaHelper.normalizeId(job.getId());

      String sourceNodeId = originalJobID;
      String linkSourceNodeId = link.getSource().getDagNodeId();
      if (linkSourceNodeId.startsWith(originalJobID)) {
        if (linkSourceNodeId.equals(sourceNodeId)) {
          sourceNodeId = job.getId();
        } else {
          sourceNodeId = InternalSchemaHelper.concatenateIds(job.getId(), InternalSchemaHelper.getLastPart(linkSourceNodeId));
        }
      }
      String destinationNodeId = originalJobID;
      String linkDestinationNodeId = link.getDestination().getDagNodeId();
      if (linkDestinationNodeId.startsWith(originalJobID)) {
        if (linkDestinationNodeId.equals(destinationNodeId)) {
          destinationNodeId = job.getId();
        } else {
          destinationNodeId = InternalSchemaHelper.concatenateIds(job.getId(), InternalSchemaHelper.getLastPart(linkDestinationNodeId));
        }
      }
      LinkRecord childLink = new LinkRecord(contextId, sourceNodeId, link.getSource().getId(), LinkPortType.valueOf(link.getSource().getType().toString()), destinationNodeId, link.getDestination().getId(), LinkPortType.valueOf(link.getDestination().getType().toString()), link.getPosition());
      linkRecordService.create(childLink);

      handleLinkPort(jobRecordService.find(sourceNodeId, contextId), link.getSource(), true);
      handleLinkPort(jobRecordService.find(destinationNodeId, contextId), link.getDestination(), false);
    }
  }
  
  /**
   * Handle links for roll-out 
   */
  private void handleLinkPort(JobRecord job, DAGLinkPort linkPort, boolean isSource) {
    if (linkPort.getType().equals(LinkPortType.INPUT)) {
      if (job.getState().equals(JobState.PENDING)) {
        jobRecordService.incrementPortCounter(job, linkPort, LinkPortType.INPUT);
        jobRecordService.increaseInputPortIncoming(job, linkPort.getId());
        
        if (job.getInputPortIncoming(linkPort.getId()) > 1) {
          if (LinkMerge.isBlocking(linkPort.getLinkMerge())) {
            job.setBlocking(true);
          }
        }
      }
    } else {
      jobRecordService.incrementPortCounter(job, linkPort, LinkPortType.OUTPUT);
      if (isSource) {
        job.getOutputCounter(linkPort.getId()).updatedAsSource(1);
      }
      jobRecordService.increaseOutputPortIncoming(job, linkPort.getId());
    }
    jobRecordService.update(job);
  }

}
