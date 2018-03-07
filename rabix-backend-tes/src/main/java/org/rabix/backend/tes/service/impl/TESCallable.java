package org.rabix.backend.tes.service.impl;


import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.rabix.backend.model.RemoteTask;
import org.rabix.backend.service.RemoteServiceException;
import org.rabix.backend.service.RemoteStorageService;
import org.rabix.backend.service.TaskRunCallable;
import org.rabix.backend.tes.client.TESHTTPClientException;
import org.rabix.backend.tes.client.TESHttpClient;
import org.rabix.backend.tes.model.TESCreateTaskResponse;
import org.rabix.backend.tes.model.TESExecutor;
import org.rabix.backend.tes.model.TESFileType;
import org.rabix.backend.tes.model.TESGetTaskRequest;
import org.rabix.backend.tes.model.TESInput;
import org.rabix.backend.tes.model.TESOutput;
import org.rabix.backend.tes.model.TESResources;
import org.rabix.backend.tes.model.TESTask;
import org.rabix.backend.tes.model.TESView;
import org.rabix.bindings.CommandLine;
import org.rabix.bindings.model.FileValue;
import org.rabix.bindings.model.FileValue.FileType;
import org.rabix.bindings.model.Job;
import org.rabix.bindings.model.requirement.DockerContainerRequirement;
import org.rabix.bindings.model.requirement.EnvironmentVariableRequirement;
import org.rabix.bindings.model.requirement.Requirement;
import org.rabix.bindings.model.requirement.ResourceRequirement;

public class TESCallable extends TaskRunCallable {

  private TESHttpClient tesHttpClient;
  private TESCreateTaskResponse tesJobId;

  public TESCallable(Job job, RemoteStorageService storage, TESHttpClient tesHttpClient) {
    super(job, storage);
    this.tesHttpClient = tesHttpClient;
  }

  @Override
  public void start(Iterable<FileValue> files, DockerContainerRequirement docker, CommandLine commandLine, List<Requirement> combinedRequirements) {
    Set<TESInput> inputs = new HashSet<>();


    files.forEach(fileValue -> {
      try {
        storage.stageFile(workDir, fileValue);
        inputs.add(new TESInput(fileValue.getName(), null, fileValue.getLocation(), fileValue.getPath(),
            fileValue.getType().equals(FileType.File) ? TESFileType.FILE : TESFileType.DIRECTORY, null));
      } catch (TESStorageException e) {
        e.printStackTrace();
      }
    });

    List<TESOutput> outputs = Collections.singletonList(
        new TESOutput(localDir.getFileName().toString(), null, workDir.toUri().toString(), localDir.toString(), TESFileType.DIRECTORY));

    String commandLineToolStdout = commandLine.getStandardOut();
    if (commandLineToolStdout != null && !commandLineToolStdout.startsWith("/")) {
      commandLineToolStdout = localDir.resolve(commandLineToolStdout).toString();
    }

    String commandLineToolErrLog = commandLine.getStandardError();
    if (commandLineToolErrLog == null) {
      commandLineToolErrLog = DEFAULT_COMMAND_LINE_TOOL_ERR_LOG;
    }
    commandLineToolErrLog = localDir.resolve(commandLineToolErrLog).toString();

    List<TESExecutor> command = Collections.singletonList(new TESExecutor(
            getImageId(docker), buildCommandLine(commandLine), localDir.toString(),
            commandLine.getStandardIn(), commandLineToolStdout, commandLineToolErrLog, getEnvVariables(combinedRequirements)
    ));

    TESResources resources = getResources(combinedRequirements);
    Map<String, String> tags = getTags(job);
    TESTask task = new TESTask(job.getName(), null, new ArrayList<>(inputs), outputs, resources, command, null, tags, null);

    try {
      tesJobId = tesHttpClient.runTask(task);
    } catch (TESHTTPClientException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

  private Map<String, String> getTags(Job job) {
    Map<String, String> tags = new HashMap<>();
    tags.put("workflow_id", job.getRootId().toString());
    tags.put("job_id", job.getId().toString());
    tags.put("tool_name", job.getName());
    return tags;
  }

  private Map<String, String> getEnvVariables(List<Requirement> combinedRequirements) {
    EnvironmentVariableRequirement envs = getRequirement(combinedRequirements, EnvironmentVariableRequirement.class);
    Map<String, String> variables = new HashMap<>();
    if (envs != null) {
      variables = envs.getVariables();
    }
    variables.put("HOME", localDir.toString());
    variables.put("TMPDIR", localDir.toString());
    return variables;
  }

  private TESResources getResources(List<Requirement> combinedRequirements) {
    Integer cpus = null;
    Double disk = null;
    Double ram = null;
    ResourceRequirement jobResourceRequirement = getRequirement(combinedRequirements, ResourceRequirement.class);
    if (jobResourceRequirement != null) {
      cpus = (jobResourceRequirement.getCpuMin() != null) ? jobResourceRequirement.getCpuMin().intValue() : null;
      disk = (jobResourceRequirement.getDiskSpaceMinMB() != null) ? jobResourceRequirement.getDiskSpaceMinMB().doubleValue() / 1000.0 : null;
      ram = (jobResourceRequirement.getMemMinMB() != null) ? jobResourceRequirement.getMemMinMB().doubleValue() / 1000.0 : null;
    }
    TESResources resources = new TESResources(cpus, false, ram, disk, null);
    return resources;
  }

  @Override
  public RemoteTask check() throws RemoteServiceException {
    try {
      return tesHttpClient.getTask(new TESGetTaskRequest(tesJobId.getId(), TESView.FULL));
    } catch (TESHTTPClientException e) {
      throw new RemoteServiceException(e);
    }
  }

}
