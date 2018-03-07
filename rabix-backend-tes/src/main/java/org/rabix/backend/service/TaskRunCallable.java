package org.rabix.backend.service;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

import org.apache.commons.lang3.StringUtils;
import org.rabix.backend.model.RemoteTask;
import org.rabix.backend.tes.service.impl.TESStorageException;
import org.rabix.bindings.BindingException;
import org.rabix.bindings.Bindings;
import org.rabix.bindings.BindingsFactory;
import org.rabix.bindings.CommandLine;
import org.rabix.bindings.model.DirectoryValue;
import org.rabix.bindings.model.FileValue;
import org.rabix.bindings.model.Job;
import org.rabix.bindings.model.requirement.DockerContainerRequirement;
import org.rabix.bindings.model.requirement.EnvironmentVariableRequirement;
import org.rabix.bindings.model.requirement.FileRequirement;
import org.rabix.bindings.model.requirement.FileRequirement.SingleFileRequirement;
import org.rabix.bindings.model.requirement.FileRequirement.SingleInputFileRequirement;
import org.rabix.bindings.model.requirement.FileRequirement.SingleTextFileRequirement;
import org.rabix.bindings.model.requirement.Requirement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class TaskRunCallable implements Callable<WorkPair> {

  protected Job job;
  protected Path workDir;
  protected Path localDir;
  protected RemoteStorageService storage;

  private final static Logger logger = LoggerFactory.getLogger(TaskRunCallable.class);
  public final static String DEFAULT_COMMAND_LINE_TOOL_ERR_LOG = "job.err.log";


  public TaskRunCallable(Job job, RemoteStorageService storage) {
    this.job = job;
    this.storage = storage;
    workDir = storage.workDir(job);
    localDir = storage.localDir(job);
  }

  public abstract void start(Iterable<FileValue> files, DockerContainerRequirement docker, CommandLine commandLine, List<Requirement> combinedRequirements);

  public abstract RemoteTask check() throws RemoteServiceException;

  @Override
  public WorkPair call() throws Exception {
    try {
      Bindings bindings = BindingsFactory.create(job);
      job = bindings.preprocess(job, localDir, (String path, Map<String, Object> config) -> path);
      DockerContainerRequirement dockerContainerRequirement = getRequirement(getRequirements(bindings), DockerContainerRequirement.class);
      if (dockerContainerRequirement != null && dockerContainerRequirement.getDockerOutputDirectory() != null) {
        localDir = Paths.get(dockerContainerRequirement.getDockerOutputDirectory());
        job = bindings.preprocess(job, localDir, (String path, Map<String, Object> config) -> path);
      }

      if (bindings.isSelfExecutable(job)) {
        return new WorkPair(job);
      }


      Map<String, Object> wfInputs = job.getInputs();
      Collection<FileValue> flat = flatten(wfInputs);
      List<Requirement> combinedRequirements = getRequirements(bindings);
      stageFileRequirements(combinedRequirements, workDir, flat);

      flat.forEach(fileValue -> {
        try {
          storage.stageFile(workDir, fileValue);
        } catch (TESStorageException e) {
          e.printStackTrace();
        }
      });
      job = Job.cloneWithInputs(job, wfInputs);

      CommandLine commandLine = buildCommandLine(bindings);

      start(flat, dockerContainerRequirement, commandLine, combinedRequirements);
      
      RemoteTask task;
      do {
        Thread.sleep(100L);
        task = check();
        if (task == null) {
          throw new RemoteServiceException("TESJob is not created. JobId = " + job.getId());
        }
      } while (!task.isFinished());
      return new WorkPair(job, task);
    } catch (BindingException e) {
      logger.error("Failed to use Bindings", e);
      throw new RemoteServiceException("Failed to use Bindings", e);
    }
  }

  protected CommandLine buildCommandLine(Bindings bindings) throws BindingException {
    return bindings.buildCommandLineObject(job, localDir.toFile(), (String path, Map<String, Object> config) -> path);
  }

  protected List<String> buildCommandLine(CommandLine commandLine) {
    List<String> mainCommand = new ArrayList<>();
    List<String> parts = commandLine.getParts();
    StringBuilder joined = new StringBuilder();

    parts.forEach(part -> {
      if ((!mainCommand.isEmpty() && mainCommand.get(mainCommand.size() - 1).equals("-c")) || joined.length() > 0) {
        joined.append(" ").append(part);
      } else {
        mainCommand.add(part);
      }
    });
    if (joined.length() > 0) {
      mainCommand.add(joined.toString().trim());
    } else {
      joined.append(StringUtils.join(mainCommand, " "));
      mainCommand.clear();
      mainCommand.add(joined.toString());
      mainCommand.add(0, "-c");
      mainCommand.add(0, "/bin/sh");
    }
    return mainCommand;
  }

  protected Map<String, String> getVariables(List<Requirement> combinedRequirements) {
    EnvironmentVariableRequirement envs = getRequirement(combinedRequirements, EnvironmentVariableRequirement.class);
    Map<String, String> variables = new HashMap<>();
    if (envs != null) {
      variables = envs.getVariables();
    }
    variables.put("HOME", localDir.toString());
    variables.put("TMPDIR", localDir.toString());
    return variables;
  }

  protected String getImageId(DockerContainerRequirement dockerContainerRequirement) {
    String imageId;
    if (dockerContainerRequirement == null) {
      imageId = "debian:stretch-slim";
    } else {
      imageId = dockerContainerRequirement.getDockerPull();
    }
    return imageId;
  }

  private void stageFileRequirements(List<Requirement> requirements, Path workDir, Collection<FileValue> old) throws TESStorageException {
    FileRequirement fileRequirementResource = getRequirement(requirements, FileRequirement.class);
    if (fileRequirementResource == null) {
      return;
    }

    List<SingleFileRequirement> fileRequirements = fileRequirementResource.getFileRequirements();
    if (fileRequirements == null) {
      return;
    }

    for (SingleFileRequirement fileRequirement : fileRequirements) {
      logger.info("Process file requirement {}", fileRequirement);
      String filename = fileRequirement.getFilename();
      Path destinationFile = workDir.resolve(filename);
      if (fileRequirement instanceof SingleTextFileRequirement) {
        try {
          byte[] bytes = ((SingleTextFileRequirement) fileRequirement).getContent().getBytes();
          Files.createDirectories(destinationFile.getParent());
          Files.write(destinationFile, bytes);
        } catch (IOException e) {
          throw new TESStorageException(e.getMessage());
        }
        old.add(new FileValue(0l, localDir.resolve(filename).toString(), destinationFile.toUri().toString(), null, Collections.emptyList(), null, null));
        continue;
      }
      if (fileRequirement instanceof SingleInputFileRequirement) {
        FileValue content = ((SingleInputFileRequirement) fileRequirement).getContent();
        for (FileValue f : old) {
          if (f.getPath().equals(content.getPath())) {
            content = f;
          }
        }
        if (!filename.equals(content.getName())) {
          recursiveSet(content, filename);
        }
        if (content.getPath() == null) {
          content.setPath(storage.localDir(job).resolve(content.getPath()).toString());
        }
        if (!old.contains(content)) {
          old.add(content);
        }
      }
    }
  }

  private List<Requirement> getRequirements(Bindings bindings) throws BindingException {
    List<Requirement> combinedRequirements = new ArrayList<>();
    combinedRequirements.addAll(bindings.getHints(job));
    combinedRequirements.addAll(bindings.getRequirements(job));
    return combinedRequirements;
  }

  private void recursiveSet(FileValue file, String a) {
    file.setName(a);
    file.setPath(storage.localDir(job).resolve(a).toString());
    file.getSecondaryFiles().forEach(f -> {
      recursiveSet(f, f.getName());
    });
  }

  private Collection<FileValue> flatten(Map<String, Object> inputs) {
    List<FileValue> flat = new ArrayList<>();
    flatten(flat, inputs);
    return flat;
  }

  @SuppressWarnings({"rawtypes"})
  private void flatten(Collection<FileValue> inputs, Object value) {
    if (value instanceof Map)
      flatten(inputs, (Map) value);
    if (value instanceof List)
      flatten(inputs, (List) value);
    if (value instanceof FileValue)
      flatten(inputs, (FileValue) value);
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private void flatten(Collection<FileValue> inputs, Map value) {
    value.values().forEach(v -> flatten(inputs, v));
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private void flatten(Collection<FileValue> inputs, List value) {
    value.forEach(v -> flatten(inputs, v));
  }

  private void flatten(Collection<FileValue> inputs, FileValue value) {
    value.getSecondaryFiles().forEach(f -> flatten(inputs, f));
    if (value instanceof DirectoryValue) {
      List<FileValue> listing = ((DirectoryValue) value).getListing();
      if (!listing.isEmpty()) {
        listing.forEach(f -> flatten(inputs, f));
      } else {
        inputs.add(value);
      }
    } else {
      inputs.add(value);
    }
  }


  @SuppressWarnings("unchecked")
  protected <T extends Requirement> T getRequirement(List<Requirement> requirements, Class<T> clazz) {
    for (Requirement requirement : requirements) {
      if (requirement.getClass().equals(clazz)) {
        return (T) requirement;
      }
    }
    return null;
  }
}
