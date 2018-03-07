package org.rabix.backend.google.service;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.rabix.backend.model.RemoteTask;
import org.rabix.backend.service.RemoteServiceException;
import org.rabix.backend.service.RemoteStorageService;
import org.rabix.backend.service.TaskRunCallable;
import org.rabix.bindings.BindingException;
import org.rabix.bindings.Bindings;
import org.rabix.bindings.CommandLine;
import org.rabix.bindings.model.FileValue;
import org.rabix.bindings.model.Job;
import org.rabix.bindings.model.requirement.DockerContainerRequirement;
import org.rabix.bindings.model.requirement.Requirement;

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.genomics.Genomics;
import com.google.api.services.genomics.model.Disk;
import com.google.api.services.genomics.model.DockerExecutor;
import com.google.api.services.genomics.model.LocalCopy;
import com.google.api.services.genomics.model.LoggingOptions;
import com.google.api.services.genomics.model.Operation;
import com.google.api.services.genomics.model.Pipeline;
import com.google.api.services.genomics.model.PipelineParameter;
import com.google.api.services.genomics.model.PipelineResources;
import com.google.api.services.genomics.model.RunPipelineArgs;
import com.google.api.services.genomics.model.RunPipelineRequest;

public class GoogleCallable extends TaskRunCallable {

  private static final String DISK = "disk";
  private static final String OUTPUT = "output";
  private String operation;
  private Path outputLocation;
  private Path logLocation;
  private String projectId;

  public GoogleCallable(Job job, RemoteStorageService storage, String projectId) {
    super(job, storage);
    this.outputLocation = storage.storageBase().resolve("outputdir");
    this.logLocation = storage.storageBase().resolve("log");
    this.projectId = projectId;
  }

  @Override
  public void start(Iterable<FileValue> files, DockerContainerRequirement docker, CommandLine commandLine, List<Requirement> combinedRequirements) {
    
    Pipeline createRequest = new Pipeline();
    createRequest.setName("test");
    List<PipelineParameter> inputs = new ArrayList<>();
    Map<String, String> urls = new HashMap<>();
    files.forEach(f -> {
      PipelineParameter pp = new PipelineParameter();
      pp.setName(f.getName() == null ? f.getPath().substring(f.getPath().lastIndexOf('/') + 1) : f.getName());
      LocalCopy lc = new LocalCopy();
      lc.setDisk(DISK);
      lc.setPath(f.getPath().substring(1));
      pp.setLocalCopy(lc);
      inputs.add(pp);
      urls.put(pp.getName(), f.getLocation());
    });
    createRequest.setInputParameters(inputs);
    createRequest.setProjectId(projectId);

    List<PipelineParameter> outputs = new ArrayList<>();
    PipelineParameter output = new PipelineParameter();
    output.setName(OUTPUT);
    LocalCopy lc = new LocalCopy();
    lc.setDisk(DISK);
    String localDir = storage.localDir(job).toString().substring(1);
    lc.setPath(localDir + "/*");
    output.setLocalCopy(lc);
    outputs.add(output);
    createRequest.setOutputParameters(outputs);

    PipelineResources pipelineResources = new PipelineResources();
    pipelineResources.setMinimumCpuCores(1);
    pipelineResources.setMinimumRamGb(1d);
    Disk d = new Disk();
    d.setName(DISK);
    d.setMountPoint("/mnt/disk");
    d.setSizeGb(1);
    pipelineResources.setDisks(Collections.singletonList(d));
    createRequest.setResources(pipelineResources);

    DockerExecutor dockerExecutor = new DockerExecutor();
    dockerExecutor.setImageName(docker == null ? "ubuntu" : docker.getDockerPull());
    dockerExecutor
        .setCmd("mkdir -p /mnt/disk/" + localDir + "; echo test > /mnt/disk/" + localDir + "/tester ; cd /mnt/disk/" + localDir + " ; " + commandLine.build());
    createRequest.setDocker(dockerExecutor);

    Pipeline response = null;
    try {

      Genomics genomicsService = createGenomicsService();
      Genomics.Pipelines.Create request = genomicsService.pipelines().create(createRequest);
      response = request.execute();
      RunPipelineRequest runRequest = new RunPipelineRequest();
      runRequest.setPipelineId(response.getPipelineId());
      RunPipelineArgs runPipelineArgs = new RunPipelineArgs();
      runPipelineArgs.setInputs(urls);
      runPipelineArgs.setOutputs(Collections.singletonMap(OUTPUT, outputLocation.toUri().toString() + "_" + job.getId().toString() + "/"));
      runPipelineArgs.setKeepVmAliveOnFailureDuration("70000s");
      LoggingOptions loggingOptions = new LoggingOptions();
      loggingOptions.setGcsPath(logLocation.toUri().toString());
      runPipelineArgs.setLogging(loggingOptions);
      runPipelineArgs.setProjectId(projectId);
      runRequest.setPipelineArgs(runPipelineArgs);
      Operation execute = genomicsService.pipelines().run(runRequest).execute();
      this.operation = execute.getName();
    } catch (IOException | GeneralSecurityException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

  @Override
  protected CommandLine buildCommandLine(Bindings bindings) throws BindingException {
    return bindings.buildCommandLineObject(job, localDir.toFile(), (String path, Map<String, Object> config) -> "/mnt/disk" + path);
  }

  public static Genomics createGenomicsService() throws IOException, GeneralSecurityException {
    HttpTransport httpTransport = GoogleNetHttpTransport.newTrustedTransport();
    JsonFactory jsonFactory = JacksonFactory.getDefaultInstance();

    GoogleCredential credential = GoogleCredential.getApplicationDefault();
    if (credential.createScopedRequired()) {
      credential = credential.createScoped(Arrays.asList("https://www.googleapis.com/auth/cloud-platform"));
    }

    return new Genomics.Builder(httpTransport, jsonFactory, credential).setApplicationName("Google-GenomicsSample/0.1").build();
  }

  @Override
  public RemoteTask check() throws RemoteServiceException {
    Genomics genomicsService;
    try {
      genomicsService = createGenomicsService();
      Genomics.Operations.Get request = genomicsService.operations().get(operation);

      Operation response = request.execute();
      return new RemoteTask() {

        @Override
        public boolean isFinished() {
          return response.getDone();
        }

        @Override
        public boolean isSuccess() {
          return response.getError() == null;
        }

        @Override
        public URI getOutputLocation() {
          return URI.create(outputLocation.toUri().toString() + "_" + job.getId().toString() + "/");
        }

        @Override
        public String getError() {
          Map info = (Map) ((Map) response.getMetadata().get("runtimeMetadata")).get("computeEngine");
          return response.getError().getMessage() + "\n\n ssh @ " + info.get("instanceName") + " --zone=" + info.get("zone");
        }
      };
    } catch (IOException | GeneralSecurityException e) {
      e.printStackTrace();
    }
    return null;
  }

}
