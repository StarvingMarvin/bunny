package org.rabix.backend.service;

import java.nio.file.Path;
import java.util.List;

import org.rabix.backend.tes.service.impl.TESStorageException;
import org.rabix.bindings.BindingException;
import org.rabix.bindings.model.FileValue;
import org.rabix.bindings.model.Job;

public interface RemoteStorageService {

  Job transformInputFiles(Job job) throws BindingException;
   
  Path workDir(Job job);

  Path localDir(Job job);

  Path storageBase();
  
  List<FileValue> stageFile(Path workDir, FileValue fileValue) throws TESStorageException;

}
