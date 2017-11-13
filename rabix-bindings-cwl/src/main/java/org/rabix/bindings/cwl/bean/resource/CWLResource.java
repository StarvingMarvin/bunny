package org.rabix.bindings.cwl.bean.resource;

import com.fasterxml.jackson.annotation.*;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonSubTypes.Type;
import org.rabix.bindings.cwl.bean.resource.requirement.*;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "class", defaultImpl = CWLResource.class, visible = true)
@JsonSubTypes({ @Type(value = CWLDockerResource.class, name = "DockerRequirement"),
    @Type(value = CWLInlineJavascriptRequirement.class, name = "InlineJavascriptRequirement"),
    @Type(value = CWLShellCommandRequirement.class, name = "ShellCommandRequirement"),
    @Type(value = CWLResourceRequirement.class, name = "ResourceRequirement"),
    @Type(value = CWLSchemaDefRequirement.class, name = "SchemaDefRequirement"),
    @Type(value = CWLCreateFileRequirement.class, name = "CreateFileRequirement"),
    @Type(value = CWLInitialWorkDirRequirement.class, name = "InitialWorkDirRequirement"),
    @Type(value = CWLEnvVarRequirement.class, name = "EnvVarRequirement") })
@JsonInclude(Include.NON_NULL)
public class CWLResource implements Serializable {

  @JsonProperty("class")
  protected String type;
  protected Map<String, Object> raw = new HashMap<>();

  public CWLResource() {
  }

  @SuppressWarnings("unchecked")
  @JsonIgnore
  public <T> T getValue(String key) {
    if (raw == null) {
      return null;
    }

    Object value = raw.get(key);
    return value != null ? (T) value : null;
  }

  @JsonAnySetter
  public void add(String key, Object value) {
    raw.put(key, value);
  }

  @JsonAnyGetter
  public Map<String, Object> getRaw() {
    return raw;
  }

  @JsonIgnore
  public CWLResourceType getTypeEnum() {
    return CWLResourceType.OTHER;
  }

  @JsonTypeId
  public String getType() {
    return type;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((raw == null) ? 0 : raw.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    CWLResource other = (CWLResource) obj;
    if (raw == null) {
      if (other.raw != null)
        return false;
    } else if (!raw.equals(other.raw))
      return false;
    return true;
  }

  @Override
  public String toString() {
    return "CWLResource [type=" + type + ", raw=" + raw + "]";
  }

}
