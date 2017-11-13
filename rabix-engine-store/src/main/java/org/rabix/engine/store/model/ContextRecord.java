package org.rabix.engine.store.model;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.io.Serializable;
import java.time.LocalDateTime;
import java.util.Map;
import java.util.UUID;

public class ContextRecord extends TimestampedModel implements Serializable {

  public static enum ContextStatus {
    RUNNING,
    COMPLETED,
    FAILED,
    ABORTED
  }

  private UUID id;
  private Map<String, Object> config;
  private ContextStatus status;

  public ContextRecord(final UUID id, Map<String, Object> config, ContextStatus status) {
    this(id, config, status, LocalDateTime.now(), LocalDateTime.now());
  }

  public ContextRecord(final UUID id, Map<String, Object> config, ContextStatus status, LocalDateTime createdAt, LocalDateTime modifiedAt) {
    super(createdAt, modifiedAt);
    this.id = id;
    this.config = config;
    this.status = status;
  }

  public UUID getId() {
    return id;
  }

  public void setId(UUID id) {
    this.id = id;
  }

  public Map<String, Object> getConfig() {
    return config;
  }

  public void setConfig(Map<String, Object> config) {
    this.config = config;
  }

  public ContextStatus getStatus() {
    return status;
  }

  public void setStatus(ContextStatus status) {
    this.status = status;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }

    if (!(obj instanceof ContextRecord)) {
      return false;
    }

    if (obj == this) {
      return true;
    }

    ContextRecord other = (ContextRecord) obj;
    return new EqualsBuilder()
            .append(id, other.id)
            .append(config, other.config)
            .append(status, other.status)
            .isEquals();
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder()
            .append(id)
            .append(config)
            .append(status)
            .hashCode();
  }

  @Override
  public String toString() {
    return "ContextRecord [id=" + id + ", config=" + config + ", status=" + status + "]";
  }

}
