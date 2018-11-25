package kioto;

import java.util.Date;

public final class HealthCheck {
  private String event;
  private String factory;
  private String serialNumber;
  private String type;
  private String status;
  private Date lastStartedAt;
  private float temperature;
  private String ipAddress;

  public HealthCheck() {
  }

  public HealthCheck(String event,
                     String factory,
                     String serialNumber,
                     String type,
                     String status,
                     Date lastStartedAt,
                     float temperature,
                     String ipAddress) {
    this.event = event;
    this.factory = factory;
    this.serialNumber = serialNumber;
    this.type = type;
    this.status = status;
    this.lastStartedAt = lastStartedAt;
    this.temperature = temperature;
    this.ipAddress = ipAddress;
  }

  public String getEvent() {
    return event;
  }

  public void setEvent(String event) {
    this.event = event;
  }

  public String getFactory() {
    return factory;
  }

  public void setFactory(String factory) {
    this.factory = factory;
  }

  public String getSerialNumber() {
    return serialNumber;
  }

  public void setSerialNumber(String serialNumber) {
    this.serialNumber = serialNumber;
  }

  public String getType() {
    return type;
  }

  public void setType(String type) {
    this.type = type;
  }

  public String getStatus() {
    return status;
  }

  public void setStatus(String status) {
    this.status = status;
  }

  public Date getLastStartedAt() {
    return lastStartedAt;
  }

  public void setLastStartedAt(Date lastStartedAt) {
    this.lastStartedAt = lastStartedAt;
  }

  public float getTemperature() {
    return temperature;
  }

  public void setTemperature(float temperature) {
    this.temperature = temperature;
  }

  public String getIpAddress() {
    return ipAddress;
  }

  public void setIpAddress(String ipAddress) {
    this.ipAddress = ipAddress;
  }
}