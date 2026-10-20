package FlinkEcommerce.enums;

import com.fasterxml.jackson.annotation.JsonProperty;

public enum CdcOperation {
  @JsonProperty("r")
  READ,
  @JsonProperty("c")
  CREATE,
  @JsonProperty("u")
  UPDATE,
  @JsonProperty("d")
  DELETE,
}
