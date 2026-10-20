package FlinkEcommerce.dtos;

import FlinkEcommerce.enums.CdcOperation;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class ProductCdcEvent {
  private CdcOperation op;
  private Product before;
  private Product after;

  @JsonProperty("ts_ms")
  private Long tsMs;
}
