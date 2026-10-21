package FlinkEcommerce.dtos;

import java.math.BigDecimal;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class RevenuePerCategoryAccumulator {
  private String productCategory;
  private BigDecimal revenue = BigDecimal.ZERO;
  private long unitsSold;
}
