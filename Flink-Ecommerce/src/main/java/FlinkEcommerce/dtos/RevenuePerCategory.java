package FlinkEcommerce.dtos;

import java.math.BigDecimal;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class RevenuePerCategory {
  private Long windowStartTime;
  private Long windowEndTime;
  private String productCategory;
  private Long unitsSold;
  private BigDecimal totalRevenue = BigDecimal.ZERO;
}
