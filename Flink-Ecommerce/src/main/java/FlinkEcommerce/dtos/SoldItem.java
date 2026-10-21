package FlinkEcommerce.dtos;

import java.math.BigDecimal;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class SoldItem {

  private String customerId;
  private Long paidAt;
  private String orderId;
  private int quantity;
  private double unitPrice;

  private String productId;
  private String productName;
  private double productPrice;
  private String productCategory;
  private String productBrand;

  private BigDecimal totalPrice = BigDecimal.ZERO;
}
