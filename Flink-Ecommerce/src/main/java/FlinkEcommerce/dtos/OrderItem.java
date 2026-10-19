package FlinkEcommerce.dtos;

import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class OrderItem {
  private String productId;
  private double unitPrice;
  private int quantity;
}
