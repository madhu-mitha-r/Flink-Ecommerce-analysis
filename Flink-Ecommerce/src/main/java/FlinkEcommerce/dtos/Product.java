package FlinkEcommerce.dtos;

import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class Product {
  private String productId;
  private String productName;
  private double unitPrice;
  private String category;
  private String brand;
}
