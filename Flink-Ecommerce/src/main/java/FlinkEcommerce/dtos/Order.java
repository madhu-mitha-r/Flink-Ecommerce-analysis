package FlinkEcommerce.dtos;

import java.util.List;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class Order {
  private String orderId;
  private List<OrderItem> items;
  private Long eventTime;
  private double totalAmount;
  private String customerId;
  private String shippingCountry;
  private String currency;
}
