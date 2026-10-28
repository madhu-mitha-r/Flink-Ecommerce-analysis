package FlinkEcommerce.dtos;

import FlinkEcommerce.enums.JoinAnomalyType;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class OrderPaymentJoinAnomaly {
  private JoinAnomalyType joinAnomalyType;
  private Order order;
  private Payment payment;
  private String orderId;

  public static OrderPaymentJoinAnomaly of(
      JoinAnomalyType type, String orderId, Order order, Payment payment) {
    OrderPaymentJoinAnomaly anomaly = new OrderPaymentJoinAnomaly();
    anomaly.joinAnomalyType = type;
    anomaly.orderId = orderId;
    anomaly.order = order;
    anomaly.payment = payment;
    return anomaly;
  }
}
