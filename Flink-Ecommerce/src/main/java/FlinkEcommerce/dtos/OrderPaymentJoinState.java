package FlinkEcommerce.dtos;

import FlinkEcommerce.enums.OrderPaymentJoinStatus;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class OrderPaymentJoinState {
  private Order order;
  private Payment payment;
  private OrderPaymentJoinStatus status;
  private Long timerMs;
}
