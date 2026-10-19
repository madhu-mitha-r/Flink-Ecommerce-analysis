package FlinkEcommerce.dtos;

import FlinkEcommerce.enums.PaymentMethod;
import FlinkEcommerce.enums.PaymentStatus;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class Payment {

  private String orderId;
  private String transactionId;
  private Long eventTime;
  private PaymentMethod method;
  private PaymentStatus status;
  private double amount;
  private String currency;
}
