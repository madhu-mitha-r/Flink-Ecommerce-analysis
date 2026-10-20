package FlinkEcommerce.dtos;

import FlinkEcommerce.enums.PaymentMethod;
import java.util.List;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class PaidOrder {
  private String orderId;
  private List<OrderItem> items;
  private String shippingCountry;
  private Long orderedAt;
  private String transactionId;
  private double orderedAmount;
  private double paidAmount;
  private PaymentMethod paymentMethod;
  private String currency;
  private String customerId;
  private Long paidAt;

  public static PaidOrder from(Order order, Payment payment) {
    PaidOrder paidOrder = new PaidOrder();
    paidOrder.orderId = order.getOrderId();
    paidOrder.orderedAt = order.getEventTime();
    paidOrder.customerId = order.getCustomerId();
    paidOrder.items = order.getItems();
    paidOrder.shippingCountry = order.getShippingCountry();
    paidOrder.orderedAmount = order.getTotalAmount();

    paidOrder.transactionId = payment.getTransactionId();
    paidOrder.paidAmount = payment.getAmount();
    paidOrder.currency = payment.getCurrency();
    paidOrder.paidAt = payment.getEventTime();
    paidOrder.paymentMethod = payment.getMethod();

    return paidOrder;
  }
}
