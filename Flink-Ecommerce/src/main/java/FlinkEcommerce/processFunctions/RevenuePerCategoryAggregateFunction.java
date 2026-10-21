package FlinkEcommerce.processFunctions;

import FlinkEcommerce.dtos.RevenuePerCategory;
import FlinkEcommerce.dtos.RevenuePerCategoryAccumulator;
import FlinkEcommerce.dtos.SoldItem;
import java.math.BigDecimal;
import org.apache.flink.api.common.functions.AggregateFunction;

public class RevenuePerCategoryAggregateFunction
    implements AggregateFunction<SoldItem, RevenuePerCategoryAccumulator, RevenuePerCategory> {

  @Override
  public RevenuePerCategoryAccumulator createAccumulator() {
    return new RevenuePerCategoryAccumulator();
  }

  @Override
  public RevenuePerCategoryAccumulator add(
      SoldItem soldItem, RevenuePerCategoryAccumulator accumulator) {
    accumulator.setProductCategory(soldItem.getProductCategory());
    accumulator.setRevenue(
        accumulator.getRevenue().add(soldItem.getTotalPrice()));
    accumulator.setUnitsSold(accumulator.getUnitsSold() + soldItem.getQuantity());
    return accumulator;
  }

  @Override
  public RevenuePerCategory getResult(RevenuePerCategoryAccumulator accumulator) {
    RevenuePerCategory revenuePerCategory = new RevenuePerCategory();

    revenuePerCategory.setProductCategory(accumulator.getProductCategory());
    revenuePerCategory.setUnitsSold(accumulator.getUnitsSold());
    revenuePerCategory.setTotalRevenue(accumulator.getRevenue());

    return revenuePerCategory;
  }

  @Override
  public RevenuePerCategoryAccumulator merge(
      RevenuePerCategoryAccumulator a, RevenuePerCategoryAccumulator b) {
    a.setRevenue(a.getRevenue().add(b.getRevenue()));
    a.setUnitsSold(a.getUnitsSold() + b.getUnitsSold());
    // merge not called for tumbling windows
    return a;
  }
}
