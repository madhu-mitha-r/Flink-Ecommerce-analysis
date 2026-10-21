package FlinkEcommerce.processFunctions;

import FlinkEcommerce.dtos.RevenuePerCategory;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class RevenuePerCategoryWindowProcessFunction
    extends ProcessWindowFunction<RevenuePerCategory, RevenuePerCategory, String, TimeWindow> {

  @Override
  public void process(
      String key,
      ProcessWindowFunction<RevenuePerCategory, RevenuePerCategory, String, TimeWindow>.Context
          context,
      Iterable<RevenuePerCategory> elements,
      Collector<RevenuePerCategory> out)
      throws Exception {

    RevenuePerCategory revenuePerCategory = elements.iterator().next();

    revenuePerCategory.setWindowEndTime(context.window().getEnd());
    revenuePerCategory.setWindowStartTime(context.window().getStart());

    out.collect(revenuePerCategory);
  }
}
