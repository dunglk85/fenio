package filter

import org.apache.spark.sql.functions.col

/**
 * Created by linhnm on June 2023
 */

object CutomFilter {
  val idFilter: Boolean = col("customer_id") % 3 == 0
}
