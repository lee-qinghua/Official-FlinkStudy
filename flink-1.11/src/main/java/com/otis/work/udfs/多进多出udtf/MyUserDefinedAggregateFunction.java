//                                  package com.otis.company.udfs.多进多出udtf;
//
//
//                                  import org.apache.flink.api.common.typeinfo.TypeInformation;
//                                  import org.apache.flink.table.functions.UserDefinedFunction;
//
//                                  /**
//                                   * Base class for user-defined aggregates and table aggregates.
//                                   *
//                                   * @param <T>   the type of the aggregation result.
//                                   * @param <ACC> the type of the aggregation accumulator. The accumulator is used to keep the
//                                   *             aggregated values which are needed to compute an aggregation result.
//                                   */
//                                  public abstract class MyUserDefinedAggregateFunction<T, ACC> extends UserDefinedFunction {
//
//                                      /**
//                                       * Creates and init the Accumulator for this (table)aggregate function.
//                                       *
//                                       * @return the accumulator with the initial value
//                                       */
//                                      public ACC createAccumulator(); // MANDATORY 必须的
//
//                                      /**
//                                       * Returns the TypeInformation of the (table)aggregate function's result.
//                                       *
//                                       * @return The TypeInformation of the (table)aggregate function's result or null if the result
//                                       *         type should be automatically inferred.
//                                       */
//                                      public TypeInformation<T> getResultType = null; // PRE-DEFINED
//
//                                      /**
//                                       * Returns the TypeInformation of the (table)aggregate function's accumulator.
//                                       *
//                                       * @return The TypeInformation of the (table)aggregate function's accumulator or null if the
//                                       *         accumulator type should be automatically inferred.
//                                       */
//                                      public TypeInformation<ACC> getAccumulatorType = null; // PRE-DEFINED
//                                  }
//
//                                  /**
//                                   * Base class for table aggregation functions.
//                                   *
//                                   * @param <T>   the type of the aggregation result
//                                   * @param <ACC> the type of the aggregation accumulator. The accumulator is used to keep the
//                                   *             aggregated values which are needed to compute a table aggregation result.
//                                   *             TableAggregateFunction represents its state using accumulator, thereby the state of
//                                   *             the TableAggregateFunction must be put into the accumulator.
//                                   */
//                                  public abstract class TableAggregateFunction<T, ACC> extends UserDefinedAggregateFunction<T, ACC> {
//
//                                      /** Processes the input values and update the provided accumulator instance. The method
//                                       * accumulate can be overloaded with different custom types and arguments. A TableAggregateFunction
//                                       * requires at least one accumulate() method.
//                                       *
//                                       * @param accumulator           the accumulator which contains the current aggregated results
//                                       * @param [user defined inputs] the input value (usually obtained from a new arrived data).
//                                       */
//                                      public void accumulate(ACC accumulator, [user defined inputs]); // MANDATORY
//
//                                      /**
//                                       * Retracts the input values from the accumulator instance. The current design assumes the
//                                       * inputs are the values that have been previously accumulated. The method retract can be
//                                       * overloaded with different custom types and arguments. This function must be implemented for
//                                       * datastream bounded over aggregate.
//                                       *
//                                       * @param accumulator           the accumulator which contains the current aggregated results
//                                       * @param [user defined inputs] the input value (usually obtained from a new arrived data).
//                                       */
//                                      public void retract(ACC accumulator, [user defined inputs]); // OPTIONAL
//
//                                      /**
//                                       * Merges a group of accumulator instances into one accumulator instance. This function must be
//                                       * implemented for datastream session window grouping aggregate and dataset grouping aggregate.
//                                       *
//                                       * @param accumulator  the accumulator which will keep the merged aggregate results. It should
//                                       *                     be noted that the accumulator may contain the previous aggregated
//                                       *                     results. Therefore user should not replace or clean this instance in the
//                                       *                     custom merge method.
//                                       * @param its          an {@link java.lang.Iterable} pointed to a group of accumulators that will be
//                                       *                     merged.
//                                       */
//                                      public void merge(ACC accumulator, java.lang.Iterable<ACC> its); // OPTIONAL
//
//                                      /**
//                                       * Called every time when an aggregation result should be materialized. The returned value
//                                       * could be either an early and incomplete result  (periodically emitted as data arrive) or
//                                       * the final result of the  aggregation.
//                                       *
//                                       * @param accumulator the accumulator which contains the current
//                                       *                    aggregated results
//                                       * @param out         the collector used to output data
//                                       */
//                                      public void emitValue(ACC accumulator, Collector<T> out); // OPTIONAL
//
//                                      /**
//                                       * Called every time when an aggregation result should be materialized. The returned value
//                                       * could be either an early and incomplete result (periodically emitted as data arrive) or
//                                       * the final result of the aggregation.
//                                       *
//                                       * Different from emitValue, emitUpdateWithRetract is used to emit values that have been updated.
//                                       * This method outputs data incrementally in retract mode, i.e., once there is an update, we
//                                       * have to retract old records before sending new updated ones. The emitUpdateWithRetract
//                                       * method will be used in preference to the emitValue method if both methods are defined in the
//                                       * table aggregate function, because the method is treated to be more efficient than emitValue
//                                       * as it can outputvalues incrementally.
//                                       *
//                                       * @param accumulator the accumulator which contains the current
//                                       *                    aggregated results
//                                       * @param out         the retractable collector used to output data. Use collect method
//                                       *                    to output(add) records and use retract method to retract(delete)
//                                       *                    records.
//                                       */
//                                      public void emitUpdateWithRetract(ACC accumulator, RetractableCollector<T> out); // OPTIONAL
//
//                                      /**
//                                       * Collects a record and forwards it. The collector can output retract messages with the retract
//                                       * method. Note: only use it in {@code emitRetractValueIncrementally}.
//                                       */
//                                      public interface RetractableCollector<T> extends Collector<T> {
//
//                                          /**
//                                           * Retract a record.
//                                           *
//                                           * @param record The record to retract.
//                                           */
//                                          void retract(T record);
//                                      }
//                                  }
//