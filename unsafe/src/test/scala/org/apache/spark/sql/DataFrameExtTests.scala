package org.apache.spark.sql

import com.github.mrpowers.spark.fast.tests.{ColumnComparer, DataFrameComparer}
import org.apache.spark.sql.DataFrameExt.DataFrameMethods
import org.apache.spark.sql.types._
import utest._

object DataFrameExtTests extends TestSuite with DataFrameComparer with ColumnComparer with SparkSessionTestWrapper {

  val tests: Tests = Tests {
    "toSchemaWithNullabilityAligned" - {
      "align from nullable to non nullable from of nested schema" - {
        val schema = StructType(
          Seq(
            StructField(
              "foo",
              StructType(
                Seq(
                  StructField(
                    "bar",
                    StructType(
                      Seq(
                        StructField(
                          "yoo",
                          StringType
                        ),
                        StructField(
                          "zoo",
                          StringType
                        )
                      )
                    )
                  ),
                  StructField(
                    "bax",
                    StringType
                  ),
                  StructField(
                    "bay",
                    StringType
                  ),
                  StructField(
                    "baz",
                    StringType
                  )
                )
              )
            ),
            StructField(
              "v",
              ArrayType(
                ArrayType(StructType(Seq(StructField("v1", StringType), StructField("v2", StringType))), containsNull = false),
                containsNull = false
              )
            ),
            StructField(
              "w",
              ArrayType(StructType(Seq(StructField("x", StringType), StructField("y", StringType))))
            ),
            StructField(
              "x",
              StringType
            ),
            StructField(
              "y",
              StringType
            ),
            StructField(
              "z",
              StringType
            )
          )
        )

        val expectedSchema = StructType(
          Seq(
            StructField(
              "foo",
              StructType(
                Seq(
                  StructField(
                    "bar",
                    StructType(
                      Seq(
                        StructField(
                          "yoo",
                          StringType,
                          nullable = false
                        ),
                        StructField(
                          "zoo",
                          StringType,
                          nullable = false
                        )
                      )
                    ),
                    nullable = false
                  ),
                  StructField(
                    "bax",
                    StringType,
                    nullable = false
                  ),
                  StructField(
                    "bay",
                    StringType,
                    nullable = false
                  ),
                  StructField(
                    "baz",
                    StringType,
                    nullable = false
                  )
                )
              ),
              nullable = false
            ),
            StructField(
              "v",
              ArrayType(
                ArrayType(
                  StructType(Seq(StructField("v1", StringType, nullable = false), StructField("v2", StringType, nullable = false))),
                  containsNull = false
                ),
                containsNull = false
              ),
              nullable = false
            ),
            StructField(
              "w",
              ArrayType(
                StructType(Seq(StructField("x", StringType, nullable = false), StructField("y", StringType, nullable = false))),
                containsNull = false
              ),
              nullable = false
            ),
            StructField(
              "x",
              StringType,
              nullable = false
            ),
            StructField(
              "y",
              StringType,
              nullable = false
            ),
            StructField(
              "z",
              StringType,
              nullable = false
            )
          )
        )

        val expectedData = Seq(
          Row(
            Row(
              Row("yoVal", "this"),
              "baxVal",
              "bayVal",
              "is"
            ),
            Seq(
              Seq(
                Row("xVal", "yVal"),
                Row("xVal1", "yVal1")
              )
            ),
            Seq(
              Row("xVal", "yVal"),
              Row("xVal1", "yVal1")
            ),
            "something",
            "cool",
            ";)"
          )
        )

        val df = spark
          .createDataFrame(
            spark.sparkContext.parallelize(expectedData),
            schema
          )
          .toSchemaWithNullabilityAligned(expectedSchema)

        val expectedDF = spark
          .createDataFrame(
            spark.sparkContext.parallelize(expectedData),
            expectedSchema
          )

        assertSmallDataFrameEquality(
          df,
          expectedDF,
          ignoreNullable = false
        )
      }

      "align from nullable to not null when order field different" - {
        val data = Seq(
          Row(
            Row(
              Row("yoVal", "this"),
              "baxVal",
              "bayVal",
              "is"
            ),
            Seq(
              Seq(
                Row("yVal", "xVal"),
                Row("yVal1", "xVal1")
              )
            ),
            Seq(
              Row("yVal", "xVal"),
              Row("yVal1", "xVal1")
            ),
            ";)",
            "cool",
            "something"
          )
        )

        val schema = StructType(
          Seq(
            StructField(
              "foo",
              StructType(
                Seq(
                  StructField(
                    "bar",
                    StructType(
                      Seq(
                        StructField(
                          "yoo",
                          StringType
                        ),
                        StructField(
                          "zoo",
                          StringType
                        )
                      )
                    )
                  ),
                  StructField(
                    "bax",
                    StringType
                  ),
                  StructField(
                    "bay",
                    StringType
                  ),
                  StructField(
                    "baz",
                    StringType
                  )
                )
              )
            ),
            StructField(
              "v",
              ArrayType(
                ArrayType(StructType(Seq(StructField("v2", StringType), StructField("v1", StringType))), containsNull = false),
                containsNull = false
              )
            ),
            StructField(
              "w",
              ArrayType(StructType(Seq(StructField("y", StringType), StructField("x", StringType))))
            ),
            StructField(
              "z",
              StringType
            ),
            StructField(
              "y",
              StringType
            ),
            StructField(
              "x",
              StringType
            )
          )
        )

        val expectedSchema = StructType(
          Seq(
            StructField(
              "foo",
              StructType(
                Seq(
                  StructField(
                    "bar",
                    StructType(
                      Seq(
                        StructField(
                          "yoo",
                          StringType,
                          nullable = false
                        ),
                        StructField(
                          "zoo",
                          StringType,
                          nullable = false
                        )
                      )
                    ),
                    nullable = false
                  ),
                  StructField(
                    "bax",
                    StringType,
                    nullable = false
                  ),
                  StructField(
                    "bay",
                    StringType,
                    nullable = false
                  ),
                  StructField(
                    "baz",
                    StringType,
                    nullable = false
                  )
                )
              ),
              nullable = false
            ),
            StructField(
              "v",
              ArrayType(
                ArrayType(
                  StructType(Seq(StructField("v1", StringType, nullable = false), StructField("v2", StringType, nullable = false))),
                  containsNull = false
                ),
                containsNull = false
              ),
              nullable = false
            ),
            StructField(
              "w",
              ArrayType(
                StructType(Seq(StructField("x", StringType, nullable = false), StructField("y", StringType, nullable = false))),
                containsNull = false
              ),
              nullable = false
            ),
            StructField(
              "x",
              StringType,
              nullable = false
            ),
            StructField(
              "y",
              StringType,
              nullable = false
            ),
            StructField(
              "z",
              StringType,
              nullable = false
            )
          )
        )

        val expectedData = Seq(
          Row(
            Row(
              Row("yoVal", "this"),
              "baxVal",
              "bayVal",
              "is"
            ),
            Seq(
              Seq(
                Row("xVal", "yVal"),
                Row("xVal1", "yVal1")
              )
            ),
            Seq(
              Row("xVal", "yVal"),
              Row("xVal1", "yVal1")
            ),
            "something",
            "cool",
            ";)"
          )
        )

        val df = spark
          .createDataFrame(
            spark.sparkContext.parallelize(data),
            schema
          )
          .toSchemaWithNullabilityAligned(expectedSchema)

        val expectedDF = spark
          .createDataFrame(
            spark.sparkContext.parallelize(expectedData),
            expectedSchema
          )

        assertSmallDataFrameEquality(
          df,
          expectedDF,
          ignoreNullable = false
        )
      }

      "align from non nullable to nullable from of nested schema" - {
        val data = Seq(
          Row(
            Row(
              "bayVal",
              "baxVal",
              Row("this", "yoVal"),
              "is"
            ),
            Seq(
              Seq(Row("yVal", "xVal"), Row("yVal1", "xVal1"))
            ),
            Seq(
              Row("yVal", "xVal"),
              Row("yVal1", "xVal1")
            ),
            "something",
            "cool",
            ";)"
          )
        )

        val schema = StructType(
          Seq(
            StructField(
              "foo",
              StructType(
                Seq(
                  StructField(
                    "bay",
                    StringType,
                    nullable = false
                  ),
                  StructField(
                    "bax",
                    StringType,
                    nullable = false
                  ),
                  StructField(
                    "bar",
                    StructType(
                      Seq(
                        StructField(
                          "zoo",
                          StringType
                        ),
                        StructField(
                          "yoo",
                          StringType
                        )
                      )
                    ),
                    nullable = false
                  ),
                  StructField(
                    "baz",
                    StringType,
                    nullable = false
                  )
                )
              )
            ),
            StructField(
              "v",
              ArrayType(ArrayType(StructType(Seq(StructField("v2", StringType, nullable = false), StructField("v1", StringType, nullable = false)))))
            ),
            StructField(
              "w",
              ArrayType(StructType(Seq(StructField("y", StringType), StructField("x", StringType)))),
              nullable = false
            ),
            StructField(
              "x",
              StringType,
              nullable = false
            ),
            StructField(
              "y",
              StringType,
              nullable = false
            ),
            StructField(
              "z",
              StringType,
              nullable = false
            )
          )
        )

        val expectedSchema = StructType(
          Seq(
            StructField(
              "foo",
              StructType(
                Seq(
                  StructField(
                    "bay",
                    StringType
                  ),
                  StructField(
                    "bax",
                    StringType
                  ),
                  StructField(
                    "bar",
                    StructType(
                      Seq(
                        StructField(
                          "zoo",
                          StringType
                        ),
                        StructField(
                          "yoo",
                          StringType
                        )
                      )
                    )
                  ),
                  StructField(
                    "baz",
                    StringType
                  )
                )
              ),
              nullable = true
            ),
            StructField(
              "v",
              ArrayType(ArrayType(StructType(Seq(StructField("v2", StringType), StructField("v1", StringType)))))
            ),
            StructField(
              "w",
              ArrayType(StructType(Seq(StructField("y", StringType), StructField("x", StringType))))
            ),
            StructField(
              "x",
              StringType
            ),
            StructField(
              "y",
              StringType
            ),
            StructField(
              "z",
              StringType
            )
          )
        )

        val inputDf = spark
          .createDataFrame(
            spark.sparkContext.parallelize(data),
            StructType(schema)
          )

        val df = inputDf.toSchemaWithNullabilityAligned(expectedSchema, alignNotNullToNullable = true)

        val expectedData = Seq(
          Row(
            Row(
              "bayVal",
              "baxVal",
              Row("this", "yoVal"),
              "is"
            ),
            Seq(
              Seq(Row("yVal", "xVal"), Row("yVal1", "xVal1"))
            ),
            Seq(
              Row("yVal", "xVal"),
              Row("yVal1", "xVal1")
            ),
            "something",
            "cool",
            ";)"
          )
        )

        val expectedDF = spark
          .createDataFrame(
            spark.sparkContext.parallelize(expectedData),
            StructType(expectedSchema)
          )

        assertSmallDataFrameEquality(
          df,
          expectedDF,
          ignoreNullable = false
        )
      }

      "align from nullable to non nullable for nested array of struct" - {
        val schema = StructType(
          Seq(
            StructField(
              "a1",
              StructType(
                Seq(
                  StructField(
                    "a2",
                    ArrayType(StructType(Seq(StructField("a2", StringType), StructField("a4", StringType))), containsNull = false)
                  )
                )
              )
            ),
            StructField(
              "v",
              ArrayType(
                ArrayType(StructType(Seq(StructField("v1", StringType), StructField("v2", StringType))), containsNull = false),
                containsNull = false
              ),
              nullable = false
            ),
            StructField(
              "w",
              ArrayType(StructType(Seq(StructField("x", StringType), StructField("y", StringType))), containsNull = false),
              nullable = false
            ),
            StructField(
              "x",
              ArrayType(
                StructType(
                  Seq(
                    StructField("x1", ArrayType(StructType(Seq(StructField("a", StringType), StructField("b", StringType))), containsNull = false))
                  )
                ),
                containsNull = false
              )
            )
          )
        )

        val expectedSchema = StructType(
          Seq(
            StructField(
              "a1",
              StructType(
                Seq(
                  StructField(
                    "a2",
                    ArrayType(
                      StructType(Seq(StructField("a2", StringType, nullable = false), StructField("a4", StringType, nullable = false))),
                      containsNull = false
                    ),
                    nullable = false
                  )
                )
              ),
              nullable = false
            ),
            StructField(
              "v",
              ArrayType(
                ArrayType(
                  StructType(Seq(StructField("v1", StringType, nullable = false), StructField("v2", StringType, nullable = false))),
                  containsNull = false
                ),
                containsNull = false
              ),
              nullable = false
            ),
            StructField(
              "w",
              ArrayType(
                StructType(Seq(StructField("x", StringType, nullable = false), StructField("y", StringType, nullable = false))),
                containsNull = false
              ),
              nullable = false
            ),
            StructField(
              "x",
              ArrayType(
                StructType(
                  Seq(
                    StructField(
                      "x1",
                      ArrayType(
                        StructType(Seq(StructField("a", StringType, nullable = false), StructField("b", StringType, nullable = false))),
                        containsNull = false
                      ),
                      nullable = false
                    )
                  )
                ),
                containsNull = false
              ),
              nullable = false
            )
          )
        )

        val expectedData = Seq(
          Row(
            Row(
              Seq(
                Row("a2Val", "a4Val")
              )
            ),
            Seq(
              Seq(
                Row("xVal", "yVal"),
                Row("xVal1", "yVal1")
              )
            ),
            Seq(
              Row("xVal", "yVal"),
              Row("xVal1", "yVal1")
            ),
            Seq(
              Row(
                Seq(
                  Row("x3Val", "x4Val")
                )
              )
            )
          )
        )

        val df = spark
          .createDataFrame(
            spark.sparkContext.parallelize(expectedData),
            schema
          )
          .toSchemaWithNullabilityAligned(expectedSchema)

        val expectedDF = spark
          .createDataFrame(
            spark.sparkContext.parallelize(expectedData),
            StructType(expectedSchema)
          )

        assertSmallDataFrameEquality(
          df,
          expectedDF,
          ignoreNullable = false
        )
      }

      "align from non nullable to nullable for nested array of struct" - {
        val data = Seq(
          Row(
            Row(
              Seq(
                Row("a4Val", "a2Val")
              )
            ),
            Seq(
              Seq(Row("yVal", "xVal"), Row("yVal1", "xVal1"))
            ),
            Seq(
              Row("yVal", "xVal"),
              Row("yVal1", "xVal1")
            ),
            Seq(
              Row(
                Seq(
                  Row("x4Val", "x3Val")
                )
              )
            )
          )
        )

        val schema = StructType(
          Seq(
            StructField(
              "a1",
              StructType(
                Seq(
                  StructField(
                    "a2",
                    ArrayType(
                      StructType(Seq(StructField("a2", StringType, nullable = false), StructField("a4", StringType, nullable = false))),
                      containsNull = false
                    ),
                    nullable = false
                  )
                )
              ),
              nullable = false
            ),
            StructField(
              "v",
              ArrayType(
                ArrayType(
                  StructType(Seq(StructField("v1", StringType, nullable = false), StructField("v2", StringType, nullable = false))),
                  containsNull = false
                ),
                containsNull = false
              ),
              nullable = false
            ),
            StructField(
              "w",
              ArrayType(
                StructType(Seq(StructField("x", StringType, nullable = false), StructField("y", StringType, nullable = false))),
                containsNull = false
              ),
              nullable = false
            ),
            StructField(
              "x",
              ArrayType(
                StructType(
                  Seq(
                    StructField(
                      "x1",
                      ArrayType(
                        StructType(Seq(StructField("a", StringType, nullable = false), StructField("b", StringType, nullable = false))),
                        containsNull = false
                      ),
                      nullable = false
                    )
                  )
                ),
                containsNull = false
              ),
              nullable = false
            )
          )
        )

        val expectedSchema = StructType(
          Seq(
            StructField(
              "a1",
              StructType(
                Seq(
                  StructField(
                    "a2",
                    ArrayType(StructType(Seq(StructField("a4", StringType), StructField("a2", StringType))))
                  )
                )
              ),
              nullable = false
            ),
            StructField(
              "v",
              ArrayType(ArrayType(StructType(Seq(StructField("v2", StringType), StructField("v1", StringType))))),
              nullable = true
            ),
            StructField(
              "w",
              ArrayType(StructType(Seq(StructField("y", StringType), StructField("x", StringType)))),
              nullable = true
            ),
            StructField(
              "x",
              ArrayType(
                StructType(
                  Seq(StructField("x1", ArrayType(StructType(Seq(StructField("b", StringType), StructField("a", StringType)))), nullable = true))
                )
              ),
              nullable = true
            )
          )
        )

        val df = spark
          .createDataFrame(
            spark.sparkContext.parallelize(data),
            StructType(schema)
          )
          .toSchemaWithNullabilityAligned(expectedSchema, alignNotNullToNullable = true)

        val expectedData = Seq(
          Row(
            Row(
              Seq(
                Row("a2Val", "a4Val")
              )
            ),
            Seq(
              Seq(
                Row("xVal", "yVal"),
                Row("xVal1", "yVal1")
              )
            ),
            Seq(
              Row("xVal", "yVal"),
              Row("xVal1", "yVal1")
            ),
            Seq(
              Row(
                Seq(
                  Row("x3Val", "x4Val")
                )
              )
            )
          )
        )

        val expectedDF = spark
          .createDataFrame(
            spark.sparkContext.parallelize(expectedData),
            StructType(expectedSchema)
          )

        assertSmallDataFrameEquality(
          df,
          expectedDF,
          ignoreNullable = false
        )
      }

      def align(schema: StructType, row: Row, target: StructType): DataFrame =
        spark.createDataFrame(java.util.Arrays.asList(row), schema).toSchemaWithNullabilityAligned(target)

      "fill target fields missing from the original schema with nulls" - {
        val nested = StructType(Seq(StructField("code", StringType), StructField("name", StringType)))
        val target = StructType(Seq(StructField("id", IntegerType), StructField("n", nested), StructField("missing", StringType)))
        val df = align(
          StructType(Seq(StructField("id", IntegerType), StructField("n", StructType(Seq(StructField("code", StringType)))))),
          Row(1, Row("AU")),
          target
        )

        assert(df.schema == target)
        assert(df.collect().toSeq == Seq(Row(1, Row("AU", null), null)))
      }

      "reject non nullable target fields missing from the original schema" - {
        val target = StructType(Seq(StructField("id", IntegerType), StructField("missing", StringType, nullable = false)))
        intercept[IllegalArgumentException](align(StructType(Seq(StructField("id", IntegerType))), Row(1), target))
      }

      "keep null parent struct with non nullable children null" - {
        val target = StructType(Seq(StructField("opt", StructType(Seq(StructField("code", StringType, nullable = false))))))
        val df     = align(StructType(Seq(StructField("opt", StructType(Seq(StructField("code", StringType)))))), Row(null), target)

        assert(df.schema == target)
        assert(df.collect().toSeq == Seq(Row(null)))
      }

      "align arrays of primitives at top level and inside structs" - {
        val tags   = StructField("tags", ArrayType(StringType, containsNull = false), nullable = false)
        val target = StructType(Seq(tags, StructField("n", StructType(Seq(tags)), nullable = false)))
        val source =
          StructType(Seq(StructField("tags", ArrayType(StringType)), StructField("n", StructType(Seq(StructField("tags", ArrayType(StringType)))))))
        val df = align(source, Row(Seq("a"), Row(Seq("b"))), target)

        assert(df.schema == target)
        assert(df.collect().toSeq == Seq(Row(Seq("a"), Row(Seq("b")))))
      }

      "fail when a null array element is aligned to containsNull = false" - {
        val element = StructType(Seq(StructField("p", StringType)))
        val target  = StructType(Seq(StructField("l", ArrayType(element, containsNull = false))))
        val df      = align(StructType(Seq(StructField("l", ArrayType(element)))), Row(Seq(null)), target)

        intercept[Exception](df.collect())
      }

      "preserve target metadata and support names with dots" - {
        val meta   = new MetadataBuilder().putString("comment", "carrier").build()
        val target = StructType(Seq(StructField("a.b", StringType, nullable = false, meta)))
        val df     = align(StructType(Seq(StructField("a.b", StringType))), Row("x"), target)

        assert(df.schema == target)
        assert(df.collect().toSeq == Seq(Row("x")))
      }
    }
  }
}
