package org.apache.spark.sql

import org.apache.spark.sql.BebeFunctions.{assertNotNull, knownNotNull, knownNullable}
import org.apache.spark.sql.functions.{col, lit, struct, transform, when}
import org.apache.spark.sql.types.{ArrayType, DataType, StructField, StructType}

object DataFrameExt {

  /**
   * Aligns the nullability of the columns in the original schema to match the target schema.
   * Target fields missing from the original schema are filled with typed nulls (non-nullable ones are rejected).
   * Null parent structs stay null; array element nullability follows `containsNull`.
   * Data types are not cast.
   */
  private def alignSchemaNullability(
      originalSchema: StructType,
      targetSchema: StructType,
      alignNotNullToNullable: Boolean
  ): Seq[Column] = {
    def withNullability(value: Column, sourceNullable: Boolean, targetNullable: Boolean): Column =
      (targetNullable, sourceNullable, alignNotNullToNullable) match {
        case (false, true, _)     => assertNotNull(value)
        case (true, false, true)  => knownNullable(value)
        case (true, false, false) => value
        case (false, false, _)    => knownNotNull(value)
        case (true, true, _)      => knownNullable(value)
      }

    def alignValue(value: Column, source: DataType, target: DataType, sourceNullable: Boolean, path: String): Column =
      (source, target) match {
        case (s: StructType, t: StructType) =>
          val aligned = struct(t.map(tf => alignField(s.find(_.name == tf.name), tf, value.getField(tf.name), s"$path.${tf.name}")): _*)
          if (sourceNullable) when(value.isNotNull, aligned) else aligned
        case (ArrayType(se, sContainsNull), ArrayType(te, tContainsNull)) =>
          transform(value, e => withNullability(alignValue(e, se, te, sContainsNull, s"$path[]"), sContainsNull, tContainsNull))
        case _ => value
      }

    def alignField(source: Option[StructField], target: StructField, value: => Column, path: String): Column =
      (source match {
        case Some(sf) =>
          withNullability(alignValue(value, sf.dataType, target.dataType, sf.nullable, path), sf.nullable, target.nullable)
        case None if target.nullable => lit(null).cast(target.dataType)
        case None                    => throw new IllegalArgumentException(s"Non-nullable field `$path` is missing from the original schema")
      }).as(target.name, target.metadata)

    targetSchema.map { tf =>
      alignField(originalSchema.find(_.name == tf.name), tf, col(s"`${tf.name.replace("`", "``")}`"), tf.name)
    }
  }

  implicit class DataFrameMethods(df: DataFrame) {

    /**
     * Converts the DataFrame to the specified target schema, aligning nullability if required.
     * This is similar to `df.to(targetSchema)` but allows for nullability alignment by asserting nullability
     * for each column where the original nullability does not match the target nullability.
     *
     * @param targetSchema           The target schema to convert to.
     * @param alignNotNullToNullable by default if original column is not nullable and target column is nullable, it will not make it nullable.
     *                               If set to true, it will align target Dataframe column to nullable.
     * @return A new DataFrame with the specified schema.
     */
    def toSchemaWithNullabilityAligned(targetSchema: StructType, alignNotNullToNullable: Boolean = false): DataFrame = {
      val columns = alignSchemaNullability(df.schema, targetSchema, alignNotNullToNullable)
      df.select(columns: _*)
    }
  }
}
