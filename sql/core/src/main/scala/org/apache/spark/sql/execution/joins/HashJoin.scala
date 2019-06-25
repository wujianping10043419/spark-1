/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.execution.joins

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.{RowIterator, SparkPlan}
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.types.{IntegralType, LongType}

trait HashJoin {
  self: SparkPlan =>

  val leftKeys: Seq[Expression]
  val rightKeys: Seq[Expression]
  val joinType: JoinType
  val buildSide: BuildSide
  val condition: Option[Expression]
  val left: SparkPlan
  val right: SparkPlan

  override def output: Seq[Attribute] = {
    joinType match {
      case Inner =>
        left.output ++ right.output
      case LeftOuter =>
        left.output ++ right.output.map(_.withNullability(true))
      case RightOuter =>
        left.output.map(_.withNullability(true)) ++ right.output
      case j: ExistenceJoin =>
        left.output :+ j.exists
      case LeftExistence(_) =>
        left.output
      case x =>
        throw new IllegalArgumentException(s"HashJoin should not take $x as the JoinType")
    }
  }

  override def outputPartitioning: Partitioning = streamedPlan.outputPartitioning

  protected lazy val (buildPlan, streamedPlan) = buildSide match {
    case BuildLeft => (left, right)
    case BuildRight => (right, left)
  }

  protected lazy val (buildKeys, streamedKeys) = {
    require(leftKeys.map(_.dataType) == rightKeys.map(_.dataType),
      "Join keys from two sides should have same types")
    val lkeys = HashJoin.rewriteKeyExpr(leftKeys).map(BindReferences.bindReference(_, left.output))
    val rkeys = HashJoin.rewriteKeyExpr(rightKeys)
      .map(BindReferences.bindReference(_, right.output))
    buildSide match {
      case BuildLeft => (lkeys, rkeys)
      case BuildRight => (rkeys, lkeys)
    }
  }



  protected def buildSideKeyGenerator(): Projection =
    UnsafeProjection.create(buildKeys)

  protected def streamSideKeyGenerator(): UnsafeProjection =
    UnsafeProjection.create(streamedKeys)

  @transient private[this] lazy val boundCondition = if (condition.isDefined) {
    newPredicate(condition.get, streamedPlan.output ++ buildPlan.output)
  } else {
    (r: InternalRow) => true
  }

  protected def createResultProjection(): (InternalRow) => InternalRow = joinType match {
    case LeftExistence(_) =>
      UnsafeProjection.create(output, output)
    case _ =>
      // Always put the stream side on left to simplify implementation
      // both of left and right side could be null
      UnsafeProjection.create(
        output, (streamedPlan.output ++ buildPlan.output).map(_.withNullability(true)))
  }

  private def innerJoin(
                         streamIter: Iterator[InternalRow],
                         hashedRelation: HashedRelation,
                         HashJoinExec_time_match: SQLMetric,
                         HashJoinExec_time_total: SQLMetric): Iterator[InternalRow] = {
    val joinRow = new JoinedRow
    val joinKeys = streamSideKeyGenerator()
    streamIter.flatMap { srow =>
      joinRow.withLeft(srow)
      val begin = System.nanoTime()
      val matches = hashedRelation.get(joinKeys(srow))
      HashJoinExec_time_match += (System.nanoTime() - begin)
      val res = if (matches != null) {
        matches.map(joinRow.withRight(_)).filter(boundCondition)
      } else {
        Seq.empty
      }
      HashJoinExec_time_total += (System.nanoTime() - begin)
      res
    }
  }

  private def outerJoin(
                         streamedIter: Iterator[InternalRow],
                         hashedRelation: HashedRelation,
                         HashJoinExec_time_total: SQLMetric): Iterator[InternalRow] = {
    val joinedRow = new JoinedRow()
    val keyGenerator = streamSideKeyGenerator()
    val nullRow = new GenericInternalRow(buildPlan.output.length)

    streamedIter.flatMap { currentRow =>
      val rowKey = keyGenerator(currentRow)
      joinedRow.withLeft(currentRow)
      val begin = System.nanoTime()
      val buildIter = hashedRelation.get(rowKey)
      HashJoinExec_time_total += (System.nanoTime() - begin)
      new RowIterator {
        private var found = false
        override def advanceNext(): Boolean = {
          while (buildIter != null && buildIter.hasNext) {
            val begin = System.nanoTime()
            val nextBuildRow = buildIter.next()
            if (boundCondition(joinedRow.withRight(nextBuildRow))) {
              found = true
              HashJoinExec_time_total += (System.nanoTime() - begin)
              return true
            }
          }
          if (!found) {
            val begin = System.nanoTime()
            joinedRow.withRight(nullRow)
            found = true
            HashJoinExec_time_total += (System.nanoTime() - begin)
            return true
          }
          false
        }
        override def getRow: InternalRow = joinedRow
      }.toScala
    }
  }

  private def semiJoin(
                        streamIter: Iterator[InternalRow],
                        hashedRelation: HashedRelation,
                        HashJoinExec_time_total: SQLMetric): Iterator[InternalRow] = {
    val joinKeys = streamSideKeyGenerator()
    val joinedRow = new JoinedRow
    streamIter.filter { current =>
      val begin = System.nanoTime()
      val key = joinKeys(current)
      lazy val buildIter = hashedRelation.get(key)
      val res = !key.anyNull && buildIter != null && (condition.isEmpty || buildIter.exists {
        (row: InternalRow) => boundCondition(joinedRow(current, row))
      })
      HashJoinExec_time_total += (System.nanoTime() - begin)
      res
    }
  }

  private def existenceJoin(
                             streamIter: Iterator[InternalRow],
                             hashedRelation: HashedRelation,
                             HashJoinExec_time_total: SQLMetric): Iterator[InternalRow] = {
    val joinKeys = streamSideKeyGenerator()
    val result = new GenericMutableRow(Array[Any](null))
    val joinedRow = new JoinedRow
    streamIter.map { current =>
      val begin = System.nanoTime()
      val key = joinKeys(current)
      lazy val buildIter = hashedRelation.get(key)
      val exists = !key.anyNull && buildIter != null && (condition.isEmpty || buildIter.exists {
        (row: InternalRow) => boundCondition(joinedRow(current, row))
      })
      result.setBoolean(0, exists)
      val res = joinedRow(current, result)
      HashJoinExec_time_total += (System.nanoTime() - begin)
      res
    }
  }

  private def antiJoin(
                        streamIter: Iterator[InternalRow],
                        hashedRelation: HashedRelation,
                        HashJoinExec_time_total: SQLMetric): Iterator[InternalRow] = {
    val joinKeys = streamSideKeyGenerator()
    val joinedRow = new JoinedRow
    streamIter.filter { current =>
      val begin = System.nanoTime()
      val key = joinKeys(current)
      lazy val buildIter = hashedRelation.get(key)
      val res = key.anyNull || buildIter == null || (condition.isDefined && !buildIter.exists {
        row => boundCondition(joinedRow(current, row))
      })
      HashJoinExec_time_total += (System.nanoTime() - begin)
      res
    }
  }

  protected def join(
                      streamedIter: Iterator[InternalRow],
                      hashed: HashedRelation,
                      numOutputRows: SQLMetric,
                      BroadcastHashJoinExec_time_match: SQLMetric,
                      BroadcastHashJoinExec_time_total: SQLMetric): Iterator[InternalRow] = {

    val joinedIter = joinType match {
      case Inner =>
        innerJoin(streamedIter, hashed, BroadcastHashJoinExec_time_match, BroadcastHashJoinExec_time_total)
      case LeftOuter | RightOuter =>
        outerJoin(streamedIter, hashed, BroadcastHashJoinExec_time_total)
      case LeftSemi =>
        semiJoin(streamedIter, hashed, BroadcastHashJoinExec_time_total)
      case LeftAnti =>
        antiJoin(streamedIter, hashed, BroadcastHashJoinExec_time_total)
      case j: ExistenceJoin =>
        existenceJoin(streamedIter, hashed, BroadcastHashJoinExec_time_total)
      case x =>
        throw new IllegalArgumentException(
          s"BroadcastHashJoin should not take $x as the JoinType")
    }

    val resultProj = createResultProjection
    joinedIter.map { r =>
      val begin = System.nanoTime()
      numOutputRows += 1
      val result = resultProj(r)
      BroadcastHashJoinExec_time_total += (System.nanoTime() - begin)
      result
    }
  }
}

object HashJoin {
  /**
   * Try to rewrite the key as LongType so we can use getLong(), if they key can fit with a long.
   *
   * If not, returns the original expressions.
   */
  private[joins] def rewriteKeyExpr(keys: Seq[Expression]): Seq[Expression] = {
    assert(keys.nonEmpty)
    // TODO: support BooleanType, DateType and TimestampType
    if (keys.exists(!_.dataType.isInstanceOf[IntegralType])
      || keys.map(_.dataType.defaultSize).sum > 8) {
      return keys
    }

    var keyExpr: Expression = if (keys.head.dataType != LongType) {
      Cast(keys.head, LongType)
    } else {
      keys.head
    }
    keys.tail.foreach { e =>
      val bits = e.dataType.defaultSize * 8
      keyExpr = BitwiseOr(ShiftLeft(keyExpr, Literal(bits)),
        BitwiseAnd(Cast(e, LongType), Literal((1L << bits) - 1)))
    }
    keyExpr :: Nil
  }
}
