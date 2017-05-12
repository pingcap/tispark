package org.apache.spark.sql.util

/**
  * Helper classes for various collection types.
  */
object CollectionUtils {

  implicit class RichIterable[A, B <: Iterable[A]](val seq: B with Iterable[A]) {

    /**
      * Retrieves a set of all values that have duplicates in this sequence.
      *
      * @return Set of all duplicate values.
      */
    def duplicates: Set[A] = {
      def inner(seen: Set[A], duplicates: Set[A], remaining: Iterable[A]): Set[A] = {
        if (remaining.isEmpty) {
          duplicates
        } else {
          val head = remaining.head
          if (seen.contains(head)) {
            inner(seen, duplicates + head, remaining.tail)
          } else {
            inner(seen + head, duplicates, remaining.tail)
          }
        }
      }

      inner(Set.empty, Set.empty, seq)
    }

    /**
      * @return [[None]] if the given [[Iterable]] was empty, else [[Some]] of the given iterable.
      */
    def nonEmptyOpt: Option[B] =
      if (seq.nonEmpty) Some(seq) else None

    /**
      * @return [[Some]] of the given [[Iterable]] if it was empty, else [[None]]
      */
    def emptyOpt: Option[B] =
      if (seq.isEmpty) Some(seq) else None
  }

  /**
    * A map wrapper class that offers additional functionality.
    *
    * @param map The map to wrap.
    * @tparam A The key type.
    * @tparam B The value type.
    */
  implicit class RichMap[A, B](val map: Map[A, B]) {
    /**
      * Returns a map with the key updated to the value if it was not contained before.
      * Otherwise returns the original map.
      *
      * @param key The key to check.
      * @param value The value to update with in case the key is not present.
      * @return A map update with the value if the key was not present before,
      *         otherwise the original map.
      */
    def putIfAbsent(key: A, value: B): Map[A, B] = {
      if (map.contains(key)) map
      else map + (key -> value)
    }
  }

  /**
    * A map where the string keys are case insensitive.
    *
    * @param map The map that contains the data.
    * @tparam V The value type.
    */
  class CaseInsensitiveMap[V](map: Map[String, V]) extends Map[String, V] {
    val baseMap = map map {
      case (k, v) => k.toLowerCase -> v
    }
    // scalastyle:off method.name
    /**
      * Returns a new [[CaseInsensitiveMap]] with the key-value-pair added.
      *
      * @param kv The key-value-pair to add
      * @tparam B1 The type of the value
      * @return A new [[CaseInsensitiveMap]] with the key-value-pair added.
      */
    override def + [B1 >: V](kv: (String, B1)): Map[String, B1] =
      new CaseInsensitiveMap[B1](baseMap + kv.copy(_1 = kv._1.toLowerCase))
    // scalastyle:on method.name

    /** @inheritdoc */
    override def get(key: String): Option[V] = baseMap.get(key.toLowerCase)

    /** @inheritdoc */
    override def iterator: Iterator[(String, V)] = baseMap.iterator

    // scalastyle:off method.name
    /**
      * Returns a new [[CaseInsensitiveMap]] with the key-value-pair removed.
      *
      * @param key The key to remove
      * @return A new [[CaseInsensitiveMap]] with the key-value-pair removed.
      */
    override def - (key: String): Map[String, V] =
      new CaseInsensitiveMap[V](baseMap - key.toLowerCase)
    // scalastyle:on method.name


    /** @inheritdoc */
    override def canEqual(other: Any): Boolean = other.isInstanceOf[Map[_, _]]

    /** @inheritdoc */
    override def equals(other: Any): Boolean = other match {
      case that: Map[_, _] =>
        this.baseMap == that
      case _ => false
    }

    /** @inheritdoc */
    override def hashCode(): Int = baseMap.hashCode()
  }

  object CaseInsensitiveMap {
    /**
      * Creates a new case insensitive map from the given tuples.
      *
      * @param map The map to wrap inside a case insensitive map.
      * @tparam V The value type.
      * @return A new [[CaseInsensitiveMap]] with the values of the given map
      *         and the keys case insensitive.
      */
    def apply[V](map: Map[String, V]): CaseInsensitiveMap[V] =
      new CaseInsensitiveMap[V](map)

    /**
      * Creates a new case insensitive map from the given tuples.
      *
      * @param kvs The tuples to put in the case insensitive map.
      * @tparam V The value type.
      * @return A new [[CaseInsensitiveMap]] with the values of the given tuples
      *         and the keys case insensitive.
      */
    def apply[V](kvs: (String, V)*): CaseInsensitiveMap[V] =
      new CaseInsensitiveMap[V](kvs.toMap)

    /**
      * Creates a new empty [[CaseInsensitiveMap]].
      *
      * @tparam V The value type.
      * @return An empty [[CaseInsensitiveMap]].
      */
    def empty[V]: CaseInsensitiveMap[V] =
      new CaseInsensitiveMap[V](Map.empty[String, V])
  }
}
