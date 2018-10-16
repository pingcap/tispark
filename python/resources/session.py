#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

from __future__ import print_function
import sys
import warnings
from functools import reduce
from threading import RLock

if sys.version >= '3':
    basestring = unicode = str
    xrange = range
else:
    from itertools import izip as zip, imap as map

from pyspark import since
from pyspark.rdd import RDD, ignore_unicode_prefix
from pyspark.sql.conf import RuntimeConfig
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.readwriter import DataFrameReader
from pyspark.sql.streaming import DataStreamReader
from pyspark.sql.types import Row, DataType, StringType, StructType, TimestampType, \
    _make_type_verifier, _infer_schema, _has_nulltype, _merge_type, _create_converter, \
    _parse_datatype_string
from pyspark.sql.utils import install_exception_handler

__all__ = ["SparkSession"]


def _monkey_patch_RDD(sparkSession):
    def toDF(self, schema=None, sampleRatio=None):
        """
        Converts current :class:`RDD` into a :class:`DataFrame`

        This is a shorthand for ``spark.createDataFrame(rdd, schema, sampleRatio)``

        :param schema: a :class:`pyspark.sql.types.StructType` or list of names of columns
        :param samplingRatio: the sample ratio of rows used for inferring
        :return: a DataFrame

        >>> rdd.toDF().collect()
        [Row(name=u'Alice', age=1)]
        """
        return sparkSession.createDataFrame(self, schema, sampleRatio)

    RDD.toDF = toDF


class SparkSession(object):
    """The entry point to programming Spark with the Dataset and DataFrame API.

    A SparkSession can be used create :class:`DataFrame`, register :class:`DataFrame` as
    tables, execute SQL over tables, cache tables, and read parquet files.
    To create a SparkSession, use the following builder pattern:

    >>> spark = SparkSession.builder \\
    ...     .master("local") \\
    ...     .appName("Word Count") \\
    ...     .config("spark.some.config.option", "some-value") \\
    ...     .getOrCreate()

    .. autoattribute:: builder
       :annotation:
    """

    class Builder(object):
        """Builder for :class:`SparkSession`.
        """

        _lock = RLock()
        _options = {}

        @since(2.0)
        def config(self, key=None, value=None, conf=None):
            """Sets a config option. Options set using this method are automatically propagated to
            both :class:`SparkConf` and :class:`SparkSession`'s own configuration.

            For an existing SparkConf, use `conf` parameter.

            >>> from pyspark.conf import SparkConf
            >>> SparkSession.builder.config(conf=SparkConf())
            <pyspark.sql.session...

            For a (key, value) pair, you can omit parameter names.

            >>> SparkSession.builder.config("spark.some.config.option", "some-value")
            <pyspark.sql.session...

            :param key: a key name string for configuration property
            :param value: a value for configuration property
            :param conf: an instance of :class:`SparkConf`
            """
            with self._lock:
                if conf is None:
                    self._options[key] = str(value)
                else:
                    for (k, v) in conf.getAll():
                        self._options[k] = v
                return self

        @since(2.0)
        def master(self, master):
            """Sets the Spark master URL to connect to, such as "local" to run locally, "local[4]"
            to run locally with 4 cores, or "spark://master:7077" to run on a Spark standalone
            cluster.

            :param master: a url for spark master
            """
            return self.config("spark.master", master)

        @since(2.0)
        def appName(self, name):
            """Sets a name for the application, which will be shown in the Spark web UI.

            If no application name is set, a randomly generated name will be used.

            :param name: an application name
            """
            return self.config("spark.app.name", name)

        @since(2.0)
        def enableHiveSupport(self):
            """Enables Hive support, including connectivity to a persistent Hive metastore, support
            for Hive serdes, and Hive user-defined functions.
            """
            return self.config("spark.sql.catalogImplementation", "hive")

        @since(2.0)
        def getOrCreate(self):
            """Gets an existing :class:`SparkSession` or, if there is no existing one, creates a
            new one based on the options set in this builder.

            This method first checks whether there is a valid global default SparkSession, and if
            yes, return that one. If no valid global default SparkSession exists, the method
            creates a new SparkSession and assigns the newly created SparkSession as the global
            default.

            >>> s1 = SparkSession.builder.config("k1", "v1").getOrCreate()
            >>> s1.conf.get("k1") == s1.sparkContext.getConf().get("k1") == "v1"
            True

            In case an existing SparkSession is returned, the config options specified
            in this builder will be applied to the existing SparkSession.

            >>> s2 = SparkSession.builder.config("k2", "v2").getOrCreate()
            >>> s1.conf.get("k1") == s2.conf.get("k1")
            True
            >>> s1.conf.get("k2") == s2.conf.get("k2")
            True
            """
            with self._lock:
                from pyspark.context import SparkContext
                from pyspark.conf import SparkConf
                session = SparkSession._instantiatedSession
                if session is None or session._sc._jsc is None:
                    sparkConf = SparkConf()
                    for key, value in self._options.items():
                        sparkConf.set(key, value)
                    sc = SparkContext.getOrCreate(sparkConf)
                    # This SparkContext may be an existing one.
                    for key, value in self._options.items():
                        # we need to propagate the confs
                        # before we create the SparkSession. Otherwise, confs like
                        # warehouse path and metastore url will not be set correctly (
                        # these confs cannot be changed once the SparkSession is created).
                        sc._conf.set(key, value)
                    session = SparkSession(sc)
                for key, value in self._options.items():
                    session._jsparkSession.sessionState().conf().setConfString(key, value)
                for key, value in self._options.items():
                    session.sparkContext._conf.set(key, value)
                return session

    builder = Builder()
    """A class attribute having a :class:`Builder` to construct :class:`SparkSession` instances"""

    _instantiatedSession = None

    @ignore_unicode_prefix
    def __init__(self, sparkContext, jsparkSession=None):
        """Creates a new SparkSession.

        >>> from datetime import datetime
        >>> spark = SparkSession(sc)
        >>> allTypes = sc.parallelize([Row(i=1, s="string", d=1.0, l=1,
        ...     b=True, list=[1, 2, 3], dict={"s": 0}, row=Row(a=1),
        ...     time=datetime(2014, 8, 1, 14, 1, 5))])
        >>> df = allTypes.toDF()
        >>> df.createOrReplaceTempView("allTypes")
        >>> spark.sql('select i+1, d+1, not b, list[1], dict["s"], time, row.a '
        ...            'from allTypes where b and i > 0').collect()
        [Row((i + CAST(1 AS BIGINT))=2, (d + CAST(1 AS DOUBLE))=2.0, (NOT b)=False, list[1]=2, \
            dict[s]=0, time=datetime.datetime(2014, 8, 1, 14, 1, 5), a=1)]
        >>> df.rdd.map(lambda x: (x.i, x.s, x.d, x.l, x.b, x.time, x.row.a, x.list)).collect()
        [(1, u'string', 1.0, 1, True, datetime.datetime(2014, 8, 1, 14, 1, 5), 1, [1, 2, 3])]
        """
        from pyspark.sql.context import SQLContext
        self._sc = sparkContext
        self._jsc = self._sc._jsc
        self._jvm = self._sc._jvm
        if jsparkSession is None:
            jsparkSession = self._jvm.SparkSession.builder().getOrCreate()
        self._jsparkSession = jsparkSession
        self._jwrapped = self._jsparkSession.sqlContext()
        self._wrapped = SQLContext(self._sc, self, self._jwrapped)
        _monkey_patch_RDD(self)
        install_exception_handler()
        # If we had an instantiated SparkSession attached with a SparkContext
        # which is stopped now, we need to renew the instantiated SparkSession.
        # Otherwise, we will use invalid SparkSession when we call Builder.getOrCreate.
        if SparkSession._instantiatedSession is None \
                or SparkSession._instantiatedSession._sc._jsc is None:
            SparkSession._instantiatedSession = self

    def _repr_html_(self):
        return """
            <div>
                <p><b>SparkSession - {catalogImplementation}</b></p>
                {sc_HTML}
            </div>
        """.format(
            catalogImplementation=self.conf.get("spark.sql.catalogImplementation"),
            sc_HTML=self.sparkContext._repr_html_()
        )

    @since(2.0)
    def newSession(self):
        """
        Returns a new SparkSession as new session, that has separate SQLConf,
        registered temporary views and UDFs, but shared SparkContext and
        table cache.
        """
        return self.__class__(self._sc, self._jsparkSession.newSession())

    @property
    @since(2.0)
    def sparkContext(self):
        """Returns the underlying :class:`SparkContext`."""
        return self._sc

    @property
    @since(2.0)
    def version(self):
        """The version of Spark on which this application is running."""
        return self._jsparkSession.version()

    @property
    @since(2.0)
    def conf(self):
        """Runtime configuration interface for Spark.

        This is the interface through which the user can get and set all Spark and Hadoop
        configurations that are relevant to Spark SQL. When getting the value of a config,
        this defaults to the value set in the underlying :class:`SparkContext`, if any.
        """
        if not hasattr(self, "_conf"):
            self._conf = RuntimeConfig(self._jsparkSession.conf())
        return self._conf

    @property
    @since(2.0)
    def catalog(self):
        """Interface through which the user may create, drop, alter or query underlying
        databases, tables, functions etc.

        :return: :class:`Catalog`
        """
        from pyspark.sql.catalog import Catalog
        if not hasattr(self, "_catalog"):
            self._catalog = Catalog(self)
        return self._catalog

    @property
    @since(2.0)
    def udf(self):
        """Returns a :class:`UDFRegistration` for UDF registration.

        :return: :class:`UDFRegistration`
        """
        from pyspark.sql.udf import UDFRegistration
        return UDFRegistration(self)

    @since(2.0)
    def range(self, start, end=None, step=1, numPartitions=None):
        """
        Create a :class:`DataFrame` with single :class:`pyspark.sql.types.LongType` column named
        ``id``, containing elements in a range from ``start`` to ``end`` (exclusive) with
        step value ``step``.

        :param start: the start value
        :param end: the end value (exclusive)
        :param step: the incremental step (default: 1)
        :param numPartitions: the number of partitions of the DataFrame
        :return: :class:`DataFrame`

        >>> spark.range(1, 7, 2).collect()
        [Row(id=1), Row(id=3), Row(id=5)]

        If only one argument is specified, it will be used as the end value.

        >>> spark.range(3).collect()
        [Row(id=0), Row(id=1), Row(id=2)]
        """
        if numPartitions is None:
            numPartitions = self._sc.defaultParallelism

        if end is None:
            jdf = self._jsparkSession.range(0, int(start), int(step), int(numPartitions))
        else:
            jdf = self._jsparkSession.range(int(start), int(end), int(step), int(numPartitions))

        return DataFrame(jdf, self._wrapped)

    def _inferSchemaFromList(self, data, names=None):
        """
        Infer schema from list of Row or tuple.

        :param data: list of Row or tuple
        :param names: list of column names
        :return: :class:`pyspark.sql.types.StructType`
        """
        if not data:
            raise ValueError("can not infer schema from empty dataset")
        first = data[0]
        if type(first) is dict:
            warnings.warn("inferring schema from dict is deprecated,"
                          "please use pyspark.sql.Row instead")
        schema = reduce(_merge_type, (_infer_schema(row, names) for row in data))
        if _has_nulltype(schema):
            raise ValueError("Some of types cannot be determined after inferring")
        return schema

    def _inferSchema(self, rdd, samplingRatio=None, names=None):
        """
        Infer schema from an RDD of Row or tuple.

        :param rdd: an RDD of Row or tuple
        :param samplingRatio: sampling ratio, or no sampling (default)
        :return: :class:`pyspark.sql.types.StructType`
        """
        first = rdd.first()
        if not first:
            raise ValueError("The first row in RDD is empty, "
                             "can not infer schema")
        if type(first) is dict:
            warnings.warn("Using RDD of dict to inferSchema is deprecated. "
                          "Use pyspark.sql.Row instead")

        if samplingRatio is None:
            schema = _infer_schema(first, names=names)
            if _has_nulltype(schema):
                for row in rdd.take(100)[1:]:
                    schema = _merge_type(schema, _infer_schema(row, names=names))
                    if not _has_nulltype(schema):
                        break
                else:
                    raise ValueError("Some of types cannot be determined by the "
                                     "first 100 rows, please try again with sampling")
        else:
            if samplingRatio < 0.99:
                rdd = rdd.sample(False, float(samplingRatio))
            schema = rdd.map(lambda row: _infer_schema(row, names)).reduce(_merge_type)
        return schema

    def _createFromRDD(self, rdd, schema, samplingRatio):
        """
        Create an RDD for DataFrame from an existing RDD, returns the RDD and schema.
        """
        if schema is None or isinstance(schema, (list, tuple)):
            struct = self._inferSchema(rdd, samplingRatio, names=schema)
            converter = _create_converter(struct)
            rdd = rdd.map(converter)
            if isinstance(schema, (list, tuple)):
                for i, name in enumerate(schema):
                    struct.fields[i].name = name
                    struct.names[i] = name
            schema = struct

        elif not isinstance(schema, StructType):
            raise TypeError("schema should be StructType or list or None, but got: %s" % schema)

        # convert python objects to sql data
        rdd = rdd.map(schema.toInternal)
        return rdd, schema

    def _createFromLocal(self, data, schema):
        """
        Create an RDD for DataFrame from a list or pandas.DataFrame, returns
        the RDD and schema.
        """
        # make sure data could consumed multiple times
        if not isinstance(data, list):
            data = list(data)

        if schema is None or isinstance(schema, (list, tuple)):
            struct = self._inferSchemaFromList(data, names=schema)
            converter = _create_converter(struct)
            data = map(converter, data)
            if isinstance(schema, (list, tuple)):
                for i, name in enumerate(schema):
                    struct.fields[i].name = name
                    struct.names[i] = name
            schema = struct

        elif not isinstance(schema, StructType):
            raise TypeError("schema should be StructType or list or None, but got: %s" % schema)

        # convert python objects to sql data
        data = [schema.toInternal(row) for row in data]
        return self._sc.parallelize(data), schema

    def _get_numpy_record_dtype(self, rec):
        """
        Used when converting a pandas.DataFrame to Spark using to_records(), this will correct
        the dtypes of fields in a record so they can be properly loaded into Spark.
        :param rec: a numpy record to check field dtypes
        :return corrected dtype for a numpy.record or None if no correction needed
        """
        import numpy as np
        cur_dtypes = rec.dtype
        col_names = cur_dtypes.names
        record_type_list = []
        has_rec_fix = False
        for i in xrange(len(cur_dtypes)):
            curr_type = cur_dtypes[i]
            # If type is a datetime64 timestamp, convert to microseconds
            # NOTE: if dtype is datetime[ns] then np.record.tolist() will output values as longs,
            # conversion from [us] or lower will lead to py datetime objects, see SPARK-22417
            if curr_type == np.dtype('datetime64[ns]'):
                curr_type = 'datetime64[us]'
                has_rec_fix = True
            record_type_list.append((str(col_names[i]), curr_type))
        return np.dtype(record_type_list) if has_rec_fix else None

    def _convert_from_pandas(self, pdf, schema, timezone):
        """
         Convert a pandas.DataFrame to list of records that can be used to make a DataFrame
         :return list of records
        """
        if timezone is not None:
            from pyspark.sql.types import _check_series_convert_timestamps_tz_local
            copied = False
            if isinstance(schema, StructType):
                for field in schema:
                    # TODO: handle nested timestamps, such as ArrayType(TimestampType())?
                    if isinstance(field.dataType, TimestampType):
                        s = _check_series_convert_timestamps_tz_local(pdf[field.name], timezone)
                        if s is not pdf[field.name]:
                            if not copied:
                                # Copy once if the series is modified to prevent the original
                                # Pandas DataFrame from being updated
                                pdf = pdf.copy()
                                copied = True
                            pdf[field.name] = s
            else:
                for column, series in pdf.iteritems():
                    s = _check_series_convert_timestamps_tz_local(series, timezone)
                    if s is not series:
                        if not copied:
                            # Copy once if the series is modified to prevent the original
                            # Pandas DataFrame from being updated
                            pdf = pdf.copy()
                            copied = True
                        pdf[column] = s

        # Convert pandas.DataFrame to list of numpy records
        np_records = pdf.to_records(index=False)

        # Check if any columns need to be fixed for Spark to infer properly
        if len(np_records) > 0:
            record_dtype = self._get_numpy_record_dtype(np_records[0])
            if record_dtype is not None:
                return [r.astype(record_dtype).tolist() for r in np_records]

        # Convert list of numpy records to python lists
        return [r.tolist() for r in np_records]

    def _create_from_pandas_with_arrow(self, pdf, schema, timezone):
        """
        Create a DataFrame from a given pandas.DataFrame by slicing it into partitions, converting
        to Arrow data, then sending to the JVM to parallelize. If a schema is passed in, the
        data types will be used to coerce the data in Pandas to Arrow conversion.
        """
        from pyspark.serializers import ArrowSerializer, _create_batch
        from pyspark.sql.types import from_arrow_schema, to_arrow_type, TimestampType
        from pyspark.sql.utils import require_minimum_pandas_version, \
            require_minimum_pyarrow_version

        require_minimum_pandas_version()
        require_minimum_pyarrow_version()

        from pandas.api.types import is_datetime64_dtype, is_datetime64tz_dtype

        # Determine arrow types to coerce data when creating batches
        if isinstance(schema, StructType):
            arrow_types = [to_arrow_type(f.dataType) for f in schema.fields]
        elif isinstance(schema, DataType):
            raise ValueError("Single data type %s is not supported with Arrow" % str(schema))
        else:
            # Any timestamps must be coerced to be compatible with Spark
            arrow_types = [to_arrow_type(TimestampType())
                           if is_datetime64_dtype(t) or is_datetime64tz_dtype(t) else None
                           for t in pdf.dtypes]

        # Slice the DataFrame to be batched
        step = -(-len(pdf) // self.sparkContext.defaultParallelism)  # round int up
        pdf_slices = (pdf[start:start + step] for start in xrange(0, len(pdf), step))

        # Create Arrow record batches
        batches = [_create_batch([(c, t) for (_, c), t in zip(pdf_slice.iteritems(), arrow_types)],
                                 timezone)
                   for pdf_slice in pdf_slices]

        # Create the Spark schema from the first Arrow batch (always at least 1 batch after slicing)
        if isinstance(schema, (list, tuple)):
            struct = from_arrow_schema(batches[0].schema)
            for i, name in enumerate(schema):
                struct.fields[i].name = name
                struct.names[i] = name
            schema = struct

        # Create the Spark DataFrame directly from the Arrow data and schema
        jrdd = self._sc._serialize_to_jvm(batches, len(batches), ArrowSerializer())
        jdf = self._jvm.PythonSQLUtils.arrowPayloadToDataFrame(
            jrdd, schema.json(), self._wrapped._jsqlContext)
        df = DataFrame(jdf, self._wrapped)
        df._schema = schema
        return df

    @since(2.0)
    @ignore_unicode_prefix
    def createDataFrame(self, data, schema=None, samplingRatio=None, verifySchema=True):
        """
        Creates a :class:`DataFrame` from an :class:`RDD`, a list or a :class:`pandas.DataFrame`.

        When ``schema`` is a list of column names, the type of each column
        will be inferred from ``data``.

        When ``schema`` is ``None``, it will try to infer the schema (column names and types)
        from ``data``, which should be an RDD of :class:`Row`,
        or :class:`namedtuple`, or :class:`dict`.

        When ``schema`` is :class:`pyspark.sql.types.DataType` or a datatype string, it must match
        the real data, or an exception will be thrown at runtime. If the given schema is not
        :class:`pyspark.sql.types.StructType`, it will be wrapped into a
        :class:`pyspark.sql.types.StructType` as its only field, and the field name will be "value",
        each record will also be wrapped into a tuple, which can be converted to row later.

        If schema inference is needed, ``samplingRatio`` is used to determined the ratio of
        rows used for schema inference. The first row will be used if ``samplingRatio`` is ``None``.

        :param data: an RDD of any kind of SQL data representation(e.g. row, tuple, int, boolean,
            etc.), or :class:`list`, or :class:`pandas.DataFrame`.
        :param schema: a :class:`pyspark.sql.types.DataType` or a datatype string or a list of
            column names, default is ``None``.  The data type string format equals to
            :class:`pyspark.sql.types.DataType.simpleString`, except that top level struct type can
            omit the ``struct<>`` and atomic types use ``typeName()`` as their format, e.g. use
            ``byte`` instead of ``tinyint`` for :class:`pyspark.sql.types.ByteType`. We can also use
            ``int`` as a short name for ``IntegerType``.
        :param samplingRatio: the sample ratio of rows used for inferring
        :param verifySchema: verify data types of every row against schema.
        :return: :class:`DataFrame`

        .. versionchanged:: 2.1
           Added verifySchema.

        >>> l = [('Alice', 1)]
        >>> spark.createDataFrame(l).collect()
        [Row(_1=u'Alice', _2=1)]
        >>> spark.createDataFrame(l, ['name', 'age']).collect()
        [Row(name=u'Alice', age=1)]

        >>> d = [{'name': 'Alice', 'age': 1}]
        >>> spark.createDataFrame(d).collect()
        [Row(age=1, name=u'Alice')]

        >>> rdd = sc.parallelize(l)
        >>> spark.createDataFrame(rdd).collect()
        [Row(_1=u'Alice', _2=1)]
        >>> df = spark.createDataFrame(rdd, ['name', 'age'])
        >>> df.collect()
        [Row(name=u'Alice', age=1)]

        >>> from pyspark.sql import Row
        >>> Person = Row('name', 'age')
        >>> person = rdd.map(lambda r: Person(*r))
        >>> df2 = spark.createDataFrame(person)
        >>> df2.collect()
        [Row(name=u'Alice', age=1)]

        >>> from pyspark.sql.types import *
        >>> schema = StructType([
        ...    StructField("name", StringType(), True),
        ...    StructField("age", IntegerType(), True)])
        >>> df3 = spark.createDataFrame(rdd, schema)
        >>> df3.collect()
        [Row(name=u'Alice', age=1)]

        >>> spark.createDataFrame(df.toPandas()).collect()  # doctest: +SKIP
        [Row(name=u'Alice', age=1)]
        >>> spark.createDataFrame(pandas.DataFrame([[1, 2]])).collect()  # doctest: +SKIP
        [Row(0=1, 1=2)]

        >>> spark.createDataFrame(rdd, "a: string, b: int").collect()
        [Row(a=u'Alice', b=1)]
        >>> rdd = rdd.map(lambda row: row[1])
        >>> spark.createDataFrame(rdd, "int").collect()
        [Row(value=1)]
        >>> spark.createDataFrame(rdd, "boolean").collect() # doctest: +IGNORE_EXCEPTION_DETAIL
        Traceback (most recent call last):
            ...
        Py4JJavaError: ...
        """
        if isinstance(data, DataFrame):
            raise TypeError("data is already a DataFrame")

        if isinstance(schema, basestring):
            schema = _parse_datatype_string(schema)
        elif isinstance(schema, (list, tuple)):
            # Must re-encode any unicode strings to be consistent with StructField names
            schema = [x.encode('utf-8') if not isinstance(x, str) else x for x in schema]

        try:
            import pandas
            has_pandas = True
        except Exception:
            has_pandas = False
        if has_pandas and isinstance(data, pandas.DataFrame):
            from pyspark.sql.utils import require_minimum_pandas_version
            require_minimum_pandas_version()

            if self.conf.get("spark.sql.execution.pandas.respectSessionTimeZone").lower() \
               == "true":
                timezone = self.conf.get("spark.sql.session.timeZone")
            else:
                timezone = None

            # If no schema supplied by user then get the names of columns only
            if schema is None:
                schema = [str(x) if not isinstance(x, basestring) else
                          (x.encode('utf-8') if not isinstance(x, str) else x)
                          for x in data.columns]

            if self.conf.get("spark.sql.execution.arrow.enabled", "false").lower() == "true" \
                    and len(data) > 0:
                try:
                    return self._create_from_pandas_with_arrow(data, schema, timezone)
                except Exception as e:
                    warnings.warn("Arrow will not be used in createDataFrame: %s" % str(e))
                    # Fallback to create DataFrame without arrow if raise some exception
            data = self._convert_from_pandas(data, schema, timezone)

        if isinstance(schema, StructType):
            verify_func = _make_type_verifier(schema) if verifySchema else lambda _: True

            def prepare(obj):
                verify_func(obj)
                return obj
        elif isinstance(schema, DataType):
            dataType = schema
            schema = StructType().add("value", schema)

            verify_func = _make_type_verifier(
                dataType, name="field value") if verifySchema else lambda _: True

            def prepare(obj):
                verify_func(obj)
                return obj,
        else:
            prepare = lambda obj: obj

        if isinstance(data, RDD):
            rdd, schema = self._createFromRDD(data.map(prepare), schema, samplingRatio)
        else:
            rdd, schema = self._createFromLocal(map(prepare, data), schema)
        jrdd = self._jvm.SerDeUtil.toJavaArray(rdd._to_java_object_rdd())
        jdf = self._jsparkSession.applySchemaToPythonRDD(jrdd.rdd(), schema.json())
        df = DataFrame(jdf, self._wrapped)
        df._schema = schema
        return df

    @ignore_unicode_prefix
    @since(2.0)
    def sql(self, sqlQuery):
        """Returns a :class:`DataFrame` representing the result of the given query.

        :return: :class:`DataFrame`

        >>> df.createOrReplaceTempView("table1")
        >>> df2 = spark.sql("SELECT field1 AS f1, field2 as f2 from table1")
        >>> df2.collect()
        [Row(f1=1, f2=u'row1'), Row(f1=2, f2=u'row2'), Row(f1=3, f2=u'row3')]
        """
        return DataFrame(self._jsparkSession.sql(sqlQuery), self._wrapped)

    @since(2.0)
    def table(self, tableName):
        """Returns the specified table as a :class:`DataFrame`.

        :return: :class:`DataFrame`

        >>> df.createOrReplaceTempView("table1")
        >>> df2 = spark.table("table1")
        >>> sorted(df.collect()) == sorted(df2.collect())
        True
        """
        return DataFrame(self._jsparkSession.table(tableName), self._wrapped)

    @property
    @since(2.0)
    def read(self):
        """
        Returns a :class:`DataFrameReader` that can be used to read data
        in as a :class:`DataFrame`.

        :return: :class:`DataFrameReader`
        """
        return DataFrameReader(self._wrapped)

    @property
    @since(2.0)
    def readStream(self):
        """
        Returns a :class:`DataStreamReader` that can be used to read data streams
        as a streaming :class:`DataFrame`.

        .. note:: Evolving.

        :return: :class:`DataStreamReader`
        """
        return DataStreamReader(self._wrapped)

    @property
    @since(2.0)
    def streams(self):
        """Returns a :class:`StreamingQueryManager` that allows managing all the
        :class:`StreamingQuery` StreamingQueries active on `this` context.

        .. note:: Evolving.

        :return: :class:`StreamingQueryManager`
        """
        from pyspark.sql.streaming import StreamingQueryManager
        return StreamingQueryManager(self._jsparkSession.streams())

    @since(2.0)
    def stop(self):
        """Stop the underlying :class:`SparkContext`.
        """
        self._sc.stop()
        SparkSession._instantiatedSession = None

    @since(2.0)
    def __enter__(self):
        """
        Enable 'with SparkSession.builder.(...).getOrCreate() as session: app' syntax.
        """
        return self

    @since(2.0)
    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Enable 'with SparkSession.builder.(...).getOrCreate() as session: app' syntax.

        Specifically stop the SparkSession on exit of the with block.
        """
        self.stop()


def _test():
    import os
    import doctest
    from pyspark.context import SparkContext
    from pyspark.sql import Row
    import pyspark.sql.session

    os.chdir(os.environ["SPARK_HOME"])

    globs = pyspark.sql.session.__dict__.copy()
    sc = SparkContext('local[4]', 'PythonTest')
    globs['sc'] = sc
    globs['spark'] = SparkSession(sc)
    globs['rdd'] = rdd = sc.parallelize(
        [Row(field1=1, field2="row1"),
         Row(field1=2, field2="row2"),
         Row(field1=3, field2="row3")])
    globs['df'] = rdd.toDF()
    (failure_count, test_count) = doctest.testmod(
        pyspark.sql.session, globs=globs,
        optionflags=doctest.ELLIPSIS | doctest.NORMALIZE_WHITESPACE)
    globs['sc'].stop()
    if failure_count:
        exit(-1)

if __name__ == "__main__":
    _test()
