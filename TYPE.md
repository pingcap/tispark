1. How many types in TiDB

int -> TypeLong -> IntegerType
varchar -> TypeVarchar -> BytesType
datetime -> TypeDatetime -> DatetimeType
blob -> TypeBlob -> RawBytesType
binary -> TypeString -> BytesType
date -> TypeDate -> DateType
timestamp -> TypeTimestamp -> TimestampType
year -> TypeYear -> IntegerType
bigint -> TypeLonglong -> IntegerType
decimal -> TypeNewDecimal -> DecimalType
double -> TypeDouble -> RealType
float -> TypeFloat -> RealType
mediumint -> TypeInt24 -> IntegerType
real -> TypeDouble -> RealType
smallint -> TypeShort -> IntegerType
tinyint -> TypeTiny -> IntegerType
char -> TypeString -> BytesType
longtext -> TypeLongBlob -> RawBytesType
mediumtext -> TypeMediumBlob -> RawBytesType
text -> TypeBlob -> RawBytesType
tinyblob -> TypeTinyBlob -> RawBytesType
bit -> TypeBit -> BitType
time -> TypeDuration -> TimestampType
enum -> TypeEnum -> ?
set -> TypeSet -> ?

Place need type? 
1. Compare
	a. form range
2. encoding to coprocessor
	a. expression
	b. key in index
3. decode from coprocessor

constrain infer -> 
1. range
2. unsigned?

Value(Bytes, Type)

ranger.go:buildIndexRange expr -> range; datum.go:ConvertTo from mysql type to datum

+---------------+-----------------------+------+------+-------------------+-------+
| Field         | Type                  | Null | Key  | Default           | Extra |
+---------------+-----------------------+------+------+-------------------+-------+
| id_dt         | int(11)               | NO   | PRI  | NULL              |       |
| tp_varchar    | varchar(45)           | YES  | MUL  | NULL              |       |
| tp_datetime   | datetime              | YES  | MUL  | CURRENT_TIMESTAMP |       |
| tp_blob       | blob                  | YES  | MUL  | NULL              |       |
| tp_binary     | binary(2)             | YES  |      | NULL              |       |
| tp_date       | date                  | YES  | MUL  | NULL              |       |
| tp_timestamp  | timestamp             | NO   | MUL  | CURRENT_TIMESTAMP |       |
| tp_year       | year UNSIGNED         | YES  | MUL  | NULL              |       |
| tp_bigint     | bigint(20)            | YES  | MUL  | NULL              |       |
| tp_decimal    | decimal               | YES  | MUL  | NULL              |       |
| tp_double     | double                | YES  | MUL  | NULL              |       |
| tp_float      | float                 | YES  | MUL  | NULL              |       |
| tp_int        | int(11)               | YES  | MUL  | NULL              |       |
| tp_mediumint  | mediumint(9)          | YES  | MUL  | NULL              |       |
| tp_real       | double                | YES  | MUL  | NULL              |       |
| tp_smallint   | smallint(6)           | YES  | MUL  | NULL              |       |
| tp_tinyint    | tinyint(4)            | YES  | MUL  | NULL              |       |
| tp_char       | char(10)              | YES  | MUL  | NULL              |       |
| tp_nvarchar   | varchar(40)           | YES  | MUL  | NULL              |       |
| tp_longtext   | longtext              | YES  | MUL  | NULL              |       |
| tp_mediumtext | mediumtext            | YES  | MUL  | NULL              |       |
| tp_text       | text                  | YES  | MUL  | NULL              |       |
| tp_tinytext   | tinytext              | YES  | MUL  | NULL              |       |
| tp_bit        | bit(1)                | YES  | MUL  | NULL              |       |
| tp_time       | time                  | YES  | MUL  | NULL              |       |
| tp_enum       | enum('1','2','3','4') | YES  | MUL  | NULL              |       |
| tp_set        | set('a','b','c','d')  | YES  | MUL  | NULL              |       |
+---------------+-----------------------+------+------+-------------------+-------+
