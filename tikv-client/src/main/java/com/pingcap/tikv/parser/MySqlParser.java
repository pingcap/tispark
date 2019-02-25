// Generated from MySqlParser.g4 by ANTLR 4.7.1
package com.pingcap.tikv.parser;

import java.util.List;
import org.antlr.v4.runtime.*;
import org.antlr.v4.runtime.atn.*;
import org.antlr.v4.runtime.dfa.DFA;
import org.antlr.v4.runtime.misc.*;
import org.antlr.v4.runtime.tree.*;

@SuppressWarnings({"all", "warnings", "unchecked", "unused", "cast"})
public class MySqlParser extends Parser {
  static {
    RuntimeMetaData.checkVersion("4.7.1", RuntimeMetaData.VERSION);
  }

  protected static final DFA[] _decisionToDFA;
  protected static final PredictionContextCache _sharedContextCache = new PredictionContextCache();
  public static final int SPACE = 1,
      SPEC_MYSQL_COMMENT = 2,
      COMMENT_INPUT = 3,
      LINE_COMMENT = 4,
      ADD = 5,
      ALL = 6,
      ALTER = 7,
      ALWAYS = 8,
      ANALYZE = 9,
      AND = 10,
      AS = 11,
      ASC = 12,
      BEFORE = 13,
      BETWEEN = 14,
      BOTH = 15,
      BY = 16,
      CALL = 17,
      CASCADE = 18,
      CASE = 19,
      CAST = 20,
      CHANGE = 21,
      CHARACTER = 22,
      CHECK = 23,
      COLLATE = 24,
      COLUMN = 25,
      CONDITION = 26,
      CONSTRAINT = 27,
      CONTINUE = 28,
      CONVERT = 29,
      CREATE = 30,
      CROSS = 31,
      CURRENT_USER = 32,
      CURSOR = 33,
      DATABASE = 34,
      DATABASES = 35,
      DECLARE = 36,
      DEFAULT = 37,
      DELAYED = 38,
      DELETE = 39,
      DESC = 40,
      DESCRIBE = 41,
      DETERMINISTIC = 42,
      DISTINCT = 43,
      DISTINCTROW = 44,
      DROP = 45,
      EACH = 46,
      ELSE = 47,
      ELSEIF = 48,
      ENCLOSED = 49,
      ESCAPED = 50,
      EXISTS = 51,
      EXIT = 52,
      EXPLAIN = 53,
      FALSE = 54,
      FETCH = 55,
      FOR = 56,
      FORCE = 57,
      FOREIGN = 58,
      FROM = 59,
      FULLTEXT = 60,
      GENERATED = 61,
      GRANT = 62,
      GROUP = 63,
      HAVING = 64,
      HIGH_PRIORITY = 65,
      IF = 66,
      IGNORE = 67,
      IN = 68,
      INDEX = 69,
      INFILE = 70,
      INNER = 71,
      INOUT = 72,
      INSERT = 73,
      INTERVAL = 74,
      INTO = 75,
      IS = 76,
      ITERATE = 77,
      JOIN = 78,
      KEY = 79,
      KEYS = 80,
      KILL = 81,
      LEADING = 82,
      LEAVE = 83,
      LEFT = 84,
      LIKE = 85,
      LIMIT = 86,
      LINEAR = 87,
      LINES = 88,
      LOAD = 89,
      LOCK = 90,
      LOOP = 91,
      LOW_PRIORITY = 92,
      MASTER_BIND = 93,
      MASTER_SSL_VERIFY_SERVER_CERT = 94,
      MATCH = 95,
      MAXVALUE = 96,
      MODIFIES = 97,
      NATURAL = 98,
      NOT = 99,
      NO_WRITE_TO_BINLOG = 100,
      NULL_LITERAL = 101,
      ON = 102,
      OPTIMIZE = 103,
      OPTION = 104,
      OPTIONALLY = 105,
      OR = 106,
      ORDER = 107,
      OUT = 108,
      OUTER = 109,
      OUTFILE = 110,
      PARTITION = 111,
      PRIMARY = 112,
      PROCEDURE = 113,
      PURGE = 114,
      RANGE = 115,
      READ = 116,
      READS = 117,
      REFERENCES = 118,
      REGEXP = 119,
      RELEASE = 120,
      RENAME = 121,
      REPEAT = 122,
      REPLACE = 123,
      REQUIRE = 124,
      RESTRICT = 125,
      RETURN = 126,
      REVOKE = 127,
      RIGHT = 128,
      RLIKE = 129,
      SCHEMA = 130,
      SCHEMAS = 131,
      SELECT = 132,
      SET = 133,
      SEPARATOR = 134,
      SHOW = 135,
      SPATIAL = 136,
      SQL = 137,
      SQLEXCEPTION = 138,
      SQLSTATE = 139,
      SQLWARNING = 140,
      SQL_BIG_RESULT = 141,
      SQL_CALC_FOUND_ROWS = 142,
      SQL_SMALL_RESULT = 143,
      SSL = 144,
      STARTING = 145,
      STRAIGHT_JOIN = 146,
      TABLE = 147,
      TERMINATED = 148,
      THEN = 149,
      TO = 150,
      TRAILING = 151,
      TRIGGER = 152,
      TRUE = 153,
      UNDO = 154,
      UNION = 155,
      UNIQUE = 156,
      UNLOCK = 157,
      UNSIGNED = 158,
      UPDATE = 159,
      USAGE = 160,
      USE = 161,
      USING = 162,
      VALUES = 163,
      WHEN = 164,
      WHERE = 165,
      WHILE = 166,
      WITH = 167,
      WRITE = 168,
      XOR = 169,
      ZEROFILL = 170,
      TINYINT = 171,
      SMALLINT = 172,
      MEDIUMINT = 173,
      INT = 174,
      INTEGER = 175,
      BIGINT = 176,
      REAL = 177,
      DOUBLE = 178,
      FLOAT = 179,
      DECIMAL = 180,
      NUMERIC = 181,
      DATE = 182,
      TIME = 183,
      TIMESTAMP = 184,
      DATETIME = 185,
      YEAR = 186,
      CHAR = 187,
      VARCHAR = 188,
      BINARY = 189,
      VARBINARY = 190,
      TINYBLOB = 191,
      BLOB = 192,
      MEDIUMBLOB = 193,
      LONGBLOB = 194,
      TINYTEXT = 195,
      TEXT = 196,
      MEDIUMTEXT = 197,
      LONGTEXT = 198,
      ENUM = 199,
      YEAR_MONTH = 200,
      DAY_HOUR = 201,
      DAY_MINUTE = 202,
      DAY_SECOND = 203,
      HOUR_MINUTE = 204,
      HOUR_SECOND = 205,
      MINUTE_SECOND = 206,
      SECOND_MICROSECOND = 207,
      MINUTE_MICROSECOND = 208,
      HOUR_MICROSECOND = 209,
      DAY_MICROSECOND = 210,
      AVG = 211,
      BIT_AND = 212,
      BIT_OR = 213,
      BIT_XOR = 214,
      COUNT = 215,
      GROUP_CONCAT = 216,
      MAX = 217,
      MIN = 218,
      STD = 219,
      STDDEV = 220,
      STDDEV_POP = 221,
      STDDEV_SAMP = 222,
      SUM = 223,
      VAR_POP = 224,
      VAR_SAMP = 225,
      VARIANCE = 226,
      CURRENT_DATE = 227,
      CURRENT_TIME = 228,
      CURRENT_TIMESTAMP = 229,
      LOCALTIME = 230,
      CURDATE = 231,
      CURTIME = 232,
      DATE_ADD = 233,
      DATE_SUB = 234,
      EXTRACT = 235,
      LOCALTIMESTAMP = 236,
      NOW = 237,
      POSITION = 238,
      SUBSTR = 239,
      SUBSTRING = 240,
      SYSDATE = 241,
      TRIM = 242,
      UTC_DATE = 243,
      UTC_TIME = 244,
      UTC_TIMESTAMP = 245,
      ACCOUNT = 246,
      ACTION = 247,
      AFTER = 248,
      AGGREGATE = 249,
      ALGORITHM = 250,
      ANY = 251,
      AT = 252,
      AUTHORS = 253,
      AUTOCOMMIT = 254,
      AUTOEXTEND_SIZE = 255,
      AUTO_INCREMENT = 256,
      AVG_ROW_LENGTH = 257,
      BEGIN = 258,
      BINLOG = 259,
      BIT = 260,
      BLOCK = 261,
      BOOL = 262,
      BOOLEAN = 263,
      BTREE = 264,
      CACHE = 265,
      CASCADED = 266,
      CHAIN = 267,
      CHANGED = 268,
      CHANNEL = 269,
      CHECKSUM = 270,
      CIPHER = 271,
      CLIENT = 272,
      CLOSE = 273,
      COALESCE = 274,
      CODE = 275,
      COLUMNS = 276,
      COLUMN_FORMAT = 277,
      COMMENT = 278,
      COMMIT = 279,
      COMPACT = 280,
      COMPLETION = 281,
      COMPRESSED = 282,
      COMPRESSION = 283,
      CONCURRENT = 284,
      CONNECTION = 285,
      CONSISTENT = 286,
      CONTAINS = 287,
      CONTEXT = 288,
      CONTRIBUTORS = 289,
      COPY = 290,
      CPU = 291,
      DATA = 292,
      DATAFILE = 293,
      DEALLOCATE = 294,
      DEFAULT_AUTH = 295,
      DEFINER = 296,
      DELAY_KEY_WRITE = 297,
      DES_KEY_FILE = 298,
      DIRECTORY = 299,
      DISABLE = 300,
      DISCARD = 301,
      DISK = 302,
      DO = 303,
      DUMPFILE = 304,
      DUPLICATE = 305,
      DYNAMIC = 306,
      ENABLE = 307,
      ENCRYPTION = 308,
      END = 309,
      ENDS = 310,
      ENGINE = 311,
      ENGINES = 312,
      ERROR = 313,
      ERRORS = 314,
      ESCAPE = 315,
      EVEN = 316,
      EVENT = 317,
      EVENTS = 318,
      EVERY = 319,
      EXCHANGE = 320,
      EXCLUSIVE = 321,
      EXPIRE = 322,
      EXPORT = 323,
      EXTENDED = 324,
      EXTENT_SIZE = 325,
      FAST = 326,
      FAULTS = 327,
      FIELDS = 328,
      FILE_BLOCK_SIZE = 329,
      FILTER = 330,
      FIRST = 331,
      FIXED = 332,
      FLUSH = 333,
      FOLLOWS = 334,
      FOUND = 335,
      FULL = 336,
      FUNCTION = 337,
      GENERAL = 338,
      GLOBAL = 339,
      GRANTS = 340,
      GROUP_REPLICATION = 341,
      HANDLER = 342,
      HASH = 343,
      HELP = 344,
      HOST = 345,
      HOSTS = 346,
      IDENTIFIED = 347,
      IGNORE_SERVER_IDS = 348,
      IMPORT = 349,
      INDEXES = 350,
      INITIAL_SIZE = 351,
      INPLACE = 352,
      INSERT_METHOD = 353,
      INSTALL = 354,
      INSTANCE = 355,
      INVOKER = 356,
      IO = 357,
      IO_THREAD = 358,
      IPC = 359,
      ISOLATION = 360,
      ISSUER = 361,
      JSON = 362,
      KEY_BLOCK_SIZE = 363,
      LANGUAGE = 364,
      LAST = 365,
      LEAVES = 366,
      LESS = 367,
      LEVEL = 368,
      LIST = 369,
      LOCAL = 370,
      LOGFILE = 371,
      LOGS = 372,
      MASTER = 373,
      MASTER_AUTO_POSITION = 374,
      MASTER_CONNECT_RETRY = 375,
      MASTER_DELAY = 376,
      MASTER_HEARTBEAT_PERIOD = 377,
      MASTER_HOST = 378,
      MASTER_LOG_FILE = 379,
      MASTER_LOG_POS = 380,
      MASTER_PASSWORD = 381,
      MASTER_PORT = 382,
      MASTER_RETRY_COUNT = 383,
      MASTER_SSL = 384,
      MASTER_SSL_CA = 385,
      MASTER_SSL_CAPATH = 386,
      MASTER_SSL_CERT = 387,
      MASTER_SSL_CIPHER = 388,
      MASTER_SSL_CRL = 389,
      MASTER_SSL_CRLPATH = 390,
      MASTER_SSL_KEY = 391,
      MASTER_TLS_VERSION = 392,
      MASTER_USER = 393,
      MAX_CONNECTIONS_PER_HOUR = 394,
      MAX_QUERIES_PER_HOUR = 395,
      MAX_ROWS = 396,
      MAX_SIZE = 397,
      MAX_UPDATES_PER_HOUR = 398,
      MAX_USER_CONNECTIONS = 399,
      MEDIUM = 400,
      MERGE = 401,
      MID = 402,
      MIGRATE = 403,
      MIN_ROWS = 404,
      MODE = 405,
      MODIFY = 406,
      MUTEX = 407,
      MYSQL = 408,
      NAME = 409,
      NAMES = 410,
      NCHAR = 411,
      NEVER = 412,
      NEXT = 413,
      NO = 414,
      NODEGROUP = 415,
      NONE = 416,
      OFFLINE = 417,
      OFFSET = 418,
      OJ = 419,
      OLD_PASSWORD = 420,
      ONE = 421,
      ONLINE = 422,
      ONLY = 423,
      OPEN = 424,
      OPTIMIZER_COSTS = 425,
      OPTIONS = 426,
      OWNER = 427,
      PACK_KEYS = 428,
      PAGE = 429,
      PARSER = 430,
      PARTIAL = 431,
      PARTITIONING = 432,
      PARTITIONS = 433,
      PASSWORD = 434,
      PHASE = 435,
      PLUGIN = 436,
      PLUGIN_DIR = 437,
      PLUGINS = 438,
      PORT = 439,
      PRECEDES = 440,
      PREPARE = 441,
      PRESERVE = 442,
      PREV = 443,
      PROCESSLIST = 444,
      PROFILE = 445,
      PROFILES = 446,
      PROXY = 447,
      QUERY = 448,
      QUICK = 449,
      REBUILD = 450,
      RECOVER = 451,
      REDO_BUFFER_SIZE = 452,
      REDUNDANT = 453,
      RELAY = 454,
      RELAY_LOG_FILE = 455,
      RELAY_LOG_POS = 456,
      RELAYLOG = 457,
      REMOVE = 458,
      REORGANIZE = 459,
      REPAIR = 460,
      REPLICATE_DO_DB = 461,
      REPLICATE_DO_TABLE = 462,
      REPLICATE_IGNORE_DB = 463,
      REPLICATE_IGNORE_TABLE = 464,
      REPLICATE_REWRITE_DB = 465,
      REPLICATE_WILD_DO_TABLE = 466,
      REPLICATE_WILD_IGNORE_TABLE = 467,
      REPLICATION = 468,
      RESET = 469,
      RESUME = 470,
      RETURNS = 471,
      ROLLBACK = 472,
      ROLLUP = 473,
      ROTATE = 474,
      ROW = 475,
      ROWS = 476,
      ROW_FORMAT = 477,
      SAVEPOINT = 478,
      SCHEDULE = 479,
      SECURITY = 480,
      SERVER = 481,
      SESSION = 482,
      SHARE = 483,
      SHARED = 484,
      SIGNED = 485,
      SIMPLE = 486,
      SLAVE = 487,
      SLOW = 488,
      SNAPSHOT = 489,
      SOCKET = 490,
      SOME = 491,
      SONAME = 492,
      SOUNDS = 493,
      SOURCE = 494,
      SQL_AFTER_GTIDS = 495,
      SQL_AFTER_MTS_GAPS = 496,
      SQL_BEFORE_GTIDS = 497,
      SQL_BUFFER_RESULT = 498,
      SQL_CACHE = 499,
      SQL_NO_CACHE = 500,
      SQL_THREAD = 501,
      START = 502,
      STARTS = 503,
      STATS_AUTO_RECALC = 504,
      STATS_PERSISTENT = 505,
      STATS_SAMPLE_PAGES = 506,
      STATUS = 507,
      STOP = 508,
      STORAGE = 509,
      STORED = 510,
      STRING = 511,
      SUBJECT = 512,
      SUBPARTITION = 513,
      SUBPARTITIONS = 514,
      SUSPEND = 515,
      SWAPS = 516,
      SWITCHES = 517,
      TABLESPACE = 518,
      TEMPORARY = 519,
      TEMPTABLE = 520,
      THAN = 521,
      TRADITIONAL = 522,
      TRANSACTION = 523,
      TRIGGERS = 524,
      TRUNCATE = 525,
      UNDEFINED = 526,
      UNDOFILE = 527,
      UNDO_BUFFER_SIZE = 528,
      UNINSTALL = 529,
      UNKNOWN = 530,
      UNTIL = 531,
      UPGRADE = 532,
      USER = 533,
      USE_FRM = 534,
      USER_RESOURCES = 535,
      VALIDATION = 536,
      VALUE = 537,
      VARIABLES = 538,
      VIEW = 539,
      VIRTUAL = 540,
      WAIT = 541,
      WARNINGS = 542,
      WITHOUT = 543,
      WORK = 544,
      WRAPPER = 545,
      X509 = 546,
      XA = 547,
      XML = 548,
      EUR = 549,
      USA = 550,
      JIS = 551,
      ISO = 552,
      INTERNAL = 553,
      QUARTER = 554,
      MONTH = 555,
      DAY = 556,
      HOUR = 557,
      MINUTE = 558,
      WEEK = 559,
      SECOND = 560,
      MICROSECOND = 561,
      TABLES = 562,
      ROUTINE = 563,
      EXECUTE = 564,
      FILE = 565,
      PROCESS = 566,
      RELOAD = 567,
      SHUTDOWN = 568,
      SUPER = 569,
      PRIVILEGES = 570,
      ARMSCII8 = 571,
      ASCII = 572,
      BIG5 = 573,
      CP1250 = 574,
      CP1251 = 575,
      CP1256 = 576,
      CP1257 = 577,
      CP850 = 578,
      CP852 = 579,
      CP866 = 580,
      CP932 = 581,
      DEC8 = 582,
      EUCJPMS = 583,
      EUCKR = 584,
      GB2312 = 585,
      GBK = 586,
      GEOSTD8 = 587,
      GREEK = 588,
      HEBREW = 589,
      HP8 = 590,
      KEYBCS2 = 591,
      KOI8R = 592,
      KOI8U = 593,
      LATIN1 = 594,
      LATIN2 = 595,
      LATIN5 = 596,
      LATIN7 = 597,
      MACCE = 598,
      MACROMAN = 599,
      SJIS = 600,
      SWE7 = 601,
      TIS620 = 602,
      UCS2 = 603,
      UJIS = 604,
      UTF16 = 605,
      UTF16LE = 606,
      UTF32 = 607,
      UTF8 = 608,
      UTF8MB3 = 609,
      UTF8MB4 = 610,
      ARCHIVE = 611,
      BLACKHOLE = 612,
      CSV = 613,
      FEDERATED = 614,
      INNODB = 615,
      MEMORY = 616,
      MRG_MYISAM = 617,
      MYISAM = 618,
      NDB = 619,
      NDBCLUSTER = 620,
      PERFOMANCE_SCHEMA = 621,
      REPEATABLE = 622,
      COMMITTED = 623,
      UNCOMMITTED = 624,
      SERIALIZABLE = 625,
      GEOMETRYCOLLECTION = 626,
      LINESTRING = 627,
      MULTILINESTRING = 628,
      MULTIPOINT = 629,
      MULTIPOLYGON = 630,
      POINT = 631,
      POLYGON = 632,
      ABS = 633,
      ACOS = 634,
      ADDDATE = 635,
      ADDTIME = 636,
      AES_DECRYPT = 637,
      AES_ENCRYPT = 638,
      AREA = 639,
      ASBINARY = 640,
      ASIN = 641,
      ASTEXT = 642,
      ASWKB = 643,
      ASWKT = 644,
      ASYMMETRIC_DECRYPT = 645,
      ASYMMETRIC_DERIVE = 646,
      ASYMMETRIC_ENCRYPT = 647,
      ASYMMETRIC_SIGN = 648,
      ASYMMETRIC_VERIFY = 649,
      ATAN = 650,
      ATAN2 = 651,
      BENCHMARK = 652,
      BIN = 653,
      BIT_COUNT = 654,
      BIT_LENGTH = 655,
      BUFFER = 656,
      CEIL = 657,
      CEILING = 658,
      CENTROID = 659,
      CHARACTER_LENGTH = 660,
      CHARSET = 661,
      CHAR_LENGTH = 662,
      COERCIBILITY = 663,
      COLLATION = 664,
      COMPRESS = 665,
      CONCAT = 666,
      CONCAT_WS = 667,
      CONNECTION_ID = 668,
      CONV = 669,
      CONVERT_TZ = 670,
      COS = 671,
      COT = 672,
      CRC32 = 673,
      CREATE_ASYMMETRIC_PRIV_KEY = 674,
      CREATE_ASYMMETRIC_PUB_KEY = 675,
      CREATE_DH_PARAMETERS = 676,
      CREATE_DIGEST = 677,
      CROSSES = 678,
      DATEDIFF = 679,
      DATE_FORMAT = 680,
      DAYNAME = 681,
      DAYOFMONTH = 682,
      DAYOFWEEK = 683,
      DAYOFYEAR = 684,
      DECODE = 685,
      DEGREES = 686,
      DES_DECRYPT = 687,
      DES_ENCRYPT = 688,
      DIMENSION = 689,
      DISJOINT = 690,
      ELT = 691,
      ENCODE = 692,
      ENCRYPT = 693,
      ENDPOINT = 694,
      ENVELOPE = 695,
      EQUALS = 696,
      EXP = 697,
      EXPORT_SET = 698,
      EXTERIORRING = 699,
      EXTRACTVALUE = 700,
      FIELD = 701,
      FIND_IN_SET = 702,
      FLOOR = 703,
      FORMAT = 704,
      FOUND_ROWS = 705,
      FROM_BASE64 = 706,
      FROM_DAYS = 707,
      FROM_UNIXTIME = 708,
      GEOMCOLLFROMTEXT = 709,
      GEOMCOLLFROMWKB = 710,
      GEOMETRYCOLLECTIONFROMTEXT = 711,
      GEOMETRYCOLLECTIONFROMWKB = 712,
      GEOMETRYFROMTEXT = 713,
      GEOMETRYFROMWKB = 714,
      GEOMETRYN = 715,
      GEOMETRYTYPE = 716,
      GEOMFROMTEXT = 717,
      GEOMFROMWKB = 718,
      GET_FORMAT = 719,
      GET_LOCK = 720,
      GLENGTH = 721,
      GREATEST = 722,
      GTID_SUBSET = 723,
      GTID_SUBTRACT = 724,
      HEX = 725,
      IFNULL = 726,
      INET6_ATON = 727,
      INET6_NTOA = 728,
      INET_ATON = 729,
      INET_NTOA = 730,
      INSTR = 731,
      INTERIORRINGN = 732,
      INTERSECTS = 733,
      ISCLOSED = 734,
      ISEMPTY = 735,
      ISNULL = 736,
      ISSIMPLE = 737,
      IS_FREE_LOCK = 738,
      IS_IPV4 = 739,
      IS_IPV4_COMPAT = 740,
      IS_IPV4_MAPPED = 741,
      IS_IPV6 = 742,
      IS_USED_LOCK = 743,
      LAST_INSERT_ID = 744,
      LCASE = 745,
      LEAST = 746,
      LENGTH = 747,
      LINEFROMTEXT = 748,
      LINEFROMWKB = 749,
      LINESTRINGFROMTEXT = 750,
      LINESTRINGFROMWKB = 751,
      LN = 752,
      LOAD_FILE = 753,
      LOCATE = 754,
      LOG = 755,
      LOG10 = 756,
      LOG2 = 757,
      LOWER = 758,
      LPAD = 759,
      LTRIM = 760,
      MAKEDATE = 761,
      MAKETIME = 762,
      MAKE_SET = 763,
      MASTER_POS_WAIT = 764,
      MBRCONTAINS = 765,
      MBRDISJOINT = 766,
      MBREQUAL = 767,
      MBRINTERSECTS = 768,
      MBROVERLAPS = 769,
      MBRTOUCHES = 770,
      MBRWITHIN = 771,
      MD5 = 772,
      MLINEFROMTEXT = 773,
      MLINEFROMWKB = 774,
      MONTHNAME = 775,
      MPOINTFROMTEXT = 776,
      MPOINTFROMWKB = 777,
      MPOLYFROMTEXT = 778,
      MPOLYFROMWKB = 779,
      MULTILINESTRINGFROMTEXT = 780,
      MULTILINESTRINGFROMWKB = 781,
      MULTIPOINTFROMTEXT = 782,
      MULTIPOINTFROMWKB = 783,
      MULTIPOLYGONFROMTEXT = 784,
      MULTIPOLYGONFROMWKB = 785,
      NAME_CONST = 786,
      NULLIF = 787,
      NUMGEOMETRIES = 788,
      NUMINTERIORRINGS = 789,
      NUMPOINTS = 790,
      OCT = 791,
      OCTET_LENGTH = 792,
      ORD = 793,
      OVERLAPS = 794,
      PERIOD_ADD = 795,
      PERIOD_DIFF = 796,
      PI = 797,
      POINTFROMTEXT = 798,
      POINTFROMWKB = 799,
      POINTN = 800,
      POLYFROMTEXT = 801,
      POLYFROMWKB = 802,
      POLYGONFROMTEXT = 803,
      POLYGONFROMWKB = 804,
      POW = 805,
      POWER = 806,
      QUOTE = 807,
      RADIANS = 808,
      RAND = 809,
      RANDOM_BYTES = 810,
      RELEASE_LOCK = 811,
      REVERSE = 812,
      ROUND = 813,
      ROW_COUNT = 814,
      RPAD = 815,
      RTRIM = 816,
      SEC_TO_TIME = 817,
      SESSION_USER = 818,
      SHA = 819,
      SHA1 = 820,
      SHA2 = 821,
      SIGN = 822,
      SIN = 823,
      SLEEP = 824,
      SOUNDEX = 825,
      SQL_THREAD_WAIT_AFTER_GTIDS = 826,
      SQRT = 827,
      SRID = 828,
      STARTPOINT = 829,
      STRCMP = 830,
      STR_TO_DATE = 831,
      ST_AREA = 832,
      ST_ASBINARY = 833,
      ST_ASTEXT = 834,
      ST_ASWKB = 835,
      ST_ASWKT = 836,
      ST_BUFFER = 837,
      ST_CENTROID = 838,
      ST_CONTAINS = 839,
      ST_CROSSES = 840,
      ST_DIFFERENCE = 841,
      ST_DIMENSION = 842,
      ST_DISJOINT = 843,
      ST_DISTANCE = 844,
      ST_ENDPOINT = 845,
      ST_ENVELOPE = 846,
      ST_EQUALS = 847,
      ST_EXTERIORRING = 848,
      ST_GEOMCOLLFROMTEXT = 849,
      ST_GEOMCOLLFROMTXT = 850,
      ST_GEOMCOLLFROMWKB = 851,
      ST_GEOMETRYCOLLECTIONFROMTEXT = 852,
      ST_GEOMETRYCOLLECTIONFROMWKB = 853,
      ST_GEOMETRYFROMTEXT = 854,
      ST_GEOMETRYFROMWKB = 855,
      ST_GEOMETRYN = 856,
      ST_GEOMETRYTYPE = 857,
      ST_GEOMFROMTEXT = 858,
      ST_GEOMFROMWKB = 859,
      ST_INTERIORRINGN = 860,
      ST_INTERSECTION = 861,
      ST_INTERSECTS = 862,
      ST_ISCLOSED = 863,
      ST_ISEMPTY = 864,
      ST_ISSIMPLE = 865,
      ST_LINEFROMTEXT = 866,
      ST_LINEFROMWKB = 867,
      ST_LINESTRINGFROMTEXT = 868,
      ST_LINESTRINGFROMWKB = 869,
      ST_NUMGEOMETRIES = 870,
      ST_NUMINTERIORRING = 871,
      ST_NUMINTERIORRINGS = 872,
      ST_NUMPOINTS = 873,
      ST_OVERLAPS = 874,
      ST_POINTFROMTEXT = 875,
      ST_POINTFROMWKB = 876,
      ST_POINTN = 877,
      ST_POLYFROMTEXT = 878,
      ST_POLYFROMWKB = 879,
      ST_POLYGONFROMTEXT = 880,
      ST_POLYGONFROMWKB = 881,
      ST_SRID = 882,
      ST_STARTPOINT = 883,
      ST_SYMDIFFERENCE = 884,
      ST_TOUCHES = 885,
      ST_UNION = 886,
      ST_WITHIN = 887,
      ST_X = 888,
      ST_Y = 889,
      SUBDATE = 890,
      SUBSTRING_INDEX = 891,
      SUBTIME = 892,
      SYSTEM_USER = 893,
      TAN = 894,
      TIMEDIFF = 895,
      TIMESTAMPADD = 896,
      TIMESTAMPDIFF = 897,
      TIME_FORMAT = 898,
      TIME_TO_SEC = 899,
      TOUCHES = 900,
      TO_BASE64 = 901,
      TO_DAYS = 902,
      TO_SECONDS = 903,
      UCASE = 904,
      UNCOMPRESS = 905,
      UNCOMPRESSED_LENGTH = 906,
      UNHEX = 907,
      UNIX_TIMESTAMP = 908,
      UPDATEXML = 909,
      UPPER = 910,
      UUID = 911,
      UUID_SHORT = 912,
      VALIDATE_PASSWORD_STRENGTH = 913,
      VERSION = 914,
      WAIT_UNTIL_SQL_THREAD_AFTER_GTIDS = 915,
      WEEKDAY = 916,
      WEEKOFYEAR = 917,
      WEIGHT_STRING = 918,
      WITHIN = 919,
      YEARWEEK = 920,
      Y_FUNCTION = 921,
      X_FUNCTION = 922,
      VAR_ASSIGN = 923,
      PLUS_ASSIGN = 924,
      MINUS_ASSIGN = 925,
      MULT_ASSIGN = 926,
      DIV_ASSIGN = 927,
      MOD_ASSIGN = 928,
      AND_ASSIGN = 929,
      XOR_ASSIGN = 930,
      OR_ASSIGN = 931,
      STAR = 932,
      DIVIDE = 933,
      MODULE = 934,
      PLUS = 935,
      MINUSMINUS = 936,
      MINUS = 937,
      DIV = 938,
      MOD = 939,
      EQUAL_SYMBOL = 940,
      GREATER_SYMBOL = 941,
      LESS_SYMBOL = 942,
      EXCLAMATION_SYMBOL = 943,
      BIT_NOT_OP = 944,
      BIT_OR_OP = 945,
      BIT_AND_OP = 946,
      BIT_XOR_OP = 947,
      DOT = 948,
      LR_BRACKET = 949,
      RR_BRACKET = 950,
      COMMA = 951,
      SEMI = 952,
      AT_SIGN = 953,
      ZERO_DECIMAL = 954,
      ONE_DECIMAL = 955,
      TWO_DECIMAL = 956,
      SINGLE_QUOTE_SYMB = 957,
      DOUBLE_QUOTE_SYMB = 958,
      REVERSE_QUOTE_SYMB = 959,
      COLON_SYMB = 960,
      CHARSET_REVERSE_QOUTE_STRING = 961,
      FILESIZE_LITERAL = 962,
      STRING_LITERAL = 963,
      DECIMAL_LITERAL = 964,
      HEXADECIMAL_LITERAL = 965,
      REAL_LITERAL = 966,
      NULL_SPEC_LITERAL = 967,
      BIT_STRING = 968,
      STRING_CHARSET_NAME = 969,
      DOT_ID = 970,
      ID = 971,
      REVERSE_QUOTE_ID = 972,
      STRING_USER_NAME = 973,
      LOCAL_ID = 974,
      GLOBAL_ID = 975,
      ERROR_RECONGNIGION = 976;
  public static final int RULE_intervalType = 0,
      RULE_fullId = 1,
      RULE_fullColumnName = 2,
      RULE_charsetName = 3,
      RULE_collationName = 4,
      RULE_uid = 5,
      RULE_simpleId = 6,
      RULE_dottedId = 7,
      RULE_decimalLiteral = 8,
      RULE_stringLiteral = 9,
      RULE_booleanLiteral = 10,
      RULE_hexadecimalLiteral = 11,
      RULE_nullNotnull = 12,
      RULE_constant = 13,
      RULE_convertedDataType = 14,
      RULE_lengthOneDimension = 15,
      RULE_lengthTwoDimension = 16,
      RULE_expressions = 17,
      RULE_currentTimestamp = 18,
      RULE_functionCall = 19,
      RULE_specificFunction = 20,
      RULE_caseFuncAlternative = 21,
      RULE_levelsInWeightString = 22,
      RULE_levelInWeightListElement = 23,
      RULE_scalarFunctionName = 24,
      RULE_functionArgs = 25,
      RULE_functionArg = 26,
      RULE_expression = 27,
      RULE_predicate = 28,
      RULE_expressionAtom = 29,
      RULE_unaryOperator = 30,
      RULE_comparisonOperator = 31,
      RULE_logicalOperator = 32,
      RULE_bitOperator = 33,
      RULE_mathOperator = 34,
      RULE_charsetNameBase = 35,
      RULE_intervalTypeBase = 36,
      RULE_dataTypeBase = 37,
      RULE_keywordsCanBeId = 38,
      RULE_functionNameBase = 39;
  public static final String[] ruleNames = {
    "intervalType",
    "fullId",
    "fullColumnName",
    "charsetName",
    "collationName",
    "uid",
    "simpleId",
    "dottedId",
    "decimalLiteral",
    "stringLiteral",
    "booleanLiteral",
    "hexadecimalLiteral",
    "nullNotnull",
    "constant",
    "convertedDataType",
    "lengthOneDimension",
    "lengthTwoDimension",
    "expressions",
    "currentTimestamp",
    "functionCall",
    "specificFunction",
    "caseFuncAlternative",
    "levelsInWeightString",
    "levelInWeightListElement",
    "scalarFunctionName",
    "functionArgs",
    "functionArg",
    "expression",
    "predicate",
    "expressionAtom",
    "unaryOperator",
    "comparisonOperator",
    "logicalOperator",
    "bitOperator",
    "mathOperator",
    "charsetNameBase",
    "intervalTypeBase",
    "dataTypeBase",
    "keywordsCanBeId",
    "functionNameBase"
  };

  private static final String[] _LITERAL_NAMES = {
    null,
    null,
    null,
    null,
    null,
    "'ADD'",
    "'ALL'",
    "'ALTER'",
    "'ALWAYS'",
    "'ANALYZE'",
    "'AND'",
    "'AS'",
    "'ASC'",
    "'BEFORE'",
    "'BETWEEN'",
    "'BOTH'",
    "'BY'",
    "'CALL'",
    "'CASCADE'",
    "'CASE'",
    "'CAST'",
    "'CHANGE'",
    "'CHARACTER'",
    "'CHECK'",
    "'COLLATE'",
    "'COLUMN'",
    "'CONDITION'",
    "'CONSTRAINT'",
    "'CONTINUE'",
    "'CONVERT'",
    "'CREATE'",
    "'CROSS'",
    "'CURRENT_USER'",
    "'CURSOR'",
    "'DATABASE'",
    "'DATABASES'",
    "'DECLARE'",
    "'DEFAULT'",
    "'DELAYED'",
    "'DELETE'",
    "'DESC'",
    "'DESCRIBE'",
    "'DETERMINISTIC'",
    "'DISTINCT'",
    "'DISTINCTROW'",
    "'DROP'",
    "'EACH'",
    "'ELSE'",
    "'ELSEIF'",
    "'ENCLOSED'",
    "'ESCAPED'",
    "'EXISTS'",
    "'EXIT'",
    "'EXPLAIN'",
    "'FALSE'",
    "'FETCH'",
    "'FOR'",
    "'FORCE'",
    "'FOREIGN'",
    "'FROM'",
    "'FULLTEXT'",
    "'GENERATED'",
    "'GRANT'",
    "'GROUP'",
    "'HAVING'",
    "'HIGH_PRIORITY'",
    "'IF'",
    "'IGNORE'",
    "'IN'",
    "'INDEX'",
    "'INFILE'",
    "'INNER'",
    "'INOUT'",
    "'INSERT'",
    "'INTERVAL'",
    "'INTO'",
    "'IS'",
    "'ITERATE'",
    "'JOIN'",
    "'KEY'",
    "'KEYS'",
    "'KILL'",
    "'LEADING'",
    "'LEAVE'",
    "'LEFT'",
    "'LIKE'",
    "'LIMIT'",
    "'LINEAR'",
    "'LINES'",
    "'LOAD'",
    "'LOCK'",
    "'LOOP'",
    "'LOW_PRIORITY'",
    "'MASTER_BIND'",
    "'MASTER_SSL_VERIFY_SERVER_CERT'",
    "'MATCH'",
    "'MAXVALUE'",
    "'MODIFIES'",
    "'NATURAL'",
    "'NOT'",
    "'NO_WRITE_TO_BINLOG'",
    "'NULL'",
    "'ON'",
    "'OPTIMIZE'",
    "'OPTION'",
    "'OPTIONALLY'",
    "'OR'",
    "'ORDER'",
    "'OUT'",
    "'OUTER'",
    "'OUTFILE'",
    "'PARTITION'",
    "'PRIMARY'",
    "'PROCEDURE'",
    "'PURGE'",
    "'RANGE'",
    "'READ'",
    "'READS'",
    "'REFERENCES'",
    "'REGEXP'",
    "'RELEASE'",
    "'RENAME'",
    "'REPEAT'",
    "'REPLACE'",
    "'REQUIRE'",
    "'RESTRICT'",
    "'RETURN'",
    "'REVOKE'",
    "'RIGHT'",
    "'RLIKE'",
    "'SCHEMA'",
    "'SCHEMAS'",
    "'SELECT'",
    "'SET'",
    "'SEPARATOR'",
    "'SHOW'",
    "'SPATIAL'",
    "'SQL'",
    "'SQLEXCEPTION'",
    "'SQLSTATE'",
    "'SQLWARNING'",
    "'SQL_BIG_RESULT'",
    "'SQL_CALC_FOUND_ROWS'",
    "'SQL_SMALL_RESULT'",
    "'SSL'",
    "'STARTING'",
    "'STRAIGHT_JOIN'",
    "'TABLE'",
    "'TERMINATED'",
    "'THEN'",
    "'TO'",
    "'TRAILING'",
    "'TRIGGER'",
    "'TRUE'",
    "'UNDO'",
    "'UNION'",
    "'UNIQUE'",
    "'UNLOCK'",
    "'UNSIGNED'",
    "'UPDATE'",
    "'USAGE'",
    "'USE'",
    "'USING'",
    "'VALUES'",
    "'WHEN'",
    "'WHERE'",
    "'WHILE'",
    "'WITH'",
    "'WRITE'",
    "'XOR'",
    "'ZEROFILL'",
    "'TINYINT'",
    "'SMALLINT'",
    "'MEDIUMINT'",
    "'INT'",
    "'INTEGER'",
    "'BIGINT'",
    "'REAL'",
    "'DOUBLE'",
    "'FLOAT'",
    "'DECIMAL'",
    "'NUMERIC'",
    "'DATE'",
    "'TIME'",
    "'TIMESTAMP'",
    "'DATETIME'",
    "'YEAR'",
    "'CHAR'",
    "'VARCHAR'",
    "'BINARY'",
    "'VARBINARY'",
    "'TINYBLOB'",
    "'BLOB'",
    "'MEDIUMBLOB'",
    "'LONGBLOB'",
    "'TINYTEXT'",
    "'TEXT'",
    "'MEDIUMTEXT'",
    "'LONGTEXT'",
    "'ENUM'",
    "'YEAR_MONTH'",
    "'DAY_HOUR'",
    "'DAY_MINUTE'",
    "'DAY_SECOND'",
    "'HOUR_MINUTE'",
    "'HOUR_SECOND'",
    "'MINUTE_SECOND'",
    "'SECOND_MICROSECOND'",
    "'MINUTE_MICROSECOND'",
    "'HOUR_MICROSECOND'",
    "'DAY_MICROSECOND'",
    "'AVG'",
    "'BIT_AND'",
    "'BIT_OR'",
    "'BIT_XOR'",
    "'COUNT'",
    "'GROUP_CONCAT'",
    "'MAX'",
    "'MIN'",
    "'STD'",
    "'STDDEV'",
    "'STDDEV_POP'",
    "'STDDEV_SAMP'",
    "'SUM'",
    "'VAR_POP'",
    "'VAR_SAMP'",
    "'VARIANCE'",
    "'CURRENT_DATE'",
    "'CURRENT_TIME'",
    "'CURRENT_TIMESTAMP'",
    "'LOCALTIME'",
    "'CURDATE'",
    "'CURTIME'",
    "'DATE_ADD'",
    "'DATE_SUB'",
    "'EXTRACT'",
    "'LOCALTIMESTAMP'",
    "'NOW'",
    "'POSITION'",
    "'SUBSTR'",
    "'SUBSTRING'",
    "'SYSDATE'",
    "'TRIM'",
    "'UTC_DATE'",
    "'UTC_TIME'",
    "'UTC_TIMESTAMP'",
    "'ACCOUNT'",
    "'ACTION'",
    "'AFTER'",
    "'AGGREGATE'",
    "'ALGORITHM'",
    "'ANY'",
    "'AT'",
    "'AUTHORS'",
    "'AUTOCOMMIT'",
    "'AUTOEXTEND_SIZE'",
    "'AUTO_INCREMENT'",
    "'AVG_ROW_LENGTH'",
    "'BEGIN'",
    "'BINLOG'",
    "'BIT'",
    "'BLOCK'",
    "'BOOL'",
    "'BOOLEAN'",
    "'BTREE'",
    "'CACHE'",
    "'CASCADED'",
    "'CHAIN'",
    "'CHANGED'",
    "'CHANNEL'",
    "'CHECKSUM'",
    "'CIPHER'",
    "'CLIENT'",
    "'CLOSE'",
    "'COALESCE'",
    "'CODE'",
    "'COLUMNS'",
    "'COLUMN_FORMAT'",
    "'COMMENT'",
    "'COMMIT'",
    "'COMPACT'",
    "'COMPLETION'",
    "'COMPRESSED'",
    "'COMPRESSION'",
    "'CONCURRENT'",
    "'CONNECTION'",
    "'CONSISTENT'",
    "'CONTAINS'",
    "'CONTEXT'",
    "'CONTRIBUTORS'",
    "'COPY'",
    "'CPU'",
    "'DATA'",
    "'DATAFILE'",
    "'DEALLOCATE'",
    "'DEFAULT_AUTH'",
    "'DEFINER'",
    "'DELAY_KEY_WRITE'",
    "'DES_KEY_FILE'",
    "'DIRECTORY'",
    "'DISABLE'",
    "'DISCARD'",
    "'DISK'",
    "'DO'",
    "'DUMPFILE'",
    "'DUPLICATE'",
    "'DYNAMIC'",
    "'ENABLE'",
    "'ENCRYPTION'",
    "'END'",
    "'ENDS'",
    "'ENGINE'",
    "'ENGINES'",
    "'ERROR'",
    "'ERRORS'",
    "'ESCAPE'",
    "'EVEN'",
    "'EVENT'",
    "'EVENTS'",
    "'EVERY'",
    "'EXCHANGE'",
    "'EXCLUSIVE'",
    "'EXPIRE'",
    "'EXPORT'",
    "'EXTENDED'",
    "'EXTENT_SIZE'",
    "'FAST'",
    "'FAULTS'",
    "'FIELDS'",
    "'FILE_BLOCK_SIZE'",
    "'FILTER'",
    "'FIRST'",
    "'FIXED'",
    "'FLUSH'",
    "'FOLLOWS'",
    "'FOUND'",
    "'FULL'",
    "'FUNCTION'",
    "'GENERAL'",
    "'GLOBAL'",
    "'GRANTS'",
    "'GROUP_REPLICATION'",
    "'HANDLER'",
    "'HASH'",
    "'HELP'",
    "'HOST'",
    "'HOSTS'",
    "'IDENTIFIED'",
    "'IGNORE_SERVER_IDS'",
    "'IMPORT'",
    "'INDEXES'",
    "'INITIAL_SIZE'",
    "'INPLACE'",
    "'INSERT_METHOD'",
    "'INSTALL'",
    "'INSTANCE'",
    "'INVOKER'",
    "'IO'",
    "'IO_THREAD'",
    "'IPC'",
    "'ISOLATION'",
    "'ISSUER'",
    "'JSON'",
    "'KEY_BLOCK_SIZE'",
    "'LANGUAGE'",
    "'LAST'",
    "'LEAVES'",
    "'LESS'",
    "'LEVEL'",
    "'LIST'",
    "'LOCAL'",
    "'LOGFILE'",
    "'LOGS'",
    "'MASTER'",
    "'MASTER_AUTO_POSITION'",
    "'MASTER_CONNECT_RETRY'",
    "'MASTER_DELAY'",
    "'MASTER_HEARTBEAT_PERIOD'",
    "'MASTER_HOST'",
    "'MASTER_LOG_FILE'",
    "'MASTER_LOG_POS'",
    "'MASTER_PASSWORD'",
    "'MASTER_PORT'",
    "'MASTER_RETRY_COUNT'",
    "'MASTER_SSL'",
    "'MASTER_SSL_CA'",
    "'MASTER_SSL_CAPATH'",
    "'MASTER_SSL_CERT'",
    "'MASTER_SSL_CIPHER'",
    "'MASTER_SSL_CRL'",
    "'MASTER_SSL_CRLPATH'",
    "'MASTER_SSL_KEY'",
    "'MASTER_TLS_VERSION'",
    "'MASTER_USER'",
    "'MAX_CONNECTIONS_PER_HOUR'",
    "'MAX_QUERIES_PER_HOUR'",
    "'MAX_ROWS'",
    "'MAX_SIZE'",
    "'MAX_UPDATES_PER_HOUR'",
    "'MAX_USER_CONNECTIONS'",
    "'MEDIUM'",
    "'MERGE'",
    "'MID'",
    "'MIGRATE'",
    "'MIN_ROWS'",
    "'MODE'",
    "'MODIFY'",
    "'MUTEX'",
    "'MYSQL'",
    "'NAME'",
    "'NAMES'",
    "'NCHAR'",
    "'NEVER'",
    "'NEXT'",
    "'NO'",
    "'NODEGROUP'",
    "'NONE'",
    "'OFFLINE'",
    "'OFFSET'",
    "'OJ'",
    "'OLD_PASSWORD'",
    "'ONE'",
    "'ONLINE'",
    "'ONLY'",
    "'OPEN'",
    "'OPTIMIZER_COSTS'",
    "'OPTIONS'",
    "'OWNER'",
    "'PACK_KEYS'",
    "'PAGE'",
    "'PARSER'",
    "'PARTIAL'",
    "'PARTITIONING'",
    "'PARTITIONS'",
    "'PASSWORD'",
    "'PHASE'",
    "'PLUGIN'",
    "'PLUGIN_DIR'",
    "'PLUGINS'",
    "'PORT'",
    "'PRECEDES'",
    "'PREPARE'",
    "'PRESERVE'",
    "'PREV'",
    "'PROCESSLIST'",
    "'PROFILE'",
    "'PROFILES'",
    "'PROXY'",
    "'QUERY'",
    "'QUICK'",
    "'REBUILD'",
    "'RECOVER'",
    "'REDO_BUFFER_SIZE'",
    "'REDUNDANT'",
    "'RELAY'",
    "'RELAY_LOG_FILE'",
    "'RELAY_LOG_POS'",
    "'RELAYLOG'",
    "'REMOVE'",
    "'REORGANIZE'",
    "'REPAIR'",
    "'REPLICATE_DO_DB'",
    "'REPLICATE_DO_TABLE'",
    "'REPLICATE_IGNORE_DB'",
    "'REPLICATE_IGNORE_TABLE'",
    "'REPLICATE_REWRITE_DB'",
    "'REPLICATE_WILD_DO_TABLE'",
    "'REPLICATE_WILD_IGNORE_TABLE'",
    "'REPLICATION'",
    "'RESET'",
    "'RESUME'",
    "'RETURNS'",
    "'ROLLBACK'",
    "'ROLLUP'",
    "'ROTATE'",
    "'ROW'",
    "'ROWS'",
    "'ROW_FORMAT'",
    "'SAVEPOINT'",
    "'SCHEDULE'",
    "'SECURITY'",
    "'SERVER'",
    "'SESSION'",
    "'SHARE'",
    "'SHARED'",
    "'SIGNED'",
    "'SIMPLE'",
    "'SLAVE'",
    "'SLOW'",
    "'SNAPSHOT'",
    "'SOCKET'",
    "'SOME'",
    "'SONAME'",
    "'SOUNDS'",
    "'SOURCE'",
    "'SQL_AFTER_GTIDS'",
    "'SQL_AFTER_MTS_GAPS'",
    "'SQL_BEFORE_GTIDS'",
    "'SQL_BUFFER_RESULT'",
    "'SQL_CACHE'",
    "'SQL_NO_CACHE'",
    "'SQL_THREAD'",
    "'START'",
    "'STARTS'",
    "'STATS_AUTO_RECALC'",
    "'STATS_PERSISTENT'",
    "'STATS_SAMPLE_PAGES'",
    "'STATUS'",
    "'STOP'",
    "'STORAGE'",
    "'STORED'",
    "'STRING'",
    "'SUBJECT'",
    "'SUBPARTITION'",
    "'SUBPARTITIONS'",
    "'SUSPEND'",
    "'SWAPS'",
    "'SWITCHES'",
    "'TABLESPACE'",
    "'TEMPORARY'",
    "'TEMPTABLE'",
    "'THAN'",
    "'TRADITIONAL'",
    "'TRANSACTION'",
    "'TRIGGERS'",
    "'TRUNCATE'",
    "'UNDEFINED'",
    "'UNDOFILE'",
    "'UNDO_BUFFER_SIZE'",
    "'UNINSTALL'",
    "'UNKNOWN'",
    "'UNTIL'",
    "'UPGRADE'",
    "'USER'",
    "'USE_FRM'",
    "'USER_RESOURCES'",
    "'VALIDATION'",
    "'VALUE'",
    "'VARIABLES'",
    "'VIEW'",
    "'VIRTUAL'",
    "'WAIT'",
    "'WARNINGS'",
    "'WITHOUT'",
    "'WORK'",
    "'WRAPPER'",
    "'X509'",
    "'XA'",
    "'XML'",
    "'EUR'",
    "'USA'",
    "'JIS'",
    "'ISO'",
    "'INTERNAL'",
    "'QUARTER'",
    "'MONTH'",
    "'DAY'",
    "'HOUR'",
    "'MINUTE'",
    "'WEEK'",
    "'SECOND'",
    "'MICROSECOND'",
    "'TABLES'",
    "'ROUTINE'",
    "'EXECUTE'",
    "'FILE'",
    "'PROCESS'",
    "'RELOAD'",
    "'SHUTDOWN'",
    "'SUPER'",
    "'PRIVILEGES'",
    "'ARMSCII8'",
    "'ASCII'",
    "'BIG5'",
    "'CP1250'",
    "'CP1251'",
    "'CP1256'",
    "'CP1257'",
    "'CP850'",
    "'CP852'",
    "'CP866'",
    "'CP932'",
    "'DEC8'",
    "'EUCJPMS'",
    "'EUCKR'",
    "'GB2312'",
    "'GBK'",
    "'GEOSTD8'",
    "'GREEK'",
    "'HEBREW'",
    "'HP8'",
    "'KEYBCS2'",
    "'KOI8R'",
    "'KOI8U'",
    "'LATIN1'",
    "'LATIN2'",
    "'LATIN5'",
    "'LATIN7'",
    "'MACCE'",
    "'MACROMAN'",
    "'SJIS'",
    "'SWE7'",
    "'TIS620'",
    "'UCS2'",
    "'UJIS'",
    "'UTF16'",
    "'UTF16LE'",
    "'UTF32'",
    "'UTF8'",
    "'UTF8MB3'",
    "'UTF8MB4'",
    "'ARCHIVE'",
    "'BLACKHOLE'",
    "'CSV'",
    "'FEDERATED'",
    "'INNODB'",
    "'MEMORY'",
    "'MRG_MYISAM'",
    "'MYISAM'",
    "'NDB'",
    "'NDBCLUSTER'",
    "'PERFOMANCE_SCHEMA'",
    "'REPEATABLE'",
    "'COMMITTED'",
    "'UNCOMMITTED'",
    "'SERIALIZABLE'",
    "'GEOMETRYCOLLECTION'",
    "'LINESTRING'",
    "'MULTILINESTRING'",
    "'MULTIPOINT'",
    "'MULTIPOLYGON'",
    "'POINT'",
    "'POLYGON'",
    "'ABS'",
    "'ACOS'",
    "'ADDDATE'",
    "'ADDTIME'",
    "'AES_DECRYPT'",
    "'AES_ENCRYPT'",
    "'AREA'",
    "'ASBINARY'",
    "'ASIN'",
    "'ASTEXT'",
    "'ASWKB'",
    "'ASWKT'",
    "'ASYMMETRIC_DECRYPT'",
    "'ASYMMETRIC_DERIVE'",
    "'ASYMMETRIC_ENCRYPT'",
    "'ASYMMETRIC_SIGN'",
    "'ASYMMETRIC_VERIFY'",
    "'ATAN'",
    "'ATAN2'",
    "'BENCHMARK'",
    "'BIN'",
    "'BIT_COUNT'",
    "'BIT_LENGTH'",
    "'BUFFER'",
    "'CEIL'",
    "'CEILING'",
    "'CENTROID'",
    "'CHARACTER_LENGTH'",
    "'CHARSET'",
    "'CHAR_LENGTH'",
    "'COERCIBILITY'",
    "'COLLATION'",
    "'COMPRESS'",
    "'CONCAT'",
    "'CONCAT_WS'",
    "'CONNECTION_ID'",
    "'CONV'",
    "'CONVERT_TZ'",
    "'COS'",
    "'COT'",
    "'CRC32'",
    "'CREATE_ASYMMETRIC_PRIV_KEY'",
    "'CREATE_ASYMMETRIC_PUB_KEY'",
    "'CREATE_DH_PARAMETERS'",
    "'CREATE_DIGEST'",
    "'CROSSES'",
    "'DATEDIFF'",
    "'DATE_FORMAT'",
    "'DAYNAME'",
    "'DAYOFMONTH'",
    "'DAYOFWEEK'",
    "'DAYOFYEAR'",
    "'DECODE'",
    "'DEGREES'",
    "'DES_DECRYPT'",
    "'DES_ENCRYPT'",
    "'DIMENSION'",
    "'DISJOINT'",
    "'ELT'",
    "'ENCODE'",
    "'ENCRYPT'",
    "'ENDPOINT'",
    "'ENVELOPE'",
    "'EQUALS'",
    "'EXP'",
    "'EXPORT_SET'",
    "'EXTERIORRING'",
    "'EXTRACTVALUE'",
    "'FIELD'",
    "'FIND_IN_SET'",
    "'FLOOR'",
    "'FORMAT'",
    "'FOUND_ROWS'",
    "'FROM_BASE64'",
    "'FROM_DAYS'",
    "'FROM_UNIXTIME'",
    "'GEOMCOLLFROMTEXT'",
    "'GEOMCOLLFROMWKB'",
    "'GEOMETRYCOLLECTIONFROMTEXT'",
    "'GEOMETRYCOLLECTIONFROMWKB'",
    "'GEOMETRYFROMTEXT'",
    "'GEOMETRYFROMWKB'",
    "'GEOMETRYN'",
    "'GEOMETRYTYPE'",
    "'GEOMFROMTEXT'",
    "'GEOMFROMWKB'",
    "'GET_FORMAT'",
    "'GET_LOCK'",
    "'GLENGTH'",
    "'GREATEST'",
    "'GTID_SUBSET'",
    "'GTID_SUBTRACT'",
    "'HEX'",
    "'IFNULL'",
    "'INET6_ATON'",
    "'INET6_NTOA'",
    "'INET_ATON'",
    "'INET_NTOA'",
    "'INSTR'",
    "'INTERIORRINGN'",
    "'INTERSECTS'",
    "'ISCLOSED'",
    "'ISEMPTY'",
    "'ISNULL'",
    "'ISSIMPLE'",
    "'IS_FREE_LOCK'",
    "'IS_IPV4'",
    "'IS_IPV4_COMPAT'",
    "'IS_IPV4_MAPPED'",
    "'IS_IPV6'",
    "'IS_USED_LOCK'",
    "'LAST_INSERT_ID'",
    "'LCASE'",
    "'LEAST'",
    "'LENGTH'",
    "'LINEFROMTEXT'",
    "'LINEFROMWKB'",
    "'LINESTRINGFROMTEXT'",
    "'LINESTRINGFROMWKB'",
    "'LN'",
    "'LOAD_FILE'",
    "'LOCATE'",
    "'LOG'",
    "'LOG10'",
    "'LOG2'",
    "'LOWER'",
    "'LPAD'",
    "'LTRIM'",
    "'MAKEDATE'",
    "'MAKETIME'",
    "'MAKE_SET'",
    "'MASTER_POS_WAIT'",
    "'MBRCONTAINS'",
    "'MBRDISJOINT'",
    "'MBREQUAL'",
    "'MBRINTERSECTS'",
    "'MBROVERLAPS'",
    "'MBRTOUCHES'",
    "'MBRWITHIN'",
    "'MD5'",
    "'MLINEFROMTEXT'",
    "'MLINEFROMWKB'",
    "'MONTHNAME'",
    "'MPOINTFROMTEXT'",
    "'MPOINTFROMWKB'",
    "'MPOLYFROMTEXT'",
    "'MPOLYFROMWKB'",
    "'MULTILINESTRINGFROMTEXT'",
    "'MULTILINESTRINGFROMWKB'",
    "'MULTIPOINTFROMTEXT'",
    "'MULTIPOINTFROMWKB'",
    "'MULTIPOLYGONFROMTEXT'",
    "'MULTIPOLYGONFROMWKB'",
    "'NAME_CONST'",
    "'NULLIF'",
    "'NUMGEOMETRIES'",
    "'NUMINTERIORRINGS'",
    "'NUMPOINTS'",
    "'OCT'",
    "'OCTET_LENGTH'",
    "'ORD'",
    "'OVERLAPS'",
    "'PERIOD_ADD'",
    "'PERIOD_DIFF'",
    "'PI'",
    "'POINTFROMTEXT'",
    "'POINTFROMWKB'",
    "'POINTN'",
    "'POLYFROMTEXT'",
    "'POLYFROMWKB'",
    "'POLYGONFROMTEXT'",
    "'POLYGONFROMWKB'",
    "'POW'",
    "'POWER'",
    "'QUOTE'",
    "'RADIANS'",
    "'RAND'",
    "'RANDOM_BYTES'",
    "'RELEASE_LOCK'",
    "'REVERSE'",
    "'ROUND'",
    "'ROW_COUNT'",
    "'RPAD'",
    "'RTRIM'",
    "'SEC_TO_TIME'",
    "'SESSION_USER'",
    "'SHA'",
    "'SHA1'",
    "'SHA2'",
    "'SIGN'",
    "'SIN'",
    "'SLEEP'",
    "'SOUNDEX'",
    "'SQL_THREAD_WAIT_AFTER_GTIDS'",
    "'SQRT'",
    "'SRID'",
    "'STARTPOINT'",
    "'STRCMP'",
    "'STR_TO_DATE'",
    "'ST_AREA'",
    "'ST_ASBINARY'",
    "'ST_ASTEXT'",
    "'ST_ASWKB'",
    "'ST_ASWKT'",
    "'ST_BUFFER'",
    "'ST_CENTROID'",
    "'ST_CONTAINS'",
    "'ST_CROSSES'",
    "'ST_DIFFERENCE'",
    "'ST_DIMENSION'",
    "'ST_DISJOINT'",
    "'ST_DISTANCE'",
    "'ST_ENDPOINT'",
    "'ST_ENVELOPE'",
    "'ST_EQUALS'",
    "'ST_EXTERIORRING'",
    "'ST_GEOMCOLLFROMTEXT'",
    "'ST_GEOMCOLLFROMTXT'",
    "'ST_GEOMCOLLFROMWKB'",
    "'ST_GEOMETRYCOLLECTIONFROMTEXT'",
    "'ST_GEOMETRYCOLLECTIONFROMWKB'",
    "'ST_GEOMETRYFROMTEXT'",
    "'ST_GEOMETRYFROMWKB'",
    "'ST_GEOMETRYN'",
    "'ST_GEOMETRYTYPE'",
    "'ST_GEOMFROMTEXT'",
    "'ST_GEOMFROMWKB'",
    "'ST_INTERIORRINGN'",
    "'ST_INTERSECTION'",
    "'ST_INTERSECTS'",
    "'ST_ISCLOSED'",
    "'ST_ISEMPTY'",
    "'ST_ISSIMPLE'",
    "'ST_LINEFROMTEXT'",
    "'ST_LINEFROMWKB'",
    "'ST_LINESTRINGFROMTEXT'",
    "'ST_LINESTRINGFROMWKB'",
    "'ST_NUMGEOMETRIES'",
    "'ST_NUMINTERIORRING'",
    "'ST_NUMINTERIORRINGS'",
    "'ST_NUMPOINTS'",
    "'ST_OVERLAPS'",
    "'ST_POINTFROMTEXT'",
    "'ST_POINTFROMWKB'",
    "'ST_POINTN'",
    "'ST_POLYFROMTEXT'",
    "'ST_POLYFROMWKB'",
    "'ST_POLYGONFROMTEXT'",
    "'ST_POLYGONFROMWKB'",
    "'ST_SRID'",
    "'ST_STARTPOINT'",
    "'ST_SYMDIFFERENCE'",
    "'ST_TOUCHES'",
    "'ST_UNION'",
    "'ST_WITHIN'",
    "'ST_X'",
    "'ST_Y'",
    "'SUBDATE'",
    "'SUBSTRING_INDEX'",
    "'SUBTIME'",
    "'SYSTEM_USER'",
    "'TAN'",
    "'TIMEDIFF'",
    "'TIMESTAMPADD'",
    "'TIMESTAMPDIFF'",
    "'TIME_FORMAT'",
    "'TIME_TO_SEC'",
    "'TOUCHES'",
    "'TO_BASE64'",
    "'TO_DAYS'",
    "'TO_SECONDS'",
    "'UCASE'",
    "'UNCOMPRESS'",
    "'UNCOMPRESSED_LENGTH'",
    "'UNHEX'",
    "'UNIX_TIMESTAMP'",
    "'UPDATEXML'",
    "'UPPER'",
    "'UUID'",
    "'UUID_SHORT'",
    "'VALIDATE_PASSWORD_STRENGTH'",
    "'VERSION'",
    "'WAIT_UNTIL_SQL_THREAD_AFTER_GTIDS'",
    "'WEEKDAY'",
    "'WEEKOFYEAR'",
    "'WEIGHT_STRING'",
    "'WITHIN'",
    "'YEARWEEK'",
    "'Y'",
    "'X'",
    "':='",
    "'+='",
    "'-='",
    "'*='",
    "'/='",
    "'%='",
    "'&='",
    "'^='",
    "'|='",
    "'*'",
    "'/'",
    "'%'",
    "'+'",
    "'--'",
    "'-'",
    "'DIV'",
    "'MOD'",
    "'='",
    "'>'",
    "'<'",
    "'!'",
    "'~'",
    "'|'",
    "'&'",
    "'^'",
    "'.'",
    "'('",
    "')'",
    "','",
    "';'",
    "'@'",
    "'0'",
    "'1'",
    "'2'",
    "'''",
    "'\"'",
    "'`'",
    "':'"
  };
  private static final String[] _SYMBOLIC_NAMES = {
    null,
    "SPACE",
    "SPEC_MYSQL_COMMENT",
    "COMMENT_INPUT",
    "LINE_COMMENT",
    "ADD",
    "ALL",
    "ALTER",
    "ALWAYS",
    "ANALYZE",
    "AND",
    "AS",
    "ASC",
    "BEFORE",
    "BETWEEN",
    "BOTH",
    "BY",
    "CALL",
    "CASCADE",
    "CASE",
    "CAST",
    "CHANGE",
    "CHARACTER",
    "CHECK",
    "COLLATE",
    "COLUMN",
    "CONDITION",
    "CONSTRAINT",
    "CONTINUE",
    "CONVERT",
    "CREATE",
    "CROSS",
    "CURRENT_USER",
    "CURSOR",
    "DATABASE",
    "DATABASES",
    "DECLARE",
    "DEFAULT",
    "DELAYED",
    "DELETE",
    "DESC",
    "DESCRIBE",
    "DETERMINISTIC",
    "DISTINCT",
    "DISTINCTROW",
    "DROP",
    "EACH",
    "ELSE",
    "ELSEIF",
    "ENCLOSED",
    "ESCAPED",
    "EXISTS",
    "EXIT",
    "EXPLAIN",
    "FALSE",
    "FETCH",
    "FOR",
    "FORCE",
    "FOREIGN",
    "FROM",
    "FULLTEXT",
    "GENERATED",
    "GRANT",
    "GROUP",
    "HAVING",
    "HIGH_PRIORITY",
    "IF",
    "IGNORE",
    "IN",
    "INDEX",
    "INFILE",
    "INNER",
    "INOUT",
    "INSERT",
    "INTERVAL",
    "INTO",
    "IS",
    "ITERATE",
    "JOIN",
    "KEY",
    "KEYS",
    "KILL",
    "LEADING",
    "LEAVE",
    "LEFT",
    "LIKE",
    "LIMIT",
    "LINEAR",
    "LINES",
    "LOAD",
    "LOCK",
    "LOOP",
    "LOW_PRIORITY",
    "MASTER_BIND",
    "MASTER_SSL_VERIFY_SERVER_CERT",
    "MATCH",
    "MAXVALUE",
    "MODIFIES",
    "NATURAL",
    "NOT",
    "NO_WRITE_TO_BINLOG",
    "NULL_LITERAL",
    "ON",
    "OPTIMIZE",
    "OPTION",
    "OPTIONALLY",
    "OR",
    "ORDER",
    "OUT",
    "OUTER",
    "OUTFILE",
    "PARTITION",
    "PRIMARY",
    "PROCEDURE",
    "PURGE",
    "RANGE",
    "READ",
    "READS",
    "REFERENCES",
    "REGEXP",
    "RELEASE",
    "RENAME",
    "REPEAT",
    "REPLACE",
    "REQUIRE",
    "RESTRICT",
    "RETURN",
    "REVOKE",
    "RIGHT",
    "RLIKE",
    "SCHEMA",
    "SCHEMAS",
    "SELECT",
    "SET",
    "SEPARATOR",
    "SHOW",
    "SPATIAL",
    "SQL",
    "SQLEXCEPTION",
    "SQLSTATE",
    "SQLWARNING",
    "SQL_BIG_RESULT",
    "SQL_CALC_FOUND_ROWS",
    "SQL_SMALL_RESULT",
    "SSL",
    "STARTING",
    "STRAIGHT_JOIN",
    "TABLE",
    "TERMINATED",
    "THEN",
    "TO",
    "TRAILING",
    "TRIGGER",
    "TRUE",
    "UNDO",
    "UNION",
    "UNIQUE",
    "UNLOCK",
    "UNSIGNED",
    "UPDATE",
    "USAGE",
    "USE",
    "USING",
    "VALUES",
    "WHEN",
    "WHERE",
    "WHILE",
    "WITH",
    "WRITE",
    "XOR",
    "ZEROFILL",
    "TINYINT",
    "SMALLINT",
    "MEDIUMINT",
    "INT",
    "INTEGER",
    "BIGINT",
    "REAL",
    "DOUBLE",
    "FLOAT",
    "DECIMAL",
    "NUMERIC",
    "DATE",
    "TIME",
    "TIMESTAMP",
    "DATETIME",
    "YEAR",
    "CHAR",
    "VARCHAR",
    "BINARY",
    "VARBINARY",
    "TINYBLOB",
    "BLOB",
    "MEDIUMBLOB",
    "LONGBLOB",
    "TINYTEXT",
    "TEXT",
    "MEDIUMTEXT",
    "LONGTEXT",
    "ENUM",
    "YEAR_MONTH",
    "DAY_HOUR",
    "DAY_MINUTE",
    "DAY_SECOND",
    "HOUR_MINUTE",
    "HOUR_SECOND",
    "MINUTE_SECOND",
    "SECOND_MICROSECOND",
    "MINUTE_MICROSECOND",
    "HOUR_MICROSECOND",
    "DAY_MICROSECOND",
    "AVG",
    "BIT_AND",
    "BIT_OR",
    "BIT_XOR",
    "COUNT",
    "GROUP_CONCAT",
    "MAX",
    "MIN",
    "STD",
    "STDDEV",
    "STDDEV_POP",
    "STDDEV_SAMP",
    "SUM",
    "VAR_POP",
    "VAR_SAMP",
    "VARIANCE",
    "CURRENT_DATE",
    "CURRENT_TIME",
    "CURRENT_TIMESTAMP",
    "LOCALTIME",
    "CURDATE",
    "CURTIME",
    "DATE_ADD",
    "DATE_SUB",
    "EXTRACT",
    "LOCALTIMESTAMP",
    "NOW",
    "POSITION",
    "SUBSTR",
    "SUBSTRING",
    "SYSDATE",
    "TRIM",
    "UTC_DATE",
    "UTC_TIME",
    "UTC_TIMESTAMP",
    "ACCOUNT",
    "ACTION",
    "AFTER",
    "AGGREGATE",
    "ALGORITHM",
    "ANY",
    "AT",
    "AUTHORS",
    "AUTOCOMMIT",
    "AUTOEXTEND_SIZE",
    "AUTO_INCREMENT",
    "AVG_ROW_LENGTH",
    "BEGIN",
    "BINLOG",
    "BIT",
    "BLOCK",
    "BOOL",
    "BOOLEAN",
    "BTREE",
    "CACHE",
    "CASCADED",
    "CHAIN",
    "CHANGED",
    "CHANNEL",
    "CHECKSUM",
    "CIPHER",
    "CLIENT",
    "CLOSE",
    "COALESCE",
    "CODE",
    "COLUMNS",
    "COLUMN_FORMAT",
    "COMMENT",
    "COMMIT",
    "COMPACT",
    "COMPLETION",
    "COMPRESSED",
    "COMPRESSION",
    "CONCURRENT",
    "CONNECTION",
    "CONSISTENT",
    "CONTAINS",
    "CONTEXT",
    "CONTRIBUTORS",
    "COPY",
    "CPU",
    "DATA",
    "DATAFILE",
    "DEALLOCATE",
    "DEFAULT_AUTH",
    "DEFINER",
    "DELAY_KEY_WRITE",
    "DES_KEY_FILE",
    "DIRECTORY",
    "DISABLE",
    "DISCARD",
    "DISK",
    "DO",
    "DUMPFILE",
    "DUPLICATE",
    "DYNAMIC",
    "ENABLE",
    "ENCRYPTION",
    "END",
    "ENDS",
    "ENGINE",
    "ENGINES",
    "ERROR",
    "ERRORS",
    "ESCAPE",
    "EVEN",
    "EVENT",
    "EVENTS",
    "EVERY",
    "EXCHANGE",
    "EXCLUSIVE",
    "EXPIRE",
    "EXPORT",
    "EXTENDED",
    "EXTENT_SIZE",
    "FAST",
    "FAULTS",
    "FIELDS",
    "FILE_BLOCK_SIZE",
    "FILTER",
    "FIRST",
    "FIXED",
    "FLUSH",
    "FOLLOWS",
    "FOUND",
    "FULL",
    "FUNCTION",
    "GENERAL",
    "GLOBAL",
    "GRANTS",
    "GROUP_REPLICATION",
    "HANDLER",
    "HASH",
    "HELP",
    "HOST",
    "HOSTS",
    "IDENTIFIED",
    "IGNORE_SERVER_IDS",
    "IMPORT",
    "INDEXES",
    "INITIAL_SIZE",
    "INPLACE",
    "INSERT_METHOD",
    "INSTALL",
    "INSTANCE",
    "INVOKER",
    "IO",
    "IO_THREAD",
    "IPC",
    "ISOLATION",
    "ISSUER",
    "JSON",
    "KEY_BLOCK_SIZE",
    "LANGUAGE",
    "LAST",
    "LEAVES",
    "LESS",
    "LEVEL",
    "LIST",
    "LOCAL",
    "LOGFILE",
    "LOGS",
    "MASTER",
    "MASTER_AUTO_POSITION",
    "MASTER_CONNECT_RETRY",
    "MASTER_DELAY",
    "MASTER_HEARTBEAT_PERIOD",
    "MASTER_HOST",
    "MASTER_LOG_FILE",
    "MASTER_LOG_POS",
    "MASTER_PASSWORD",
    "MASTER_PORT",
    "MASTER_RETRY_COUNT",
    "MASTER_SSL",
    "MASTER_SSL_CA",
    "MASTER_SSL_CAPATH",
    "MASTER_SSL_CERT",
    "MASTER_SSL_CIPHER",
    "MASTER_SSL_CRL",
    "MASTER_SSL_CRLPATH",
    "MASTER_SSL_KEY",
    "MASTER_TLS_VERSION",
    "MASTER_USER",
    "MAX_CONNECTIONS_PER_HOUR",
    "MAX_QUERIES_PER_HOUR",
    "MAX_ROWS",
    "MAX_SIZE",
    "MAX_UPDATES_PER_HOUR",
    "MAX_USER_CONNECTIONS",
    "MEDIUM",
    "MERGE",
    "MID",
    "MIGRATE",
    "MIN_ROWS",
    "MODE",
    "MODIFY",
    "MUTEX",
    "MYSQL",
    "NAME",
    "NAMES",
    "NCHAR",
    "NEVER",
    "NEXT",
    "NO",
    "NODEGROUP",
    "NONE",
    "OFFLINE",
    "OFFSET",
    "OJ",
    "OLD_PASSWORD",
    "ONE",
    "ONLINE",
    "ONLY",
    "OPEN",
    "OPTIMIZER_COSTS",
    "OPTIONS",
    "OWNER",
    "PACK_KEYS",
    "PAGE",
    "PARSER",
    "PARTIAL",
    "PARTITIONING",
    "PARTITIONS",
    "PASSWORD",
    "PHASE",
    "PLUGIN",
    "PLUGIN_DIR",
    "PLUGINS",
    "PORT",
    "PRECEDES",
    "PREPARE",
    "PRESERVE",
    "PREV",
    "PROCESSLIST",
    "PROFILE",
    "PROFILES",
    "PROXY",
    "QUERY",
    "QUICK",
    "REBUILD",
    "RECOVER",
    "REDO_BUFFER_SIZE",
    "REDUNDANT",
    "RELAY",
    "RELAY_LOG_FILE",
    "RELAY_LOG_POS",
    "RELAYLOG",
    "REMOVE",
    "REORGANIZE",
    "REPAIR",
    "REPLICATE_DO_DB",
    "REPLICATE_DO_TABLE",
    "REPLICATE_IGNORE_DB",
    "REPLICATE_IGNORE_TABLE",
    "REPLICATE_REWRITE_DB",
    "REPLICATE_WILD_DO_TABLE",
    "REPLICATE_WILD_IGNORE_TABLE",
    "REPLICATION",
    "RESET",
    "RESUME",
    "RETURNS",
    "ROLLBACK",
    "ROLLUP",
    "ROTATE",
    "ROW",
    "ROWS",
    "ROW_FORMAT",
    "SAVEPOINT",
    "SCHEDULE",
    "SECURITY",
    "SERVER",
    "SESSION",
    "SHARE",
    "SHARED",
    "SIGNED",
    "SIMPLE",
    "SLAVE",
    "SLOW",
    "SNAPSHOT",
    "SOCKET",
    "SOME",
    "SONAME",
    "SOUNDS",
    "SOURCE",
    "SQL_AFTER_GTIDS",
    "SQL_AFTER_MTS_GAPS",
    "SQL_BEFORE_GTIDS",
    "SQL_BUFFER_RESULT",
    "SQL_CACHE",
    "SQL_NO_CACHE",
    "SQL_THREAD",
    "START",
    "STARTS",
    "STATS_AUTO_RECALC",
    "STATS_PERSISTENT",
    "STATS_SAMPLE_PAGES",
    "STATUS",
    "STOP",
    "STORAGE",
    "STORED",
    "STRING",
    "SUBJECT",
    "SUBPARTITION",
    "SUBPARTITIONS",
    "SUSPEND",
    "SWAPS",
    "SWITCHES",
    "TABLESPACE",
    "TEMPORARY",
    "TEMPTABLE",
    "THAN",
    "TRADITIONAL",
    "TRANSACTION",
    "TRIGGERS",
    "TRUNCATE",
    "UNDEFINED",
    "UNDOFILE",
    "UNDO_BUFFER_SIZE",
    "UNINSTALL",
    "UNKNOWN",
    "UNTIL",
    "UPGRADE",
    "USER",
    "USE_FRM",
    "USER_RESOURCES",
    "VALIDATION",
    "VALUE",
    "VARIABLES",
    "VIEW",
    "VIRTUAL",
    "WAIT",
    "WARNINGS",
    "WITHOUT",
    "WORK",
    "WRAPPER",
    "X509",
    "XA",
    "XML",
    "EUR",
    "USA",
    "JIS",
    "ISO",
    "INTERNAL",
    "QUARTER",
    "MONTH",
    "DAY",
    "HOUR",
    "MINUTE",
    "WEEK",
    "SECOND",
    "MICROSECOND",
    "TABLES",
    "ROUTINE",
    "EXECUTE",
    "FILE",
    "PROCESS",
    "RELOAD",
    "SHUTDOWN",
    "SUPER",
    "PRIVILEGES",
    "ARMSCII8",
    "ASCII",
    "BIG5",
    "CP1250",
    "CP1251",
    "CP1256",
    "CP1257",
    "CP850",
    "CP852",
    "CP866",
    "CP932",
    "DEC8",
    "EUCJPMS",
    "EUCKR",
    "GB2312",
    "GBK",
    "GEOSTD8",
    "GREEK",
    "HEBREW",
    "HP8",
    "KEYBCS2",
    "KOI8R",
    "KOI8U",
    "LATIN1",
    "LATIN2",
    "LATIN5",
    "LATIN7",
    "MACCE",
    "MACROMAN",
    "SJIS",
    "SWE7",
    "TIS620",
    "UCS2",
    "UJIS",
    "UTF16",
    "UTF16LE",
    "UTF32",
    "UTF8",
    "UTF8MB3",
    "UTF8MB4",
    "ARCHIVE",
    "BLACKHOLE",
    "CSV",
    "FEDERATED",
    "INNODB",
    "MEMORY",
    "MRG_MYISAM",
    "MYISAM",
    "NDB",
    "NDBCLUSTER",
    "PERFOMANCE_SCHEMA",
    "REPEATABLE",
    "COMMITTED",
    "UNCOMMITTED",
    "SERIALIZABLE",
    "GEOMETRYCOLLECTION",
    "LINESTRING",
    "MULTILINESTRING",
    "MULTIPOINT",
    "MULTIPOLYGON",
    "POINT",
    "POLYGON",
    "ABS",
    "ACOS",
    "ADDDATE",
    "ADDTIME",
    "AES_DECRYPT",
    "AES_ENCRYPT",
    "AREA",
    "ASBINARY",
    "ASIN",
    "ASTEXT",
    "ASWKB",
    "ASWKT",
    "ASYMMETRIC_DECRYPT",
    "ASYMMETRIC_DERIVE",
    "ASYMMETRIC_ENCRYPT",
    "ASYMMETRIC_SIGN",
    "ASYMMETRIC_VERIFY",
    "ATAN",
    "ATAN2",
    "BENCHMARK",
    "BIN",
    "BIT_COUNT",
    "BIT_LENGTH",
    "BUFFER",
    "CEIL",
    "CEILING",
    "CENTROID",
    "CHARACTER_LENGTH",
    "CHARSET",
    "CHAR_LENGTH",
    "COERCIBILITY",
    "COLLATION",
    "COMPRESS",
    "CONCAT",
    "CONCAT_WS",
    "CONNECTION_ID",
    "CONV",
    "CONVERT_TZ",
    "COS",
    "COT",
    "CRC32",
    "CREATE_ASYMMETRIC_PRIV_KEY",
    "CREATE_ASYMMETRIC_PUB_KEY",
    "CREATE_DH_PARAMETERS",
    "CREATE_DIGEST",
    "CROSSES",
    "DATEDIFF",
    "DATE_FORMAT",
    "DAYNAME",
    "DAYOFMONTH",
    "DAYOFWEEK",
    "DAYOFYEAR",
    "DECODE",
    "DEGREES",
    "DES_DECRYPT",
    "DES_ENCRYPT",
    "DIMENSION",
    "DISJOINT",
    "ELT",
    "ENCODE",
    "ENCRYPT",
    "ENDPOINT",
    "ENVELOPE",
    "EQUALS",
    "EXP",
    "EXPORT_SET",
    "EXTERIORRING",
    "EXTRACTVALUE",
    "FIELD",
    "FIND_IN_SET",
    "FLOOR",
    "FORMAT",
    "FOUND_ROWS",
    "FROM_BASE64",
    "FROM_DAYS",
    "FROM_UNIXTIME",
    "GEOMCOLLFROMTEXT",
    "GEOMCOLLFROMWKB",
    "GEOMETRYCOLLECTIONFROMTEXT",
    "GEOMETRYCOLLECTIONFROMWKB",
    "GEOMETRYFROMTEXT",
    "GEOMETRYFROMWKB",
    "GEOMETRYN",
    "GEOMETRYTYPE",
    "GEOMFROMTEXT",
    "GEOMFROMWKB",
    "GET_FORMAT",
    "GET_LOCK",
    "GLENGTH",
    "GREATEST",
    "GTID_SUBSET",
    "GTID_SUBTRACT",
    "HEX",
    "IFNULL",
    "INET6_ATON",
    "INET6_NTOA",
    "INET_ATON",
    "INET_NTOA",
    "INSTR",
    "INTERIORRINGN",
    "INTERSECTS",
    "ISCLOSED",
    "ISEMPTY",
    "ISNULL",
    "ISSIMPLE",
    "IS_FREE_LOCK",
    "IS_IPV4",
    "IS_IPV4_COMPAT",
    "IS_IPV4_MAPPED",
    "IS_IPV6",
    "IS_USED_LOCK",
    "LAST_INSERT_ID",
    "LCASE",
    "LEAST",
    "LENGTH",
    "LINEFROMTEXT",
    "LINEFROMWKB",
    "LINESTRINGFROMTEXT",
    "LINESTRINGFROMWKB",
    "LN",
    "LOAD_FILE",
    "LOCATE",
    "LOG",
    "LOG10",
    "LOG2",
    "LOWER",
    "LPAD",
    "LTRIM",
    "MAKEDATE",
    "MAKETIME",
    "MAKE_SET",
    "MASTER_POS_WAIT",
    "MBRCONTAINS",
    "MBRDISJOINT",
    "MBREQUAL",
    "MBRINTERSECTS",
    "MBROVERLAPS",
    "MBRTOUCHES",
    "MBRWITHIN",
    "MD5",
    "MLINEFROMTEXT",
    "MLINEFROMWKB",
    "MONTHNAME",
    "MPOINTFROMTEXT",
    "MPOINTFROMWKB",
    "MPOLYFROMTEXT",
    "MPOLYFROMWKB",
    "MULTILINESTRINGFROMTEXT",
    "MULTILINESTRINGFROMWKB",
    "MULTIPOINTFROMTEXT",
    "MULTIPOINTFROMWKB",
    "MULTIPOLYGONFROMTEXT",
    "MULTIPOLYGONFROMWKB",
    "NAME_CONST",
    "NULLIF",
    "NUMGEOMETRIES",
    "NUMINTERIORRINGS",
    "NUMPOINTS",
    "OCT",
    "OCTET_LENGTH",
    "ORD",
    "OVERLAPS",
    "PERIOD_ADD",
    "PERIOD_DIFF",
    "PI",
    "POINTFROMTEXT",
    "POINTFROMWKB",
    "POINTN",
    "POLYFROMTEXT",
    "POLYFROMWKB",
    "POLYGONFROMTEXT",
    "POLYGONFROMWKB",
    "POW",
    "POWER",
    "QUOTE",
    "RADIANS",
    "RAND",
    "RANDOM_BYTES",
    "RELEASE_LOCK",
    "REVERSE",
    "ROUND",
    "ROW_COUNT",
    "RPAD",
    "RTRIM",
    "SEC_TO_TIME",
    "SESSION_USER",
    "SHA",
    "SHA1",
    "SHA2",
    "SIGN",
    "SIN",
    "SLEEP",
    "SOUNDEX",
    "SQL_THREAD_WAIT_AFTER_GTIDS",
    "SQRT",
    "SRID",
    "STARTPOINT",
    "STRCMP",
    "STR_TO_DATE",
    "ST_AREA",
    "ST_ASBINARY",
    "ST_ASTEXT",
    "ST_ASWKB",
    "ST_ASWKT",
    "ST_BUFFER",
    "ST_CENTROID",
    "ST_CONTAINS",
    "ST_CROSSES",
    "ST_DIFFERENCE",
    "ST_DIMENSION",
    "ST_DISJOINT",
    "ST_DISTANCE",
    "ST_ENDPOINT",
    "ST_ENVELOPE",
    "ST_EQUALS",
    "ST_EXTERIORRING",
    "ST_GEOMCOLLFROMTEXT",
    "ST_GEOMCOLLFROMTXT",
    "ST_GEOMCOLLFROMWKB",
    "ST_GEOMETRYCOLLECTIONFROMTEXT",
    "ST_GEOMETRYCOLLECTIONFROMWKB",
    "ST_GEOMETRYFROMTEXT",
    "ST_GEOMETRYFROMWKB",
    "ST_GEOMETRYN",
    "ST_GEOMETRYTYPE",
    "ST_GEOMFROMTEXT",
    "ST_GEOMFROMWKB",
    "ST_INTERIORRINGN",
    "ST_INTERSECTION",
    "ST_INTERSECTS",
    "ST_ISCLOSED",
    "ST_ISEMPTY",
    "ST_ISSIMPLE",
    "ST_LINEFROMTEXT",
    "ST_LINEFROMWKB",
    "ST_LINESTRINGFROMTEXT",
    "ST_LINESTRINGFROMWKB",
    "ST_NUMGEOMETRIES",
    "ST_NUMINTERIORRING",
    "ST_NUMINTERIORRINGS",
    "ST_NUMPOINTS",
    "ST_OVERLAPS",
    "ST_POINTFROMTEXT",
    "ST_POINTFROMWKB",
    "ST_POINTN",
    "ST_POLYFROMTEXT",
    "ST_POLYFROMWKB",
    "ST_POLYGONFROMTEXT",
    "ST_POLYGONFROMWKB",
    "ST_SRID",
    "ST_STARTPOINT",
    "ST_SYMDIFFERENCE",
    "ST_TOUCHES",
    "ST_UNION",
    "ST_WITHIN",
    "ST_X",
    "ST_Y",
    "SUBDATE",
    "SUBSTRING_INDEX",
    "SUBTIME",
    "SYSTEM_USER",
    "TAN",
    "TIMEDIFF",
    "TIMESTAMPADD",
    "TIMESTAMPDIFF",
    "TIME_FORMAT",
    "TIME_TO_SEC",
    "TOUCHES",
    "TO_BASE64",
    "TO_DAYS",
    "TO_SECONDS",
    "UCASE",
    "UNCOMPRESS",
    "UNCOMPRESSED_LENGTH",
    "UNHEX",
    "UNIX_TIMESTAMP",
    "UPDATEXML",
    "UPPER",
    "UUID",
    "UUID_SHORT",
    "VALIDATE_PASSWORD_STRENGTH",
    "VERSION",
    "WAIT_UNTIL_SQL_THREAD_AFTER_GTIDS",
    "WEEKDAY",
    "WEEKOFYEAR",
    "WEIGHT_STRING",
    "WITHIN",
    "YEARWEEK",
    "Y_FUNCTION",
    "X_FUNCTION",
    "VAR_ASSIGN",
    "PLUS_ASSIGN",
    "MINUS_ASSIGN",
    "MULT_ASSIGN",
    "DIV_ASSIGN",
    "MOD_ASSIGN",
    "AND_ASSIGN",
    "XOR_ASSIGN",
    "OR_ASSIGN",
    "STAR",
    "DIVIDE",
    "MODULE",
    "PLUS",
    "MINUSMINUS",
    "MINUS",
    "DIV",
    "MOD",
    "EQUAL_SYMBOL",
    "GREATER_SYMBOL",
    "LESS_SYMBOL",
    "EXCLAMATION_SYMBOL",
    "BIT_NOT_OP",
    "BIT_OR_OP",
    "BIT_AND_OP",
    "BIT_XOR_OP",
    "DOT",
    "LR_BRACKET",
    "RR_BRACKET",
    "COMMA",
    "SEMI",
    "AT_SIGN",
    "ZERO_DECIMAL",
    "ONE_DECIMAL",
    "TWO_DECIMAL",
    "SINGLE_QUOTE_SYMB",
    "DOUBLE_QUOTE_SYMB",
    "REVERSE_QUOTE_SYMB",
    "COLON_SYMB",
    "CHARSET_REVERSE_QOUTE_STRING",
    "FILESIZE_LITERAL",
    "STRING_LITERAL",
    "DECIMAL_LITERAL",
    "HEXADECIMAL_LITERAL",
    "REAL_LITERAL",
    "NULL_SPEC_LITERAL",
    "BIT_STRING",
    "STRING_CHARSET_NAME",
    "DOT_ID",
    "ID",
    "REVERSE_QUOTE_ID",
    "STRING_USER_NAME",
    "LOCAL_ID",
    "GLOBAL_ID",
    "ERROR_RECONGNIGION"
  };
  public static final Vocabulary VOCABULARY = new VocabularyImpl(_LITERAL_NAMES, _SYMBOLIC_NAMES);

  /** @deprecated Use {@link #VOCABULARY} instead. */
  @Deprecated public static final String[] tokenNames;

  static {
    tokenNames = new String[_SYMBOLIC_NAMES.length];
    for (int i = 0; i < tokenNames.length; i++) {
      tokenNames[i] = VOCABULARY.getLiteralName(i);
      if (tokenNames[i] == null) {
        tokenNames[i] = VOCABULARY.getSymbolicName(i);
      }

      if (tokenNames[i] == null) {
        tokenNames[i] = "<INVALID>";
      }
    }
  }

  @Override
  @Deprecated
  public String[] getTokenNames() {
    return tokenNames;
  }

  @Override
  public Vocabulary getVocabulary() {
    return VOCABULARY;
  }

  @Override
  public String getGrammarFileName() {
    return "MySqlParser.g4";
  }

  @Override
  public String[] getRuleNames() {
    return ruleNames;
  }

  @Override
  public String getSerializedATN() {
    return _serializedATN;
  }

  @Override
  public ATN getATN() {
    return _ATN;
  }

  public MySqlParser(TokenStream input) {
    super(input);
    _interp = new ParserATNSimulator(this, _ATN, _decisionToDFA, _sharedContextCache);
  }

  public static class IntervalTypeContext extends ParserRuleContext {
    public IntervalTypeBaseContext intervalTypeBase() {
      return getRuleContext(IntervalTypeBaseContext.class, 0);
    }

    public TerminalNode YEAR() {
      return getToken(MySqlParser.YEAR, 0);
    }

    public TerminalNode YEAR_MONTH() {
      return getToken(MySqlParser.YEAR_MONTH, 0);
    }

    public TerminalNode DAY_HOUR() {
      return getToken(MySqlParser.DAY_HOUR, 0);
    }

    public TerminalNode DAY_MINUTE() {
      return getToken(MySqlParser.DAY_MINUTE, 0);
    }

    public TerminalNode DAY_SECOND() {
      return getToken(MySqlParser.DAY_SECOND, 0);
    }

    public TerminalNode HOUR_MINUTE() {
      return getToken(MySqlParser.HOUR_MINUTE, 0);
    }

    public TerminalNode HOUR_SECOND() {
      return getToken(MySqlParser.HOUR_SECOND, 0);
    }

    public TerminalNode MINUTE_SECOND() {
      return getToken(MySqlParser.MINUTE_SECOND, 0);
    }

    public TerminalNode SECOND_MICROSECOND() {
      return getToken(MySqlParser.SECOND_MICROSECOND, 0);
    }

    public TerminalNode MINUTE_MICROSECOND() {
      return getToken(MySqlParser.MINUTE_MICROSECOND, 0);
    }

    public TerminalNode HOUR_MICROSECOND() {
      return getToken(MySqlParser.HOUR_MICROSECOND, 0);
    }

    public TerminalNode DAY_MICROSECOND() {
      return getToken(MySqlParser.DAY_MICROSECOND, 0);
    }

    public IntervalTypeContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_intervalType;
    }

    @Override
    public void enterRule(ParseTreeListener listener) {
      if (listener instanceof MySqlParserListener)
        ((MySqlParserListener) listener).enterIntervalType(this);
    }

    @Override
    public void exitRule(ParseTreeListener listener) {
      if (listener instanceof MySqlParserListener)
        ((MySqlParserListener) listener).exitIntervalType(this);
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof MySqlParserVisitor)
        return ((MySqlParserVisitor<? extends T>) visitor).visitIntervalType(this);
      else return visitor.visitChildren(this);
    }
  }

  public final IntervalTypeContext intervalType() throws RecognitionException {
    IntervalTypeContext _localctx = new IntervalTypeContext(_ctx, getState());
    enterRule(_localctx, 0, RULE_intervalType);
    try {
      setState(93);
      _errHandler.sync(this);
      switch (_input.LA(1)) {
        case QUARTER:
        case MONTH:
        case DAY:
        case HOUR:
        case MINUTE:
        case WEEK:
        case SECOND:
        case MICROSECOND:
          enterOuterAlt(_localctx, 1);
          {
            setState(80);
            intervalTypeBase();
          }
          break;
        case YEAR:
          enterOuterAlt(_localctx, 2);
          {
            setState(81);
            match(YEAR);
          }
          break;
        case YEAR_MONTH:
          enterOuterAlt(_localctx, 3);
          {
            setState(82);
            match(YEAR_MONTH);
          }
          break;
        case DAY_HOUR:
          enterOuterAlt(_localctx, 4);
          {
            setState(83);
            match(DAY_HOUR);
          }
          break;
        case DAY_MINUTE:
          enterOuterAlt(_localctx, 5);
          {
            setState(84);
            match(DAY_MINUTE);
          }
          break;
        case DAY_SECOND:
          enterOuterAlt(_localctx, 6);
          {
            setState(85);
            match(DAY_SECOND);
          }
          break;
        case HOUR_MINUTE:
          enterOuterAlt(_localctx, 7);
          {
            setState(86);
            match(HOUR_MINUTE);
          }
          break;
        case HOUR_SECOND:
          enterOuterAlt(_localctx, 8);
          {
            setState(87);
            match(HOUR_SECOND);
          }
          break;
        case MINUTE_SECOND:
          enterOuterAlt(_localctx, 9);
          {
            setState(88);
            match(MINUTE_SECOND);
          }
          break;
        case SECOND_MICROSECOND:
          enterOuterAlt(_localctx, 10);
          {
            setState(89);
            match(SECOND_MICROSECOND);
          }
          break;
        case MINUTE_MICROSECOND:
          enterOuterAlt(_localctx, 11);
          {
            setState(90);
            match(MINUTE_MICROSECOND);
          }
          break;
        case HOUR_MICROSECOND:
          enterOuterAlt(_localctx, 12);
          {
            setState(91);
            match(HOUR_MICROSECOND);
          }
          break;
        case DAY_MICROSECOND:
          enterOuterAlt(_localctx, 13);
          {
            setState(92);
            match(DAY_MICROSECOND);
          }
          break;
        default:
          throw new NoViableAltException(this);
      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  public static class FullIdContext extends ParserRuleContext {
    public List<UidContext> uid() {
      return getRuleContexts(UidContext.class);
    }

    public UidContext uid(int i) {
      return getRuleContext(UidContext.class, i);
    }

    public TerminalNode DOT_ID() {
      return getToken(MySqlParser.DOT_ID, 0);
    }

    public FullIdContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_fullId;
    }

    @Override
    public void enterRule(ParseTreeListener listener) {
      if (listener instanceof MySqlParserListener)
        ((MySqlParserListener) listener).enterFullId(this);
    }

    @Override
    public void exitRule(ParseTreeListener listener) {
      if (listener instanceof MySqlParserListener)
        ((MySqlParserListener) listener).exitFullId(this);
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof MySqlParserVisitor)
        return ((MySqlParserVisitor<? extends T>) visitor).visitFullId(this);
      else return visitor.visitChildren(this);
    }
  }

  public final FullIdContext fullId() throws RecognitionException {
    FullIdContext _localctx = new FullIdContext(_ctx, getState());
    enterRule(_localctx, 2, RULE_fullId);
    try {
      enterOuterAlt(_localctx, 1);
      {
        setState(95);
        uid();
        setState(99);
        _errHandler.sync(this);
        switch (_input.LA(1)) {
          case DOT_ID:
            {
              setState(96);
              match(DOT_ID);
            }
            break;
          case DOT:
            {
              setState(97);
              match(DOT);
              setState(98);
              uid();
            }
            break;
          case LR_BRACKET:
            break;
          default:
            break;
        }
      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  public static class FullColumnNameContext extends ParserRuleContext {
    public UidContext uid() {
      return getRuleContext(UidContext.class, 0);
    }

    public List<DottedIdContext> dottedId() {
      return getRuleContexts(DottedIdContext.class);
    }

    public DottedIdContext dottedId(int i) {
      return getRuleContext(DottedIdContext.class, i);
    }

    public FullColumnNameContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_fullColumnName;
    }

    @Override
    public void enterRule(ParseTreeListener listener) {
      if (listener instanceof MySqlParserListener)
        ((MySqlParserListener) listener).enterFullColumnName(this);
    }

    @Override
    public void exitRule(ParseTreeListener listener) {
      if (listener instanceof MySqlParserListener)
        ((MySqlParserListener) listener).exitFullColumnName(this);
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof MySqlParserVisitor)
        return ((MySqlParserVisitor<? extends T>) visitor).visitFullColumnName(this);
      else return visitor.visitChildren(this);
    }
  }

  public final FullColumnNameContext fullColumnName() throws RecognitionException {
    FullColumnNameContext _localctx = new FullColumnNameContext(_ctx, getState());
    enterRule(_localctx, 4, RULE_fullColumnName);
    try {
      enterOuterAlt(_localctx, 1);
      {
        setState(101);
        uid();
        setState(106);
        _errHandler.sync(this);
        switch (getInterpreter().adaptivePredict(_input, 3, _ctx)) {
          case 1:
            {
              setState(102);
              dottedId();
              setState(104);
              _errHandler.sync(this);
              switch (getInterpreter().adaptivePredict(_input, 2, _ctx)) {
                case 1:
                  {
                    setState(103);
                    dottedId();
                  }
                  break;
              }
            }
            break;
        }
      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  public static class CharsetNameContext extends ParserRuleContext {
    public TerminalNode BINARY() {
      return getToken(MySqlParser.BINARY, 0);
    }

    public CharsetNameBaseContext charsetNameBase() {
      return getRuleContext(CharsetNameBaseContext.class, 0);
    }

    public TerminalNode STRING_LITERAL() {
      return getToken(MySqlParser.STRING_LITERAL, 0);
    }

    public TerminalNode CHARSET_REVERSE_QOUTE_STRING() {
      return getToken(MySqlParser.CHARSET_REVERSE_QOUTE_STRING, 0);
    }

    public CharsetNameContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_charsetName;
    }

    @Override
    public void enterRule(ParseTreeListener listener) {
      if (listener instanceof MySqlParserListener)
        ((MySqlParserListener) listener).enterCharsetName(this);
    }

    @Override
    public void exitRule(ParseTreeListener listener) {
      if (listener instanceof MySqlParserListener)
        ((MySqlParserListener) listener).exitCharsetName(this);
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof MySqlParserVisitor)
        return ((MySqlParserVisitor<? extends T>) visitor).visitCharsetName(this);
      else return visitor.visitChildren(this);
    }
  }

  public final CharsetNameContext charsetName() throws RecognitionException {
    CharsetNameContext _localctx = new CharsetNameContext(_ctx, getState());
    enterRule(_localctx, 6, RULE_charsetName);
    try {
      setState(112);
      _errHandler.sync(this);
      switch (_input.LA(1)) {
        case BINARY:
          enterOuterAlt(_localctx, 1);
          {
            setState(108);
            match(BINARY);
          }
          break;
        case ARMSCII8:
        case ASCII:
        case BIG5:
        case CP1250:
        case CP1251:
        case CP1256:
        case CP1257:
        case CP850:
        case CP852:
        case CP866:
        case CP932:
        case DEC8:
        case EUCJPMS:
        case EUCKR:
        case GB2312:
        case GBK:
        case GEOSTD8:
        case GREEK:
        case HEBREW:
        case HP8:
        case KEYBCS2:
        case KOI8R:
        case KOI8U:
        case LATIN1:
        case LATIN2:
        case LATIN5:
        case LATIN7:
        case MACCE:
        case MACROMAN:
        case SJIS:
        case SWE7:
        case TIS620:
        case UCS2:
        case UJIS:
        case UTF16:
        case UTF16LE:
        case UTF32:
        case UTF8:
        case UTF8MB3:
        case UTF8MB4:
          enterOuterAlt(_localctx, 2);
          {
            setState(109);
            charsetNameBase();
          }
          break;
        case STRING_LITERAL:
          enterOuterAlt(_localctx, 3);
          {
            setState(110);
            match(STRING_LITERAL);
          }
          break;
        case CHARSET_REVERSE_QOUTE_STRING:
          enterOuterAlt(_localctx, 4);
          {
            setState(111);
            match(CHARSET_REVERSE_QOUTE_STRING);
          }
          break;
        default:
          throw new NoViableAltException(this);
      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  public static class CollationNameContext extends ParserRuleContext {
    public UidContext uid() {
      return getRuleContext(UidContext.class, 0);
    }

    public TerminalNode STRING_LITERAL() {
      return getToken(MySqlParser.STRING_LITERAL, 0);
    }

    public CollationNameContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_collationName;
    }

    @Override
    public void enterRule(ParseTreeListener listener) {
      if (listener instanceof MySqlParserListener)
        ((MySqlParserListener) listener).enterCollationName(this);
    }

    @Override
    public void exitRule(ParseTreeListener listener) {
      if (listener instanceof MySqlParserListener)
        ((MySqlParserListener) listener).exitCollationName(this);
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof MySqlParserVisitor)
        return ((MySqlParserVisitor<? extends T>) visitor).visitCollationName(this);
      else return visitor.visitChildren(this);
    }
  }

  public final CollationNameContext collationName() throws RecognitionException {
    CollationNameContext _localctx = new CollationNameContext(_ctx, getState());
    enterRule(_localctx, 8, RULE_collationName);
    try {
      setState(116);
      _errHandler.sync(this);
      switch (_input.LA(1)) {
        case DATABASE:
        case LEFT:
        case RIGHT:
        case DATE:
        case TIME:
        case TIMESTAMP:
        case DATETIME:
        case YEAR:
        case TEXT:
        case ENUM:
        case COUNT:
        case POSITION:
        case ACCOUNT:
        case ACTION:
        case AFTER:
        case AGGREGATE:
        case ALGORITHM:
        case ANY:
        case AT:
        case AUTHORS:
        case AUTOCOMMIT:
        case AUTOEXTEND_SIZE:
        case AUTO_INCREMENT:
        case AVG_ROW_LENGTH:
        case BEGIN:
        case BINLOG:
        case BIT:
        case BLOCK:
        case BOOL:
        case BOOLEAN:
        case BTREE:
        case CASCADED:
        case CHAIN:
        case CHANGED:
        case CHANNEL:
        case CHECKSUM:
        case CIPHER:
        case CLIENT:
        case COALESCE:
        case CODE:
        case COLUMNS:
        case COLUMN_FORMAT:
        case COMMENT:
        case COMMIT:
        case COMPACT:
        case COMPLETION:
        case COMPRESSED:
        case COMPRESSION:
        case CONCURRENT:
        case CONNECTION:
        case CONSISTENT:
        case CONTAINS:
        case CONTEXT:
        case CONTRIBUTORS:
        case COPY:
        case CPU:
        case DATA:
        case DATAFILE:
        case DEALLOCATE:
        case DEFAULT_AUTH:
        case DEFINER:
        case DELAY_KEY_WRITE:
        case DIRECTORY:
        case DISABLE:
        case DISCARD:
        case DISK:
        case DO:
        case DUMPFILE:
        case DUPLICATE:
        case DYNAMIC:
        case ENABLE:
        case ENCRYPTION:
        case ENDS:
        case ENGINE:
        case ENGINES:
        case ERROR:
        case ERRORS:
        case ESCAPE:
        case EVEN:
        case EVENT:
        case EVENTS:
        case EVERY:
        case EXCHANGE:
        case EXCLUSIVE:
        case EXPIRE:
        case EXTENDED:
        case EXTENT_SIZE:
        case FAST:
        case FAULTS:
        case FIELDS:
        case FILE_BLOCK_SIZE:
        case FILTER:
        case FIRST:
        case FIXED:
        case FOLLOWS:
        case FULL:
        case FUNCTION:
        case GLOBAL:
        case GRANTS:
        case GROUP_REPLICATION:
        case HASH:
        case HOST:
        case IDENTIFIED:
        case IGNORE_SERVER_IDS:
        case IMPORT:
        case INDEXES:
        case INITIAL_SIZE:
        case INPLACE:
        case INSERT_METHOD:
        case INSTANCE:
        case INVOKER:
        case IO:
        case IO_THREAD:
        case IPC:
        case ISOLATION:
        case ISSUER:
        case KEY_BLOCK_SIZE:
        case LANGUAGE:
        case LAST:
        case LEAVES:
        case LESS:
        case LEVEL:
        case LIST:
        case LOCAL:
        case LOGFILE:
        case LOGS:
        case MASTER:
        case MASTER_AUTO_POSITION:
        case MASTER_CONNECT_RETRY:
        case MASTER_DELAY:
        case MASTER_HEARTBEAT_PERIOD:
        case MASTER_HOST:
        case MASTER_LOG_FILE:
        case MASTER_LOG_POS:
        case MASTER_PASSWORD:
        case MASTER_PORT:
        case MASTER_RETRY_COUNT:
        case MASTER_SSL:
        case MASTER_SSL_CA:
        case MASTER_SSL_CAPATH:
        case MASTER_SSL_CERT:
        case MASTER_SSL_CIPHER:
        case MASTER_SSL_CRL:
        case MASTER_SSL_CRLPATH:
        case MASTER_SSL_KEY:
        case MASTER_TLS_VERSION:
        case MASTER_USER:
        case MAX_CONNECTIONS_PER_HOUR:
        case MAX_QUERIES_PER_HOUR:
        case MAX_ROWS:
        case MAX_SIZE:
        case MAX_UPDATES_PER_HOUR:
        case MAX_USER_CONNECTIONS:
        case MEDIUM:
        case MERGE:
        case MID:
        case MIGRATE:
        case MIN_ROWS:
        case MODIFY:
        case MUTEX:
        case MYSQL:
        case NAME:
        case NAMES:
        case NCHAR:
        case NEVER:
        case NO:
        case NODEGROUP:
        case NONE:
        case OFFLINE:
        case OFFSET:
        case OJ:
        case OLD_PASSWORD:
        case ONE:
        case ONLINE:
        case ONLY:
        case OPTIMIZER_COSTS:
        case OPTIONS:
        case OWNER:
        case PACK_KEYS:
        case PAGE:
        case PARSER:
        case PARTIAL:
        case PARTITIONING:
        case PARTITIONS:
        case PASSWORD:
        case PHASE:
        case PLUGIN_DIR:
        case PLUGINS:
        case PORT:
        case PRECEDES:
        case PREPARE:
        case PRESERVE:
        case PREV:
        case PROCESSLIST:
        case PROFILE:
        case PROFILES:
        case PROXY:
        case QUERY:
        case QUICK:
        case REBUILD:
        case RECOVER:
        case REDO_BUFFER_SIZE:
        case REDUNDANT:
        case RELAY_LOG_FILE:
        case RELAY_LOG_POS:
        case RELAYLOG:
        case REMOVE:
        case REORGANIZE:
        case REPAIR:
        case REPLICATE_DO_DB:
        case REPLICATE_DO_TABLE:
        case REPLICATE_IGNORE_DB:
        case REPLICATE_IGNORE_TABLE:
        case REPLICATE_REWRITE_DB:
        case REPLICATE_WILD_DO_TABLE:
        case REPLICATE_WILD_IGNORE_TABLE:
        case REPLICATION:
        case RESUME:
        case RETURNS:
        case ROLLBACK:
        case ROLLUP:
        case ROTATE:
        case ROW:
        case ROWS:
        case ROW_FORMAT:
        case SAVEPOINT:
        case SCHEDULE:
        case SECURITY:
        case SERVER:
        case SESSION:
        case SHARE:
        case SHARED:
        case SIGNED:
        case SIMPLE:
        case SLAVE:
        case SNAPSHOT:
        case SOCKET:
        case SOME:
        case SOUNDS:
        case SOURCE:
        case SQL_AFTER_GTIDS:
        case SQL_AFTER_MTS_GAPS:
        case SQL_BEFORE_GTIDS:
        case SQL_BUFFER_RESULT:
        case SQL_CACHE:
        case SQL_NO_CACHE:
        case SQL_THREAD:
        case START:
        case STARTS:
        case STATS_AUTO_RECALC:
        case STATS_PERSISTENT:
        case STATS_SAMPLE_PAGES:
        case STATUS:
        case STOP:
        case STORAGE:
        case STRING:
        case SUBJECT:
        case SUBPARTITION:
        case SUBPARTITIONS:
        case SUSPEND:
        case SWAPS:
        case SWITCHES:
        case TABLESPACE:
        case TEMPORARY:
        case TEMPTABLE:
        case THAN:
        case TRANSACTION:
        case TRUNCATE:
        case UNDEFINED:
        case UNDOFILE:
        case UNDO_BUFFER_SIZE:
        case UNKNOWN:
        case UPGRADE:
        case USER:
        case VALIDATION:
        case VALUE:
        case VARIABLES:
        case VIEW:
        case WAIT:
        case WARNINGS:
        case WITHOUT:
        case WORK:
        case WRAPPER:
        case X509:
        case XA:
        case XML:
        case QUARTER:
        case MONTH:
        case DAY:
        case HOUR:
        case MINUTE:
        case WEEK:
        case SECOND:
        case MICROSECOND:
        case ARMSCII8:
        case ASCII:
        case BIG5:
        case CP1250:
        case CP1251:
        case CP1256:
        case CP1257:
        case CP850:
        case CP852:
        case CP866:
        case CP932:
        case DEC8:
        case EUCJPMS:
        case EUCKR:
        case GB2312:
        case GBK:
        case GEOSTD8:
        case GREEK:
        case HEBREW:
        case HP8:
        case KEYBCS2:
        case KOI8R:
        case KOI8U:
        case LATIN1:
        case LATIN2:
        case LATIN5:
        case LATIN7:
        case MACCE:
        case MACROMAN:
        case SJIS:
        case SWE7:
        case TIS620:
        case UCS2:
        case UJIS:
        case UTF16:
        case UTF16LE:
        case UTF32:
        case UTF8:
        case UTF8MB3:
        case UTF8MB4:
        case MEMORY:
        case GEOMETRYCOLLECTION:
        case LINESTRING:
        case MULTILINESTRING:
        case MULTIPOINT:
        case MULTIPOLYGON:
        case POINT:
        case POLYGON:
        case ABS:
        case ACOS:
        case ADDDATE:
        case ADDTIME:
        case AES_DECRYPT:
        case AES_ENCRYPT:
        case AREA:
        case ASBINARY:
        case ASIN:
        case ASTEXT:
        case ASWKB:
        case ASWKT:
        case ASYMMETRIC_DECRYPT:
        case ASYMMETRIC_DERIVE:
        case ASYMMETRIC_ENCRYPT:
        case ASYMMETRIC_SIGN:
        case ASYMMETRIC_VERIFY:
        case ATAN:
        case ATAN2:
        case BENCHMARK:
        case BIN:
        case BIT_COUNT:
        case BIT_LENGTH:
        case BUFFER:
        case CEIL:
        case CEILING:
        case CENTROID:
        case CHARACTER_LENGTH:
        case CHARSET:
        case CHAR_LENGTH:
        case COERCIBILITY:
        case COLLATION:
        case COMPRESS:
        case CONCAT:
        case CONCAT_WS:
        case CONNECTION_ID:
        case CONV:
        case CONVERT_TZ:
        case COS:
        case COT:
        case CRC32:
        case CREATE_ASYMMETRIC_PRIV_KEY:
        case CREATE_ASYMMETRIC_PUB_KEY:
        case CREATE_DH_PARAMETERS:
        case CREATE_DIGEST:
        case CROSSES:
        case DATEDIFF:
        case DATE_FORMAT:
        case DAYNAME:
        case DAYOFMONTH:
        case DAYOFWEEK:
        case DAYOFYEAR:
        case DECODE:
        case DEGREES:
        case DES_DECRYPT:
        case DES_ENCRYPT:
        case DIMENSION:
        case DISJOINT:
        case ELT:
        case ENCODE:
        case ENCRYPT:
        case ENDPOINT:
        case ENVELOPE:
        case EQUALS:
        case EXP:
        case EXPORT_SET:
        case EXTERIORRING:
        case EXTRACTVALUE:
        case FIELD:
        case FIND_IN_SET:
        case FLOOR:
        case FORMAT:
        case FOUND_ROWS:
        case FROM_BASE64:
        case FROM_DAYS:
        case FROM_UNIXTIME:
        case GEOMCOLLFROMTEXT:
        case GEOMCOLLFROMWKB:
        case GEOMETRYCOLLECTIONFROMTEXT:
        case GEOMETRYCOLLECTIONFROMWKB:
        case GEOMETRYFROMTEXT:
        case GEOMETRYFROMWKB:
        case GEOMETRYN:
        case GEOMETRYTYPE:
        case GEOMFROMTEXT:
        case GEOMFROMWKB:
        case GET_FORMAT:
        case GET_LOCK:
        case GLENGTH:
        case GREATEST:
        case GTID_SUBSET:
        case GTID_SUBTRACT:
        case HEX:
        case IFNULL:
        case INET6_ATON:
        case INET6_NTOA:
        case INET_ATON:
        case INET_NTOA:
        case INSTR:
        case INTERIORRINGN:
        case INTERSECTS:
        case ISCLOSED:
        case ISEMPTY:
        case ISNULL:
        case ISSIMPLE:
        case IS_FREE_LOCK:
        case IS_IPV4:
        case IS_IPV4_COMPAT:
        case IS_IPV4_MAPPED:
        case IS_IPV6:
        case IS_USED_LOCK:
        case LAST_INSERT_ID:
        case LCASE:
        case LEAST:
        case LENGTH:
        case LINEFROMTEXT:
        case LINEFROMWKB:
        case LINESTRINGFROMTEXT:
        case LINESTRINGFROMWKB:
        case LN:
        case LOAD_FILE:
        case LOCATE:
        case LOG:
        case LOG10:
        case LOG2:
        case LOWER:
        case LPAD:
        case LTRIM:
        case MAKEDATE:
        case MAKETIME:
        case MAKE_SET:
        case MASTER_POS_WAIT:
        case MBRCONTAINS:
        case MBRDISJOINT:
        case MBREQUAL:
        case MBRINTERSECTS:
        case MBROVERLAPS:
        case MBRTOUCHES:
        case MBRWITHIN:
        case MD5:
        case MLINEFROMTEXT:
        case MLINEFROMWKB:
        case MONTHNAME:
        case MPOINTFROMTEXT:
        case MPOINTFROMWKB:
        case MPOLYFROMTEXT:
        case MPOLYFROMWKB:
        case MULTILINESTRINGFROMTEXT:
        case MULTILINESTRINGFROMWKB:
        case MULTIPOINTFROMTEXT:
        case MULTIPOINTFROMWKB:
        case MULTIPOLYGONFROMTEXT:
        case MULTIPOLYGONFROMWKB:
        case NAME_CONST:
        case NULLIF:
        case NUMGEOMETRIES:
        case NUMINTERIORRINGS:
        case NUMPOINTS:
        case OCT:
        case OCTET_LENGTH:
        case ORD:
        case OVERLAPS:
        case PERIOD_ADD:
        case PERIOD_DIFF:
        case PI:
        case POINTFROMTEXT:
        case POINTFROMWKB:
        case POINTN:
        case POLYFROMTEXT:
        case POLYFROMWKB:
        case POLYGONFROMTEXT:
        case POLYGONFROMWKB:
        case POW:
        case POWER:
        case QUOTE:
        case RADIANS:
        case RAND:
        case RANDOM_BYTES:
        case RELEASE_LOCK:
        case REVERSE:
        case ROUND:
        case ROW_COUNT:
        case RPAD:
        case RTRIM:
        case SEC_TO_TIME:
        case SESSION_USER:
        case SHA:
        case SHA1:
        case SHA2:
        case SIGN:
        case SIN:
        case SLEEP:
        case SOUNDEX:
        case SQL_THREAD_WAIT_AFTER_GTIDS:
        case SQRT:
        case SRID:
        case STARTPOINT:
        case STRCMP:
        case STR_TO_DATE:
        case ST_AREA:
        case ST_ASBINARY:
        case ST_ASTEXT:
        case ST_ASWKB:
        case ST_ASWKT:
        case ST_BUFFER:
        case ST_CENTROID:
        case ST_CONTAINS:
        case ST_CROSSES:
        case ST_DIFFERENCE:
        case ST_DIMENSION:
        case ST_DISJOINT:
        case ST_DISTANCE:
        case ST_ENDPOINT:
        case ST_ENVELOPE:
        case ST_EQUALS:
        case ST_EXTERIORRING:
        case ST_GEOMCOLLFROMTEXT:
        case ST_GEOMCOLLFROMTXT:
        case ST_GEOMCOLLFROMWKB:
        case ST_GEOMETRYCOLLECTIONFROMTEXT:
        case ST_GEOMETRYCOLLECTIONFROMWKB:
        case ST_GEOMETRYFROMTEXT:
        case ST_GEOMETRYFROMWKB:
        case ST_GEOMETRYN:
        case ST_GEOMETRYTYPE:
        case ST_GEOMFROMTEXT:
        case ST_GEOMFROMWKB:
        case ST_INTERIORRINGN:
        case ST_INTERSECTION:
        case ST_INTERSECTS:
        case ST_ISCLOSED:
        case ST_ISEMPTY:
        case ST_ISSIMPLE:
        case ST_LINEFROMTEXT:
        case ST_LINEFROMWKB:
        case ST_LINESTRINGFROMTEXT:
        case ST_LINESTRINGFROMWKB:
        case ST_NUMGEOMETRIES:
        case ST_NUMINTERIORRING:
        case ST_NUMINTERIORRINGS:
        case ST_NUMPOINTS:
        case ST_OVERLAPS:
        case ST_POINTFROMTEXT:
        case ST_POINTFROMWKB:
        case ST_POINTN:
        case ST_POLYFROMTEXT:
        case ST_POLYFROMWKB:
        case ST_POLYGONFROMTEXT:
        case ST_POLYGONFROMWKB:
        case ST_SRID:
        case ST_STARTPOINT:
        case ST_SYMDIFFERENCE:
        case ST_TOUCHES:
        case ST_UNION:
        case ST_WITHIN:
        case ST_X:
        case ST_Y:
        case SUBDATE:
        case SUBSTRING_INDEX:
        case SUBTIME:
        case SYSTEM_USER:
        case TAN:
        case TIMEDIFF:
        case TIMESTAMPADD:
        case TIMESTAMPDIFF:
        case TIME_FORMAT:
        case TIME_TO_SEC:
        case TOUCHES:
        case TO_BASE64:
        case TO_DAYS:
        case TO_SECONDS:
        case UCASE:
        case UNCOMPRESS:
        case UNCOMPRESSED_LENGTH:
        case UNHEX:
        case UNIX_TIMESTAMP:
        case UPDATEXML:
        case UPPER:
        case UUID:
        case UUID_SHORT:
        case VALIDATE_PASSWORD_STRENGTH:
        case VERSION:
        case WAIT_UNTIL_SQL_THREAD_AFTER_GTIDS:
        case WEEKDAY:
        case WEEKOFYEAR:
        case WEIGHT_STRING:
        case WITHIN:
        case YEARWEEK:
        case Y_FUNCTION:
        case X_FUNCTION:
        case CHARSET_REVERSE_QOUTE_STRING:
        case ID:
        case REVERSE_QUOTE_ID:
          enterOuterAlt(_localctx, 1);
          {
            setState(114);
            uid();
          }
          break;
        case STRING_LITERAL:
          enterOuterAlt(_localctx, 2);
          {
            setState(115);
            match(STRING_LITERAL);
          }
          break;
        default:
          throw new NoViableAltException(this);
      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  public static class UidContext extends ParserRuleContext {
    public SimpleIdContext simpleId() {
      return getRuleContext(SimpleIdContext.class, 0);
    }

    public TerminalNode REVERSE_QUOTE_ID() {
      return getToken(MySqlParser.REVERSE_QUOTE_ID, 0);
    }

    public TerminalNode CHARSET_REVERSE_QOUTE_STRING() {
      return getToken(MySqlParser.CHARSET_REVERSE_QOUTE_STRING, 0);
    }

    public UidContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_uid;
    }

    @Override
    public void enterRule(ParseTreeListener listener) {
      if (listener instanceof MySqlParserListener) ((MySqlParserListener) listener).enterUid(this);
    }

    @Override
    public void exitRule(ParseTreeListener listener) {
      if (listener instanceof MySqlParserListener) ((MySqlParserListener) listener).exitUid(this);
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof MySqlParserVisitor)
        return ((MySqlParserVisitor<? extends T>) visitor).visitUid(this);
      else return visitor.visitChildren(this);
    }
  }

  public final UidContext uid() throws RecognitionException {
    UidContext _localctx = new UidContext(_ctx, getState());
    enterRule(_localctx, 10, RULE_uid);
    try {
      setState(121);
      _errHandler.sync(this);
      switch (_input.LA(1)) {
        case DATABASE:
        case LEFT:
        case RIGHT:
        case DATE:
        case TIME:
        case TIMESTAMP:
        case DATETIME:
        case YEAR:
        case TEXT:
        case ENUM:
        case COUNT:
        case POSITION:
        case ACCOUNT:
        case ACTION:
        case AFTER:
        case AGGREGATE:
        case ALGORITHM:
        case ANY:
        case AT:
        case AUTHORS:
        case AUTOCOMMIT:
        case AUTOEXTEND_SIZE:
        case AUTO_INCREMENT:
        case AVG_ROW_LENGTH:
        case BEGIN:
        case BINLOG:
        case BIT:
        case BLOCK:
        case BOOL:
        case BOOLEAN:
        case BTREE:
        case CASCADED:
        case CHAIN:
        case CHANGED:
        case CHANNEL:
        case CHECKSUM:
        case CIPHER:
        case CLIENT:
        case COALESCE:
        case CODE:
        case COLUMNS:
        case COLUMN_FORMAT:
        case COMMENT:
        case COMMIT:
        case COMPACT:
        case COMPLETION:
        case COMPRESSED:
        case COMPRESSION:
        case CONCURRENT:
        case CONNECTION:
        case CONSISTENT:
        case CONTAINS:
        case CONTEXT:
        case CONTRIBUTORS:
        case COPY:
        case CPU:
        case DATA:
        case DATAFILE:
        case DEALLOCATE:
        case DEFAULT_AUTH:
        case DEFINER:
        case DELAY_KEY_WRITE:
        case DIRECTORY:
        case DISABLE:
        case DISCARD:
        case DISK:
        case DO:
        case DUMPFILE:
        case DUPLICATE:
        case DYNAMIC:
        case ENABLE:
        case ENCRYPTION:
        case ENDS:
        case ENGINE:
        case ENGINES:
        case ERROR:
        case ERRORS:
        case ESCAPE:
        case EVEN:
        case EVENT:
        case EVENTS:
        case EVERY:
        case EXCHANGE:
        case EXCLUSIVE:
        case EXPIRE:
        case EXTENDED:
        case EXTENT_SIZE:
        case FAST:
        case FAULTS:
        case FIELDS:
        case FILE_BLOCK_SIZE:
        case FILTER:
        case FIRST:
        case FIXED:
        case FOLLOWS:
        case FULL:
        case FUNCTION:
        case GLOBAL:
        case GRANTS:
        case GROUP_REPLICATION:
        case HASH:
        case HOST:
        case IDENTIFIED:
        case IGNORE_SERVER_IDS:
        case IMPORT:
        case INDEXES:
        case INITIAL_SIZE:
        case INPLACE:
        case INSERT_METHOD:
        case INSTANCE:
        case INVOKER:
        case IO:
        case IO_THREAD:
        case IPC:
        case ISOLATION:
        case ISSUER:
        case KEY_BLOCK_SIZE:
        case LANGUAGE:
        case LAST:
        case LEAVES:
        case LESS:
        case LEVEL:
        case LIST:
        case LOCAL:
        case LOGFILE:
        case LOGS:
        case MASTER:
        case MASTER_AUTO_POSITION:
        case MASTER_CONNECT_RETRY:
        case MASTER_DELAY:
        case MASTER_HEARTBEAT_PERIOD:
        case MASTER_HOST:
        case MASTER_LOG_FILE:
        case MASTER_LOG_POS:
        case MASTER_PASSWORD:
        case MASTER_PORT:
        case MASTER_RETRY_COUNT:
        case MASTER_SSL:
        case MASTER_SSL_CA:
        case MASTER_SSL_CAPATH:
        case MASTER_SSL_CERT:
        case MASTER_SSL_CIPHER:
        case MASTER_SSL_CRL:
        case MASTER_SSL_CRLPATH:
        case MASTER_SSL_KEY:
        case MASTER_TLS_VERSION:
        case MASTER_USER:
        case MAX_CONNECTIONS_PER_HOUR:
        case MAX_QUERIES_PER_HOUR:
        case MAX_ROWS:
        case MAX_SIZE:
        case MAX_UPDATES_PER_HOUR:
        case MAX_USER_CONNECTIONS:
        case MEDIUM:
        case MERGE:
        case MID:
        case MIGRATE:
        case MIN_ROWS:
        case MODIFY:
        case MUTEX:
        case MYSQL:
        case NAME:
        case NAMES:
        case NCHAR:
        case NEVER:
        case NO:
        case NODEGROUP:
        case NONE:
        case OFFLINE:
        case OFFSET:
        case OJ:
        case OLD_PASSWORD:
        case ONE:
        case ONLINE:
        case ONLY:
        case OPTIMIZER_COSTS:
        case OPTIONS:
        case OWNER:
        case PACK_KEYS:
        case PAGE:
        case PARSER:
        case PARTIAL:
        case PARTITIONING:
        case PARTITIONS:
        case PASSWORD:
        case PHASE:
        case PLUGIN_DIR:
        case PLUGINS:
        case PORT:
        case PRECEDES:
        case PREPARE:
        case PRESERVE:
        case PREV:
        case PROCESSLIST:
        case PROFILE:
        case PROFILES:
        case PROXY:
        case QUERY:
        case QUICK:
        case REBUILD:
        case RECOVER:
        case REDO_BUFFER_SIZE:
        case REDUNDANT:
        case RELAY_LOG_FILE:
        case RELAY_LOG_POS:
        case RELAYLOG:
        case REMOVE:
        case REORGANIZE:
        case REPAIR:
        case REPLICATE_DO_DB:
        case REPLICATE_DO_TABLE:
        case REPLICATE_IGNORE_DB:
        case REPLICATE_IGNORE_TABLE:
        case REPLICATE_REWRITE_DB:
        case REPLICATE_WILD_DO_TABLE:
        case REPLICATE_WILD_IGNORE_TABLE:
        case REPLICATION:
        case RESUME:
        case RETURNS:
        case ROLLBACK:
        case ROLLUP:
        case ROTATE:
        case ROW:
        case ROWS:
        case ROW_FORMAT:
        case SAVEPOINT:
        case SCHEDULE:
        case SECURITY:
        case SERVER:
        case SESSION:
        case SHARE:
        case SHARED:
        case SIGNED:
        case SIMPLE:
        case SLAVE:
        case SNAPSHOT:
        case SOCKET:
        case SOME:
        case SOUNDS:
        case SOURCE:
        case SQL_AFTER_GTIDS:
        case SQL_AFTER_MTS_GAPS:
        case SQL_BEFORE_GTIDS:
        case SQL_BUFFER_RESULT:
        case SQL_CACHE:
        case SQL_NO_CACHE:
        case SQL_THREAD:
        case START:
        case STARTS:
        case STATS_AUTO_RECALC:
        case STATS_PERSISTENT:
        case STATS_SAMPLE_PAGES:
        case STATUS:
        case STOP:
        case STORAGE:
        case STRING:
        case SUBJECT:
        case SUBPARTITION:
        case SUBPARTITIONS:
        case SUSPEND:
        case SWAPS:
        case SWITCHES:
        case TABLESPACE:
        case TEMPORARY:
        case TEMPTABLE:
        case THAN:
        case TRANSACTION:
        case TRUNCATE:
        case UNDEFINED:
        case UNDOFILE:
        case UNDO_BUFFER_SIZE:
        case UNKNOWN:
        case UPGRADE:
        case USER:
        case VALIDATION:
        case VALUE:
        case VARIABLES:
        case VIEW:
        case WAIT:
        case WARNINGS:
        case WITHOUT:
        case WORK:
        case WRAPPER:
        case X509:
        case XA:
        case XML:
        case QUARTER:
        case MONTH:
        case DAY:
        case HOUR:
        case MINUTE:
        case WEEK:
        case SECOND:
        case MICROSECOND:
        case ARMSCII8:
        case ASCII:
        case BIG5:
        case CP1250:
        case CP1251:
        case CP1256:
        case CP1257:
        case CP850:
        case CP852:
        case CP866:
        case CP932:
        case DEC8:
        case EUCJPMS:
        case EUCKR:
        case GB2312:
        case GBK:
        case GEOSTD8:
        case GREEK:
        case HEBREW:
        case HP8:
        case KEYBCS2:
        case KOI8R:
        case KOI8U:
        case LATIN1:
        case LATIN2:
        case LATIN5:
        case LATIN7:
        case MACCE:
        case MACROMAN:
        case SJIS:
        case SWE7:
        case TIS620:
        case UCS2:
        case UJIS:
        case UTF16:
        case UTF16LE:
        case UTF32:
        case UTF8:
        case UTF8MB3:
        case UTF8MB4:
        case MEMORY:
        case GEOMETRYCOLLECTION:
        case LINESTRING:
        case MULTILINESTRING:
        case MULTIPOINT:
        case MULTIPOLYGON:
        case POINT:
        case POLYGON:
        case ABS:
        case ACOS:
        case ADDDATE:
        case ADDTIME:
        case AES_DECRYPT:
        case AES_ENCRYPT:
        case AREA:
        case ASBINARY:
        case ASIN:
        case ASTEXT:
        case ASWKB:
        case ASWKT:
        case ASYMMETRIC_DECRYPT:
        case ASYMMETRIC_DERIVE:
        case ASYMMETRIC_ENCRYPT:
        case ASYMMETRIC_SIGN:
        case ASYMMETRIC_VERIFY:
        case ATAN:
        case ATAN2:
        case BENCHMARK:
        case BIN:
        case BIT_COUNT:
        case BIT_LENGTH:
        case BUFFER:
        case CEIL:
        case CEILING:
        case CENTROID:
        case CHARACTER_LENGTH:
        case CHARSET:
        case CHAR_LENGTH:
        case COERCIBILITY:
        case COLLATION:
        case COMPRESS:
        case CONCAT:
        case CONCAT_WS:
        case CONNECTION_ID:
        case CONV:
        case CONVERT_TZ:
        case COS:
        case COT:
        case CRC32:
        case CREATE_ASYMMETRIC_PRIV_KEY:
        case CREATE_ASYMMETRIC_PUB_KEY:
        case CREATE_DH_PARAMETERS:
        case CREATE_DIGEST:
        case CROSSES:
        case DATEDIFF:
        case DATE_FORMAT:
        case DAYNAME:
        case DAYOFMONTH:
        case DAYOFWEEK:
        case DAYOFYEAR:
        case DECODE:
        case DEGREES:
        case DES_DECRYPT:
        case DES_ENCRYPT:
        case DIMENSION:
        case DISJOINT:
        case ELT:
        case ENCODE:
        case ENCRYPT:
        case ENDPOINT:
        case ENVELOPE:
        case EQUALS:
        case EXP:
        case EXPORT_SET:
        case EXTERIORRING:
        case EXTRACTVALUE:
        case FIELD:
        case FIND_IN_SET:
        case FLOOR:
        case FORMAT:
        case FOUND_ROWS:
        case FROM_BASE64:
        case FROM_DAYS:
        case FROM_UNIXTIME:
        case GEOMCOLLFROMTEXT:
        case GEOMCOLLFROMWKB:
        case GEOMETRYCOLLECTIONFROMTEXT:
        case GEOMETRYCOLLECTIONFROMWKB:
        case GEOMETRYFROMTEXT:
        case GEOMETRYFROMWKB:
        case GEOMETRYN:
        case GEOMETRYTYPE:
        case GEOMFROMTEXT:
        case GEOMFROMWKB:
        case GET_FORMAT:
        case GET_LOCK:
        case GLENGTH:
        case GREATEST:
        case GTID_SUBSET:
        case GTID_SUBTRACT:
        case HEX:
        case IFNULL:
        case INET6_ATON:
        case INET6_NTOA:
        case INET_ATON:
        case INET_NTOA:
        case INSTR:
        case INTERIORRINGN:
        case INTERSECTS:
        case ISCLOSED:
        case ISEMPTY:
        case ISNULL:
        case ISSIMPLE:
        case IS_FREE_LOCK:
        case IS_IPV4:
        case IS_IPV4_COMPAT:
        case IS_IPV4_MAPPED:
        case IS_IPV6:
        case IS_USED_LOCK:
        case LAST_INSERT_ID:
        case LCASE:
        case LEAST:
        case LENGTH:
        case LINEFROMTEXT:
        case LINEFROMWKB:
        case LINESTRINGFROMTEXT:
        case LINESTRINGFROMWKB:
        case LN:
        case LOAD_FILE:
        case LOCATE:
        case LOG:
        case LOG10:
        case LOG2:
        case LOWER:
        case LPAD:
        case LTRIM:
        case MAKEDATE:
        case MAKETIME:
        case MAKE_SET:
        case MASTER_POS_WAIT:
        case MBRCONTAINS:
        case MBRDISJOINT:
        case MBREQUAL:
        case MBRINTERSECTS:
        case MBROVERLAPS:
        case MBRTOUCHES:
        case MBRWITHIN:
        case MD5:
        case MLINEFROMTEXT:
        case MLINEFROMWKB:
        case MONTHNAME:
        case MPOINTFROMTEXT:
        case MPOINTFROMWKB:
        case MPOLYFROMTEXT:
        case MPOLYFROMWKB:
        case MULTILINESTRINGFROMTEXT:
        case MULTILINESTRINGFROMWKB:
        case MULTIPOINTFROMTEXT:
        case MULTIPOINTFROMWKB:
        case MULTIPOLYGONFROMTEXT:
        case MULTIPOLYGONFROMWKB:
        case NAME_CONST:
        case NULLIF:
        case NUMGEOMETRIES:
        case NUMINTERIORRINGS:
        case NUMPOINTS:
        case OCT:
        case OCTET_LENGTH:
        case ORD:
        case OVERLAPS:
        case PERIOD_ADD:
        case PERIOD_DIFF:
        case PI:
        case POINTFROMTEXT:
        case POINTFROMWKB:
        case POINTN:
        case POLYFROMTEXT:
        case POLYFROMWKB:
        case POLYGONFROMTEXT:
        case POLYGONFROMWKB:
        case POW:
        case POWER:
        case QUOTE:
        case RADIANS:
        case RAND:
        case RANDOM_BYTES:
        case RELEASE_LOCK:
        case REVERSE:
        case ROUND:
        case ROW_COUNT:
        case RPAD:
        case RTRIM:
        case SEC_TO_TIME:
        case SESSION_USER:
        case SHA:
        case SHA1:
        case SHA2:
        case SIGN:
        case SIN:
        case SLEEP:
        case SOUNDEX:
        case SQL_THREAD_WAIT_AFTER_GTIDS:
        case SQRT:
        case SRID:
        case STARTPOINT:
        case STRCMP:
        case STR_TO_DATE:
        case ST_AREA:
        case ST_ASBINARY:
        case ST_ASTEXT:
        case ST_ASWKB:
        case ST_ASWKT:
        case ST_BUFFER:
        case ST_CENTROID:
        case ST_CONTAINS:
        case ST_CROSSES:
        case ST_DIFFERENCE:
        case ST_DIMENSION:
        case ST_DISJOINT:
        case ST_DISTANCE:
        case ST_ENDPOINT:
        case ST_ENVELOPE:
        case ST_EQUALS:
        case ST_EXTERIORRING:
        case ST_GEOMCOLLFROMTEXT:
        case ST_GEOMCOLLFROMTXT:
        case ST_GEOMCOLLFROMWKB:
        case ST_GEOMETRYCOLLECTIONFROMTEXT:
        case ST_GEOMETRYCOLLECTIONFROMWKB:
        case ST_GEOMETRYFROMTEXT:
        case ST_GEOMETRYFROMWKB:
        case ST_GEOMETRYN:
        case ST_GEOMETRYTYPE:
        case ST_GEOMFROMTEXT:
        case ST_GEOMFROMWKB:
        case ST_INTERIORRINGN:
        case ST_INTERSECTION:
        case ST_INTERSECTS:
        case ST_ISCLOSED:
        case ST_ISEMPTY:
        case ST_ISSIMPLE:
        case ST_LINEFROMTEXT:
        case ST_LINEFROMWKB:
        case ST_LINESTRINGFROMTEXT:
        case ST_LINESTRINGFROMWKB:
        case ST_NUMGEOMETRIES:
        case ST_NUMINTERIORRING:
        case ST_NUMINTERIORRINGS:
        case ST_NUMPOINTS:
        case ST_OVERLAPS:
        case ST_POINTFROMTEXT:
        case ST_POINTFROMWKB:
        case ST_POINTN:
        case ST_POLYFROMTEXT:
        case ST_POLYFROMWKB:
        case ST_POLYGONFROMTEXT:
        case ST_POLYGONFROMWKB:
        case ST_SRID:
        case ST_STARTPOINT:
        case ST_SYMDIFFERENCE:
        case ST_TOUCHES:
        case ST_UNION:
        case ST_WITHIN:
        case ST_X:
        case ST_Y:
        case SUBDATE:
        case SUBSTRING_INDEX:
        case SUBTIME:
        case SYSTEM_USER:
        case TAN:
        case TIMEDIFF:
        case TIMESTAMPADD:
        case TIMESTAMPDIFF:
        case TIME_FORMAT:
        case TIME_TO_SEC:
        case TOUCHES:
        case TO_BASE64:
        case TO_DAYS:
        case TO_SECONDS:
        case UCASE:
        case UNCOMPRESS:
        case UNCOMPRESSED_LENGTH:
        case UNHEX:
        case UNIX_TIMESTAMP:
        case UPDATEXML:
        case UPPER:
        case UUID:
        case UUID_SHORT:
        case VALIDATE_PASSWORD_STRENGTH:
        case VERSION:
        case WAIT_UNTIL_SQL_THREAD_AFTER_GTIDS:
        case WEEKDAY:
        case WEEKOFYEAR:
        case WEIGHT_STRING:
        case WITHIN:
        case YEARWEEK:
        case Y_FUNCTION:
        case X_FUNCTION:
        case ID:
          enterOuterAlt(_localctx, 1);
          {
            setState(118);
            simpleId();
          }
          break;
        case REVERSE_QUOTE_ID:
          enterOuterAlt(_localctx, 2);
          {
            setState(119);
            match(REVERSE_QUOTE_ID);
          }
          break;
        case CHARSET_REVERSE_QOUTE_STRING:
          enterOuterAlt(_localctx, 3);
          {
            setState(120);
            match(CHARSET_REVERSE_QOUTE_STRING);
          }
          break;
        default:
          throw new NoViableAltException(this);
      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  public static class SimpleIdContext extends ParserRuleContext {
    public TerminalNode ID() {
      return getToken(MySqlParser.ID, 0);
    }

    public CharsetNameBaseContext charsetNameBase() {
      return getRuleContext(CharsetNameBaseContext.class, 0);
    }

    public IntervalTypeBaseContext intervalTypeBase() {
      return getRuleContext(IntervalTypeBaseContext.class, 0);
    }

    public DataTypeBaseContext dataTypeBase() {
      return getRuleContext(DataTypeBaseContext.class, 0);
    }

    public KeywordsCanBeIdContext keywordsCanBeId() {
      return getRuleContext(KeywordsCanBeIdContext.class, 0);
    }

    public FunctionNameBaseContext functionNameBase() {
      return getRuleContext(FunctionNameBaseContext.class, 0);
    }

    public SimpleIdContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_simpleId;
    }

    @Override
    public void enterRule(ParseTreeListener listener) {
      if (listener instanceof MySqlParserListener)
        ((MySqlParserListener) listener).enterSimpleId(this);
    }

    @Override
    public void exitRule(ParseTreeListener listener) {
      if (listener instanceof MySqlParserListener)
        ((MySqlParserListener) listener).exitSimpleId(this);
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof MySqlParserVisitor)
        return ((MySqlParserVisitor<? extends T>) visitor).visitSimpleId(this);
      else return visitor.visitChildren(this);
    }
  }

  public final SimpleIdContext simpleId() throws RecognitionException {
    SimpleIdContext _localctx = new SimpleIdContext(_ctx, getState());
    enterRule(_localctx, 12, RULE_simpleId);
    try {
      setState(129);
      _errHandler.sync(this);
      switch (getInterpreter().adaptivePredict(_input, 7, _ctx)) {
        case 1:
          enterOuterAlt(_localctx, 1);
          {
            setState(123);
            match(ID);
          }
          break;
        case 2:
          enterOuterAlt(_localctx, 2);
          {
            setState(124);
            charsetNameBase();
          }
          break;
        case 3:
          enterOuterAlt(_localctx, 3);
          {
            setState(125);
            intervalTypeBase();
          }
          break;
        case 4:
          enterOuterAlt(_localctx, 4);
          {
            setState(126);
            dataTypeBase();
          }
          break;
        case 5:
          enterOuterAlt(_localctx, 5);
          {
            setState(127);
            keywordsCanBeId();
          }
          break;
        case 6:
          enterOuterAlt(_localctx, 6);
          {
            setState(128);
            functionNameBase();
          }
          break;
      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  public static class DottedIdContext extends ParserRuleContext {
    public TerminalNode DOT_ID() {
      return getToken(MySqlParser.DOT_ID, 0);
    }

    public UidContext uid() {
      return getRuleContext(UidContext.class, 0);
    }

    public DottedIdContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_dottedId;
    }

    @Override
    public void enterRule(ParseTreeListener listener) {
      if (listener instanceof MySqlParserListener)
        ((MySqlParserListener) listener).enterDottedId(this);
    }

    @Override
    public void exitRule(ParseTreeListener listener) {
      if (listener instanceof MySqlParserListener)
        ((MySqlParserListener) listener).exitDottedId(this);
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof MySqlParserVisitor)
        return ((MySqlParserVisitor<? extends T>) visitor).visitDottedId(this);
      else return visitor.visitChildren(this);
    }
  }

  public final DottedIdContext dottedId() throws RecognitionException {
    DottedIdContext _localctx = new DottedIdContext(_ctx, getState());
    enterRule(_localctx, 14, RULE_dottedId);
    try {
      setState(134);
      _errHandler.sync(this);
      switch (_input.LA(1)) {
        case DOT_ID:
          enterOuterAlt(_localctx, 1);
          {
            setState(131);
            match(DOT_ID);
          }
          break;
        case DOT:
          enterOuterAlt(_localctx, 2);
          {
            setState(132);
            match(DOT);
            setState(133);
            uid();
          }
          break;
        default:
          throw new NoViableAltException(this);
      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  public static class DecimalLiteralContext extends ParserRuleContext {
    public TerminalNode DECIMAL_LITERAL() {
      return getToken(MySqlParser.DECIMAL_LITERAL, 0);
    }

    public TerminalNode ZERO_DECIMAL() {
      return getToken(MySqlParser.ZERO_DECIMAL, 0);
    }

    public TerminalNode ONE_DECIMAL() {
      return getToken(MySqlParser.ONE_DECIMAL, 0);
    }

    public TerminalNode TWO_DECIMAL() {
      return getToken(MySqlParser.TWO_DECIMAL, 0);
    }

    public DecimalLiteralContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_decimalLiteral;
    }

    @Override
    public void enterRule(ParseTreeListener listener) {
      if (listener instanceof MySqlParserListener)
        ((MySqlParserListener) listener).enterDecimalLiteral(this);
    }

    @Override
    public void exitRule(ParseTreeListener listener) {
      if (listener instanceof MySqlParserListener)
        ((MySqlParserListener) listener).exitDecimalLiteral(this);
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof MySqlParserVisitor)
        return ((MySqlParserVisitor<? extends T>) visitor).visitDecimalLiteral(this);
      else return visitor.visitChildren(this);
    }
  }

  public final DecimalLiteralContext decimalLiteral() throws RecognitionException {
    DecimalLiteralContext _localctx = new DecimalLiteralContext(_ctx, getState());
    enterRule(_localctx, 16, RULE_decimalLiteral);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
        setState(136);
        _la = _input.LA(1);
        if (!(((((_la - 954)) & ~0x3f) == 0
            && ((1L << (_la - 954))
                    & ((1L << (ZERO_DECIMAL - 954))
                        | (1L << (ONE_DECIMAL - 954))
                        | (1L << (TWO_DECIMAL - 954))
                        | (1L << (DECIMAL_LITERAL - 954))))
                != 0))) {
          _errHandler.recoverInline(this);
        } else {
          if (_input.LA(1) == Token.EOF) matchedEOF = true;
          _errHandler.reportMatch(this);
          consume();
        }
      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  public static class StringLiteralContext extends ParserRuleContext {
    public List<TerminalNode> STRING_LITERAL() {
      return getTokens(MySqlParser.STRING_LITERAL);
    }

    public TerminalNode STRING_LITERAL(int i) {
      return getToken(MySqlParser.STRING_LITERAL, i);
    }

    public StringLiteralContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_stringLiteral;
    }

    @Override
    public void enterRule(ParseTreeListener listener) {
      if (listener instanceof MySqlParserListener)
        ((MySqlParserListener) listener).enterStringLiteral(this);
    }

    @Override
    public void exitRule(ParseTreeListener listener) {
      if (listener instanceof MySqlParserListener)
        ((MySqlParserListener) listener).exitStringLiteral(this);
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof MySqlParserVisitor)
        return ((MySqlParserVisitor<? extends T>) visitor).visitStringLiteral(this);
      else return visitor.visitChildren(this);
    }
  }

  public final StringLiteralContext stringLiteral() throws RecognitionException {
    StringLiteralContext _localctx = new StringLiteralContext(_ctx, getState());
    enterRule(_localctx, 18, RULE_stringLiteral);
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      {
        setState(139);
        _errHandler.sync(this);
        _alt = 1;
        do {
          switch (_alt) {
            case 1:
              {
                {
                  setState(138);
                  match(STRING_LITERAL);
                }
              }
              break;
            default:
              throw new NoViableAltException(this);
          }
          setState(141);
          _errHandler.sync(this);
          _alt = getInterpreter().adaptivePredict(_input, 9, _ctx);
        } while (_alt != 2 && _alt != org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER);
      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  public static class BooleanLiteralContext extends ParserRuleContext {
    public TerminalNode TRUE() {
      return getToken(MySqlParser.TRUE, 0);
    }

    public TerminalNode FALSE() {
      return getToken(MySqlParser.FALSE, 0);
    }

    public BooleanLiteralContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_booleanLiteral;
    }

    @Override
    public void enterRule(ParseTreeListener listener) {
      if (listener instanceof MySqlParserListener)
        ((MySqlParserListener) listener).enterBooleanLiteral(this);
    }

    @Override
    public void exitRule(ParseTreeListener listener) {
      if (listener instanceof MySqlParserListener)
        ((MySqlParserListener) listener).exitBooleanLiteral(this);
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof MySqlParserVisitor)
        return ((MySqlParserVisitor<? extends T>) visitor).visitBooleanLiteral(this);
      else return visitor.visitChildren(this);
    }
  }

  public final BooleanLiteralContext booleanLiteral() throws RecognitionException {
    BooleanLiteralContext _localctx = new BooleanLiteralContext(_ctx, getState());
    enterRule(_localctx, 20, RULE_booleanLiteral);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
        setState(143);
        _la = _input.LA(1);
        if (!(_la == FALSE || _la == TRUE)) {
          _errHandler.recoverInline(this);
        } else {
          if (_input.LA(1) == Token.EOF) matchedEOF = true;
          _errHandler.reportMatch(this);
          consume();
        }
      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  public static class HexadecimalLiteralContext extends ParserRuleContext {
    public TerminalNode HEXADECIMAL_LITERAL() {
      return getToken(MySqlParser.HEXADECIMAL_LITERAL, 0);
    }

    public TerminalNode STRING_CHARSET_NAME() {
      return getToken(MySqlParser.STRING_CHARSET_NAME, 0);
    }

    public HexadecimalLiteralContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_hexadecimalLiteral;
    }

    @Override
    public void enterRule(ParseTreeListener listener) {
      if (listener instanceof MySqlParserListener)
        ((MySqlParserListener) listener).enterHexadecimalLiteral(this);
    }

    @Override
    public void exitRule(ParseTreeListener listener) {
      if (listener instanceof MySqlParserListener)
        ((MySqlParserListener) listener).exitHexadecimalLiteral(this);
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof MySqlParserVisitor)
        return ((MySqlParserVisitor<? extends T>) visitor).visitHexadecimalLiteral(this);
      else return visitor.visitChildren(this);
    }
  }

  public final HexadecimalLiteralContext hexadecimalLiteral() throws RecognitionException {
    HexadecimalLiteralContext _localctx = new HexadecimalLiteralContext(_ctx, getState());
    enterRule(_localctx, 22, RULE_hexadecimalLiteral);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
        setState(146);
        _errHandler.sync(this);
        _la = _input.LA(1);
        if (_la == STRING_CHARSET_NAME) {
          {
            setState(145);
            match(STRING_CHARSET_NAME);
          }
        }

        setState(148);
        match(HEXADECIMAL_LITERAL);
      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  public static class NullNotnullContext extends ParserRuleContext {
    public TerminalNode NULL_LITERAL() {
      return getToken(MySqlParser.NULL_LITERAL, 0);
    }

    public TerminalNode NULL_SPEC_LITERAL() {
      return getToken(MySqlParser.NULL_SPEC_LITERAL, 0);
    }

    public TerminalNode NOT() {
      return getToken(MySqlParser.NOT, 0);
    }

    public NullNotnullContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_nullNotnull;
    }

    @Override
    public void enterRule(ParseTreeListener listener) {
      if (listener instanceof MySqlParserListener)
        ((MySqlParserListener) listener).enterNullNotnull(this);
    }

    @Override
    public void exitRule(ParseTreeListener listener) {
      if (listener instanceof MySqlParserListener)
        ((MySqlParserListener) listener).exitNullNotnull(this);
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof MySqlParserVisitor)
        return ((MySqlParserVisitor<? extends T>) visitor).visitNullNotnull(this);
      else return visitor.visitChildren(this);
    }
  }

  public final NullNotnullContext nullNotnull() throws RecognitionException {
    NullNotnullContext _localctx = new NullNotnullContext(_ctx, getState());
    enterRule(_localctx, 24, RULE_nullNotnull);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
        setState(151);
        _errHandler.sync(this);
        _la = _input.LA(1);
        if (_la == NOT) {
          {
            setState(150);
            match(NOT);
          }
        }

        setState(153);
        _la = _input.LA(1);
        if (!(_la == NULL_LITERAL || _la == NULL_SPEC_LITERAL)) {
          _errHandler.recoverInline(this);
        } else {
          if (_input.LA(1) == Token.EOF) matchedEOF = true;
          _errHandler.reportMatch(this);
          consume();
        }
      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  public static class ConstantContext extends ParserRuleContext {
    public Token nullLiteral;

    public StringLiteralContext stringLiteral() {
      return getRuleContext(StringLiteralContext.class, 0);
    }

    public DecimalLiteralContext decimalLiteral() {
      return getRuleContext(DecimalLiteralContext.class, 0);
    }

    public HexadecimalLiteralContext hexadecimalLiteral() {
      return getRuleContext(HexadecimalLiteralContext.class, 0);
    }

    public BooleanLiteralContext booleanLiteral() {
      return getRuleContext(BooleanLiteralContext.class, 0);
    }

    public TerminalNode REAL_LITERAL() {
      return getToken(MySqlParser.REAL_LITERAL, 0);
    }

    public TerminalNode BIT_STRING() {
      return getToken(MySqlParser.BIT_STRING, 0);
    }

    public TerminalNode NULL_LITERAL() {
      return getToken(MySqlParser.NULL_LITERAL, 0);
    }

    public TerminalNode NULL_SPEC_LITERAL() {
      return getToken(MySqlParser.NULL_SPEC_LITERAL, 0);
    }

    public TerminalNode NOT() {
      return getToken(MySqlParser.NOT, 0);
    }

    public ConstantContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_constant;
    }

    @Override
    public void enterRule(ParseTreeListener listener) {
      if (listener instanceof MySqlParserListener)
        ((MySqlParserListener) listener).enterConstant(this);
    }

    @Override
    public void exitRule(ParseTreeListener listener) {
      if (listener instanceof MySqlParserListener)
        ((MySqlParserListener) listener).exitConstant(this);
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof MySqlParserVisitor)
        return ((MySqlParserVisitor<? extends T>) visitor).visitConstant(this);
      else return visitor.visitChildren(this);
    }
  }

  public final ConstantContext constant() throws RecognitionException {
    ConstantContext _localctx = new ConstantContext(_ctx, getState());
    enterRule(_localctx, 26, RULE_constant);
    int _la;
    try {
      setState(167);
      _errHandler.sync(this);
      switch (_input.LA(1)) {
        case STRING_LITERAL:
          enterOuterAlt(_localctx, 1);
          {
            setState(155);
            stringLiteral();
          }
          break;
        case ZERO_DECIMAL:
        case ONE_DECIMAL:
        case TWO_DECIMAL:
        case DECIMAL_LITERAL:
          enterOuterAlt(_localctx, 2);
          {
            setState(156);
            decimalLiteral();
          }
          break;
        case MINUS:
          enterOuterAlt(_localctx, 3);
          {
            setState(157);
            match(MINUS);
            setState(158);
            decimalLiteral();
          }
          break;
        case HEXADECIMAL_LITERAL:
        case STRING_CHARSET_NAME:
          enterOuterAlt(_localctx, 4);
          {
            setState(159);
            hexadecimalLiteral();
          }
          break;
        case FALSE:
        case TRUE:
          enterOuterAlt(_localctx, 5);
          {
            setState(160);
            booleanLiteral();
          }
          break;
        case REAL_LITERAL:
          enterOuterAlt(_localctx, 6);
          {
            setState(161);
            match(REAL_LITERAL);
          }
          break;
        case BIT_STRING:
          enterOuterAlt(_localctx, 7);
          {
            setState(162);
            match(BIT_STRING);
          }
          break;
        case NOT:
        case NULL_LITERAL:
        case NULL_SPEC_LITERAL:
          enterOuterAlt(_localctx, 8);
          {
            setState(164);
            _errHandler.sync(this);
            _la = _input.LA(1);
            if (_la == NOT) {
              {
                setState(163);
                match(NOT);
              }
            }

            setState(166);
            ((ConstantContext) _localctx).nullLiteral = _input.LT(1);
            _la = _input.LA(1);
            if (!(_la == NULL_LITERAL || _la == NULL_SPEC_LITERAL)) {
              ((ConstantContext) _localctx).nullLiteral = (Token) _errHandler.recoverInline(this);
            } else {
              if (_input.LA(1) == Token.EOF) matchedEOF = true;
              _errHandler.reportMatch(this);
              consume();
            }
          }
          break;
        default:
          throw new NoViableAltException(this);
      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  public static class ConvertedDataTypeContext extends ParserRuleContext {
    public Token typeName;

    public TerminalNode BINARY() {
      return getToken(MySqlParser.BINARY, 0);
    }

    public TerminalNode NCHAR() {
      return getToken(MySqlParser.NCHAR, 0);
    }

    public LengthOneDimensionContext lengthOneDimension() {
      return getRuleContext(LengthOneDimensionContext.class, 0);
    }

    public TerminalNode CHAR() {
      return getToken(MySqlParser.CHAR, 0);
    }

    public TerminalNode CHARACTER() {
      return getToken(MySqlParser.CHARACTER, 0);
    }

    public TerminalNode SET() {
      return getToken(MySqlParser.SET, 0);
    }

    public CharsetNameContext charsetName() {
      return getRuleContext(CharsetNameContext.class, 0);
    }

    public TerminalNode DATE() {
      return getToken(MySqlParser.DATE, 0);
    }

    public TerminalNode DATETIME() {
      return getToken(MySqlParser.DATETIME, 0);
    }

    public TerminalNode TIME() {
      return getToken(MySqlParser.TIME, 0);
    }

    public TerminalNode DECIMAL() {
      return getToken(MySqlParser.DECIMAL, 0);
    }

    public LengthTwoDimensionContext lengthTwoDimension() {
      return getRuleContext(LengthTwoDimensionContext.class, 0);
    }

    public TerminalNode SIGNED() {
      return getToken(MySqlParser.SIGNED, 0);
    }

    public TerminalNode UNSIGNED() {
      return getToken(MySqlParser.UNSIGNED, 0);
    }

    public TerminalNode INTEGER() {
      return getToken(MySqlParser.INTEGER, 0);
    }

    public ConvertedDataTypeContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_convertedDataType;
    }

    @Override
    public void enterRule(ParseTreeListener listener) {
      if (listener instanceof MySqlParserListener)
        ((MySqlParserListener) listener).enterConvertedDataType(this);
    }

    @Override
    public void exitRule(ParseTreeListener listener) {
      if (listener instanceof MySqlParserListener)
        ((MySqlParserListener) listener).exitConvertedDataType(this);
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof MySqlParserVisitor)
        return ((MySqlParserVisitor<? extends T>) visitor).visitConvertedDataType(this);
      else return visitor.visitChildren(this);
    }
  }

  public final ConvertedDataTypeContext convertedDataType() throws RecognitionException {
    ConvertedDataTypeContext _localctx = new ConvertedDataTypeContext(_ctx, getState());
    enterRule(_localctx, 28, RULE_convertedDataType);
    int _la;
    try {
      setState(191);
      _errHandler.sync(this);
      switch (_input.LA(1)) {
        case BINARY:
        case NCHAR:
          enterOuterAlt(_localctx, 1);
          {
            setState(169);
            ((ConvertedDataTypeContext) _localctx).typeName = _input.LT(1);
            _la = _input.LA(1);
            if (!(_la == BINARY || _la == NCHAR)) {
              ((ConvertedDataTypeContext) _localctx).typeName =
                  (Token) _errHandler.recoverInline(this);
            } else {
              if (_input.LA(1) == Token.EOF) matchedEOF = true;
              _errHandler.reportMatch(this);
              consume();
            }
            setState(171);
            _errHandler.sync(this);
            _la = _input.LA(1);
            if (_la == LR_BRACKET) {
              {
                setState(170);
                lengthOneDimension();
              }
            }
          }
          break;
        case CHAR:
          enterOuterAlt(_localctx, 2);
          {
            setState(173);
            ((ConvertedDataTypeContext) _localctx).typeName = match(CHAR);
            setState(175);
            _errHandler.sync(this);
            _la = _input.LA(1);
            if (_la == LR_BRACKET) {
              {
                setState(174);
                lengthOneDimension();
              }
            }

            setState(180);
            _errHandler.sync(this);
            _la = _input.LA(1);
            if (_la == CHARACTER) {
              {
                setState(177);
                match(CHARACTER);
                setState(178);
                match(SET);
                setState(179);
                charsetName();
              }
            }
          }
          break;
        case DATE:
        case TIME:
        case DATETIME:
          enterOuterAlt(_localctx, 3);
          {
            setState(182);
            ((ConvertedDataTypeContext) _localctx).typeName = _input.LT(1);
            _la = _input.LA(1);
            if (!(((((_la - 182)) & ~0x3f) == 0
                && ((1L << (_la - 182))
                        & ((1L << (DATE - 182)) | (1L << (TIME - 182)) | (1L << (DATETIME - 182))))
                    != 0))) {
              ((ConvertedDataTypeContext) _localctx).typeName =
                  (Token) _errHandler.recoverInline(this);
            } else {
              if (_input.LA(1) == Token.EOF) matchedEOF = true;
              _errHandler.reportMatch(this);
              consume();
            }
          }
          break;
        case DECIMAL:
          enterOuterAlt(_localctx, 4);
          {
            setState(183);
            ((ConvertedDataTypeContext) _localctx).typeName = match(DECIMAL);
            setState(185);
            _errHandler.sync(this);
            _la = _input.LA(1);
            if (_la == LR_BRACKET) {
              {
                setState(184);
                lengthTwoDimension();
              }
            }
          }
          break;
        case UNSIGNED:
        case SIGNED:
          enterOuterAlt(_localctx, 5);
          {
            setState(187);
            _la = _input.LA(1);
            if (!(_la == UNSIGNED || _la == SIGNED)) {
              _errHandler.recoverInline(this);
            } else {
              if (_input.LA(1) == Token.EOF) matchedEOF = true;
              _errHandler.reportMatch(this);
              consume();
            }
            setState(189);
            _errHandler.sync(this);
            _la = _input.LA(1);
            if (_la == INTEGER) {
              {
                setState(188);
                match(INTEGER);
              }
            }
          }
          break;
        default:
          throw new NoViableAltException(this);
      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  public static class LengthOneDimensionContext extends ParserRuleContext {
    public DecimalLiteralContext decimalLiteral() {
      return getRuleContext(DecimalLiteralContext.class, 0);
    }

    public LengthOneDimensionContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_lengthOneDimension;
    }

    @Override
    public void enterRule(ParseTreeListener listener) {
      if (listener instanceof MySqlParserListener)
        ((MySqlParserListener) listener).enterLengthOneDimension(this);
    }

    @Override
    public void exitRule(ParseTreeListener listener) {
      if (listener instanceof MySqlParserListener)
        ((MySqlParserListener) listener).exitLengthOneDimension(this);
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof MySqlParserVisitor)
        return ((MySqlParserVisitor<? extends T>) visitor).visitLengthOneDimension(this);
      else return visitor.visitChildren(this);
    }
  }

  public final LengthOneDimensionContext lengthOneDimension() throws RecognitionException {
    LengthOneDimensionContext _localctx = new LengthOneDimensionContext(_ctx, getState());
    enterRule(_localctx, 30, RULE_lengthOneDimension);
    try {
      enterOuterAlt(_localctx, 1);
      {
        setState(193);
        match(LR_BRACKET);
        setState(194);
        decimalLiteral();
        setState(195);
        match(RR_BRACKET);
      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  public static class LengthTwoDimensionContext extends ParserRuleContext {
    public List<DecimalLiteralContext> decimalLiteral() {
      return getRuleContexts(DecimalLiteralContext.class);
    }

    public DecimalLiteralContext decimalLiteral(int i) {
      return getRuleContext(DecimalLiteralContext.class, i);
    }

    public LengthTwoDimensionContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_lengthTwoDimension;
    }

    @Override
    public void enterRule(ParseTreeListener listener) {
      if (listener instanceof MySqlParserListener)
        ((MySqlParserListener) listener).enterLengthTwoDimension(this);
    }

    @Override
    public void exitRule(ParseTreeListener listener) {
      if (listener instanceof MySqlParserListener)
        ((MySqlParserListener) listener).exitLengthTwoDimension(this);
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof MySqlParserVisitor)
        return ((MySqlParserVisitor<? extends T>) visitor).visitLengthTwoDimension(this);
      else return visitor.visitChildren(this);
    }
  }

  public final LengthTwoDimensionContext lengthTwoDimension() throws RecognitionException {
    LengthTwoDimensionContext _localctx = new LengthTwoDimensionContext(_ctx, getState());
    enterRule(_localctx, 32, RULE_lengthTwoDimension);
    try {
      enterOuterAlt(_localctx, 1);
      {
        setState(197);
        match(LR_BRACKET);
        setState(198);
        decimalLiteral();
        setState(199);
        match(COMMA);
        setState(200);
        decimalLiteral();
        setState(201);
        match(RR_BRACKET);
      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  public static class ExpressionsContext extends ParserRuleContext {
    public List<ExpressionContext> expression() {
      return getRuleContexts(ExpressionContext.class);
    }

    public ExpressionContext expression(int i) {
      return getRuleContext(ExpressionContext.class, i);
    }

    public ExpressionsContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_expressions;
    }

    @Override
    public void enterRule(ParseTreeListener listener) {
      if (listener instanceof MySqlParserListener)
        ((MySqlParserListener) listener).enterExpressions(this);
    }

    @Override
    public void exitRule(ParseTreeListener listener) {
      if (listener instanceof MySqlParserListener)
        ((MySqlParserListener) listener).exitExpressions(this);
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof MySqlParserVisitor)
        return ((MySqlParserVisitor<? extends T>) visitor).visitExpressions(this);
      else return visitor.visitChildren(this);
    }
  }

  public final ExpressionsContext expressions() throws RecognitionException {
    ExpressionsContext _localctx = new ExpressionsContext(_ctx, getState());
    enterRule(_localctx, 34, RULE_expressions);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
        setState(203);
        expression(0);
        setState(208);
        _errHandler.sync(this);
        _la = _input.LA(1);
        while (_la == COMMA) {
          {
            {
              setState(204);
              match(COMMA);
              setState(205);
              expression(0);
            }
          }
          setState(210);
          _errHandler.sync(this);
          _la = _input.LA(1);
        }
      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  public static class CurrentTimestampContext extends ParserRuleContext {
    public TerminalNode NOW() {
      return getToken(MySqlParser.NOW, 0);
    }

    public TerminalNode CURRENT_TIMESTAMP() {
      return getToken(MySqlParser.CURRENT_TIMESTAMP, 0);
    }

    public TerminalNode LOCALTIME() {
      return getToken(MySqlParser.LOCALTIME, 0);
    }

    public TerminalNode LOCALTIMESTAMP() {
      return getToken(MySqlParser.LOCALTIMESTAMP, 0);
    }

    public DecimalLiteralContext decimalLiteral() {
      return getRuleContext(DecimalLiteralContext.class, 0);
    }

    public CurrentTimestampContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_currentTimestamp;
    }

    @Override
    public void enterRule(ParseTreeListener listener) {
      if (listener instanceof MySqlParserListener)
        ((MySqlParserListener) listener).enterCurrentTimestamp(this);
    }

    @Override
    public void exitRule(ParseTreeListener listener) {
      if (listener instanceof MySqlParserListener)
        ((MySqlParserListener) listener).exitCurrentTimestamp(this);
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof MySqlParserVisitor)
        return ((MySqlParserVisitor<? extends T>) visitor).visitCurrentTimestamp(this);
      else return visitor.visitChildren(this);
    }
  }

  public final CurrentTimestampContext currentTimestamp() throws RecognitionException {
    CurrentTimestampContext _localctx = new CurrentTimestampContext(_ctx, getState());
    enterRule(_localctx, 36, RULE_currentTimestamp);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
        setState(225);
        _errHandler.sync(this);
        switch (_input.LA(1)) {
          case CURRENT_TIMESTAMP:
          case LOCALTIME:
          case LOCALTIMESTAMP:
            {
              setState(211);
              _la = _input.LA(1);
              if (!(((((_la - 229)) & ~0x3f) == 0
                  && ((1L << (_la - 229))
                          & ((1L << (CURRENT_TIMESTAMP - 229))
                              | (1L << (LOCALTIME - 229))
                              | (1L << (LOCALTIMESTAMP - 229))))
                      != 0))) {
                _errHandler.recoverInline(this);
              } else {
                if (_input.LA(1) == Token.EOF) matchedEOF = true;
                _errHandler.reportMatch(this);
                consume();
              }
              setState(217);
              _errHandler.sync(this);
              _la = _input.LA(1);
              if (_la == LR_BRACKET) {
                {
                  setState(212);
                  match(LR_BRACKET);
                  setState(214);
                  _errHandler.sync(this);
                  _la = _input.LA(1);
                  if (((((_la - 954)) & ~0x3f) == 0
                      && ((1L << (_la - 954))
                              & ((1L << (ZERO_DECIMAL - 954))
                                  | (1L << (ONE_DECIMAL - 954))
                                  | (1L << (TWO_DECIMAL - 954))
                                  | (1L << (DECIMAL_LITERAL - 954))))
                          != 0)) {
                    {
                      setState(213);
                      decimalLiteral();
                    }
                  }

                  setState(216);
                  match(RR_BRACKET);
                }
              }
            }
            break;
          case NOW:
            {
              setState(219);
              match(NOW);
              setState(220);
              match(LR_BRACKET);
              setState(222);
              _errHandler.sync(this);
              _la = _input.LA(1);
              if (((((_la - 954)) & ~0x3f) == 0
                  && ((1L << (_la - 954))
                          & ((1L << (ZERO_DECIMAL - 954))
                              | (1L << (ONE_DECIMAL - 954))
                              | (1L << (TWO_DECIMAL - 954))
                              | (1L << (DECIMAL_LITERAL - 954))))
                      != 0)) {
                {
                  setState(221);
                  decimalLiteral();
                }
              }

              setState(224);
              match(RR_BRACKET);
            }
            break;
          default:
            throw new NoViableAltException(this);
        }
      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  public static class FunctionCallContext extends ParserRuleContext {
    public FunctionCallContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_functionCall;
    }

    public FunctionCallContext() {}

    public void copyFrom(FunctionCallContext ctx) {
      super.copyFrom(ctx);
    }
  }

  public static class SpecificFunctionCallContext extends FunctionCallContext {
    public SpecificFunctionContext specificFunction() {
      return getRuleContext(SpecificFunctionContext.class, 0);
    }

    public SpecificFunctionCallContext(FunctionCallContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public void enterRule(ParseTreeListener listener) {
      if (listener instanceof MySqlParserListener)
        ((MySqlParserListener) listener).enterSpecificFunctionCall(this);
    }

    @Override
    public void exitRule(ParseTreeListener listener) {
      if (listener instanceof MySqlParserListener)
        ((MySqlParserListener) listener).exitSpecificFunctionCall(this);
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof MySqlParserVisitor)
        return ((MySqlParserVisitor<? extends T>) visitor).visitSpecificFunctionCall(this);
      else return visitor.visitChildren(this);
    }
  }

  public static class UdfFunctionCallContext extends FunctionCallContext {
    public FullIdContext fullId() {
      return getRuleContext(FullIdContext.class, 0);
    }

    public FunctionArgsContext functionArgs() {
      return getRuleContext(FunctionArgsContext.class, 0);
    }

    public UdfFunctionCallContext(FunctionCallContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public void enterRule(ParseTreeListener listener) {
      if (listener instanceof MySqlParserListener)
        ((MySqlParserListener) listener).enterUdfFunctionCall(this);
    }

    @Override
    public void exitRule(ParseTreeListener listener) {
      if (listener instanceof MySqlParserListener)
        ((MySqlParserListener) listener).exitUdfFunctionCall(this);
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof MySqlParserVisitor)
        return ((MySqlParserVisitor<? extends T>) visitor).visitUdfFunctionCall(this);
      else return visitor.visitChildren(this);
    }
  }

  public static class ScalarFunctionCallContext extends FunctionCallContext {
    public ScalarFunctionNameContext scalarFunctionName() {
      return getRuleContext(ScalarFunctionNameContext.class, 0);
    }

    public FunctionArgsContext functionArgs() {
      return getRuleContext(FunctionArgsContext.class, 0);
    }

    public ScalarFunctionCallContext(FunctionCallContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public void enterRule(ParseTreeListener listener) {
      if (listener instanceof MySqlParserListener)
        ((MySqlParserListener) listener).enterScalarFunctionCall(this);
    }

    @Override
    public void exitRule(ParseTreeListener listener) {
      if (listener instanceof MySqlParserListener)
        ((MySqlParserListener) listener).exitScalarFunctionCall(this);
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof MySqlParserVisitor)
        return ((MySqlParserVisitor<? extends T>) visitor).visitScalarFunctionCall(this);
      else return visitor.visitChildren(this);
    }
  }

  public final FunctionCallContext functionCall() throws RecognitionException {
    FunctionCallContext _localctx = new FunctionCallContext(_ctx, getState());
    enterRule(_localctx, 38, RULE_functionCall);
    int _la;
    try {
      setState(242);
      _errHandler.sync(this);
      switch (getInterpreter().adaptivePredict(_input, 27, _ctx)) {
        case 1:
          _localctx = new SpecificFunctionCallContext(_localctx);
          enterOuterAlt(_localctx, 1);
          {
            setState(227);
            specificFunction();
          }
          break;
        case 2:
          _localctx = new ScalarFunctionCallContext(_localctx);
          enterOuterAlt(_localctx, 2);
          {
            setState(228);
            scalarFunctionName();
            setState(229);
            match(LR_BRACKET);
            setState(231);
            _errHandler.sync(this);
            _la = _input.LA(1);
            if ((((_la) & ~0x3f) == 0
                    && ((1L << _la)
                            & ((1L << CASE)
                                | (1L << CAST)
                                | (1L << CONVERT)
                                | (1L << CURRENT_USER)
                                | (1L << DATABASE)
                                | (1L << FALSE)))
                        != 0)
                || ((((_la - 66)) & ~0x3f) == 0
                    && ((1L << (_la - 66))
                            & ((1L << (IF - 66))
                                | (1L << (INSERT - 66))
                                | (1L << (INTERVAL - 66))
                                | (1L << (LEFT - 66))
                                | (1L << (NOT - 66))
                                | (1L << (NULL_LITERAL - 66))
                                | (1L << (REPLACE - 66))
                                | (1L << (RIGHT - 66))))
                        != 0)
                || ((((_la - 153)) & ~0x3f) == 0
                    && ((1L << (_la - 153))
                            & ((1L << (TRUE - 153))
                                | (1L << (VALUES - 153))
                                | (1L << (DATE - 153))
                                | (1L << (TIME - 153))
                                | (1L << (TIMESTAMP - 153))
                                | (1L << (DATETIME - 153))
                                | (1L << (YEAR - 153))
                                | (1L << (CHAR - 153))
                                | (1L << (BINARY - 153))
                                | (1L << (TEXT - 153))
                                | (1L << (ENUM - 153))
                                | (1L << (COUNT - 153))))
                        != 0)
                || ((((_la - 227)) & ~0x3f) == 0
                    && ((1L << (_la - 227))
                            & ((1L << (CURRENT_DATE - 227))
                                | (1L << (CURRENT_TIME - 227))
                                | (1L << (CURRENT_TIMESTAMP - 227))
                                | (1L << (LOCALTIME - 227))
                                | (1L << (CURDATE - 227))
                                | (1L << (CURTIME - 227))
                                | (1L << (DATE_ADD - 227))
                                | (1L << (DATE_SUB - 227))
                                | (1L << (EXTRACT - 227))
                                | (1L << (LOCALTIMESTAMP - 227))
                                | (1L << (NOW - 227))
                                | (1L << (POSITION - 227))
                                | (1L << (SUBSTR - 227))
                                | (1L << (SUBSTRING - 227))
                                | (1L << (SYSDATE - 227))
                                | (1L << (TRIM - 227))
                                | (1L << (UTC_DATE - 227))
                                | (1L << (UTC_TIME - 227))
                                | (1L << (UTC_TIMESTAMP - 227))
                                | (1L << (ACCOUNT - 227))
                                | (1L << (ACTION - 227))
                                | (1L << (AFTER - 227))
                                | (1L << (AGGREGATE - 227))
                                | (1L << (ALGORITHM - 227))
                                | (1L << (ANY - 227))
                                | (1L << (AT - 227))
                                | (1L << (AUTHORS - 227))
                                | (1L << (AUTOCOMMIT - 227))
                                | (1L << (AUTOEXTEND_SIZE - 227))
                                | (1L << (AUTO_INCREMENT - 227))
                                | (1L << (AVG_ROW_LENGTH - 227))
                                | (1L << (BEGIN - 227))
                                | (1L << (BINLOG - 227))
                                | (1L << (BIT - 227))
                                | (1L << (BLOCK - 227))
                                | (1L << (BOOL - 227))
                                | (1L << (BOOLEAN - 227))
                                | (1L << (BTREE - 227))
                                | (1L << (CASCADED - 227))
                                | (1L << (CHAIN - 227))
                                | (1L << (CHANGED - 227))
                                | (1L << (CHANNEL - 227))
                                | (1L << (CHECKSUM - 227))
                                | (1L << (CIPHER - 227))
                                | (1L << (CLIENT - 227))
                                | (1L << (COALESCE - 227))
                                | (1L << (CODE - 227))
                                | (1L << (COLUMNS - 227))
                                | (1L << (COLUMN_FORMAT - 227))
                                | (1L << (COMMENT - 227))
                                | (1L << (COMMIT - 227))
                                | (1L << (COMPACT - 227))
                                | (1L << (COMPLETION - 227))
                                | (1L << (COMPRESSED - 227))
                                | (1L << (COMPRESSION - 227))
                                | (1L << (CONCURRENT - 227))
                                | (1L << (CONNECTION - 227))
                                | (1L << (CONSISTENT - 227))
                                | (1L << (CONTAINS - 227))
                                | (1L << (CONTEXT - 227))
                                | (1L << (CONTRIBUTORS - 227))
                                | (1L << (COPY - 227))))
                        != 0)
                || ((((_la - 291)) & ~0x3f) == 0
                    && ((1L << (_la - 291))
                            & ((1L << (CPU - 291))
                                | (1L << (DATA - 291))
                                | (1L << (DATAFILE - 291))
                                | (1L << (DEALLOCATE - 291))
                                | (1L << (DEFAULT_AUTH - 291))
                                | (1L << (DEFINER - 291))
                                | (1L << (DELAY_KEY_WRITE - 291))
                                | (1L << (DIRECTORY - 291))
                                | (1L << (DISABLE - 291))
                                | (1L << (DISCARD - 291))
                                | (1L << (DISK - 291))
                                | (1L << (DO - 291))
                                | (1L << (DUMPFILE - 291))
                                | (1L << (DUPLICATE - 291))
                                | (1L << (DYNAMIC - 291))
                                | (1L << (ENABLE - 291))
                                | (1L << (ENCRYPTION - 291))
                                | (1L << (ENDS - 291))
                                | (1L << (ENGINE - 291))
                                | (1L << (ENGINES - 291))
                                | (1L << (ERROR - 291))
                                | (1L << (ERRORS - 291))
                                | (1L << (ESCAPE - 291))
                                | (1L << (EVEN - 291))
                                | (1L << (EVENT - 291))
                                | (1L << (EVENTS - 291))
                                | (1L << (EVERY - 291))
                                | (1L << (EXCHANGE - 291))
                                | (1L << (EXCLUSIVE - 291))
                                | (1L << (EXPIRE - 291))
                                | (1L << (EXTENDED - 291))
                                | (1L << (EXTENT_SIZE - 291))
                                | (1L << (FAST - 291))
                                | (1L << (FAULTS - 291))
                                | (1L << (FIELDS - 291))
                                | (1L << (FILE_BLOCK_SIZE - 291))
                                | (1L << (FILTER - 291))
                                | (1L << (FIRST - 291))
                                | (1L << (FIXED - 291))
                                | (1L << (FOLLOWS - 291))
                                | (1L << (FULL - 291))
                                | (1L << (FUNCTION - 291))
                                | (1L << (GLOBAL - 291))
                                | (1L << (GRANTS - 291))
                                | (1L << (GROUP_REPLICATION - 291))
                                | (1L << (HASH - 291))
                                | (1L << (HOST - 291))
                                | (1L << (IDENTIFIED - 291))
                                | (1L << (IGNORE_SERVER_IDS - 291))
                                | (1L << (IMPORT - 291))
                                | (1L << (INDEXES - 291))
                                | (1L << (INITIAL_SIZE - 291))
                                | (1L << (INPLACE - 291))
                                | (1L << (INSERT_METHOD - 291))))
                        != 0)
                || ((((_la - 355)) & ~0x3f) == 0
                    && ((1L << (_la - 355))
                            & ((1L << (INSTANCE - 355))
                                | (1L << (INVOKER - 355))
                                | (1L << (IO - 355))
                                | (1L << (IO_THREAD - 355))
                                | (1L << (IPC - 355))
                                | (1L << (ISOLATION - 355))
                                | (1L << (ISSUER - 355))
                                | (1L << (KEY_BLOCK_SIZE - 355))
                                | (1L << (LANGUAGE - 355))
                                | (1L << (LAST - 355))
                                | (1L << (LEAVES - 355))
                                | (1L << (LESS - 355))
                                | (1L << (LEVEL - 355))
                                | (1L << (LIST - 355))
                                | (1L << (LOCAL - 355))
                                | (1L << (LOGFILE - 355))
                                | (1L << (LOGS - 355))
                                | (1L << (MASTER - 355))
                                | (1L << (MASTER_AUTO_POSITION - 355))
                                | (1L << (MASTER_CONNECT_RETRY - 355))
                                | (1L << (MASTER_DELAY - 355))
                                | (1L << (MASTER_HEARTBEAT_PERIOD - 355))
                                | (1L << (MASTER_HOST - 355))
                                | (1L << (MASTER_LOG_FILE - 355))
                                | (1L << (MASTER_LOG_POS - 355))
                                | (1L << (MASTER_PASSWORD - 355))
                                | (1L << (MASTER_PORT - 355))
                                | (1L << (MASTER_RETRY_COUNT - 355))
                                | (1L << (MASTER_SSL - 355))
                                | (1L << (MASTER_SSL_CA - 355))
                                | (1L << (MASTER_SSL_CAPATH - 355))
                                | (1L << (MASTER_SSL_CERT - 355))
                                | (1L << (MASTER_SSL_CIPHER - 355))
                                | (1L << (MASTER_SSL_CRL - 355))
                                | (1L << (MASTER_SSL_CRLPATH - 355))
                                | (1L << (MASTER_SSL_KEY - 355))
                                | (1L << (MASTER_TLS_VERSION - 355))
                                | (1L << (MASTER_USER - 355))
                                | (1L << (MAX_CONNECTIONS_PER_HOUR - 355))
                                | (1L << (MAX_QUERIES_PER_HOUR - 355))
                                | (1L << (MAX_ROWS - 355))
                                | (1L << (MAX_SIZE - 355))
                                | (1L << (MAX_UPDATES_PER_HOUR - 355))
                                | (1L << (MAX_USER_CONNECTIONS - 355))
                                | (1L << (MEDIUM - 355))
                                | (1L << (MERGE - 355))
                                | (1L << (MID - 355))
                                | (1L << (MIGRATE - 355))
                                | (1L << (MIN_ROWS - 355))
                                | (1L << (MODIFY - 355))
                                | (1L << (MUTEX - 355))
                                | (1L << (MYSQL - 355))
                                | (1L << (NAME - 355))
                                | (1L << (NAMES - 355))
                                | (1L << (NCHAR - 355))
                                | (1L << (NEVER - 355))
                                | (1L << (NO - 355))
                                | (1L << (NODEGROUP - 355))
                                | (1L << (NONE - 355))
                                | (1L << (OFFLINE - 355))
                                | (1L << (OFFSET - 355))))
                        != 0)
                || ((((_la - 419)) & ~0x3f) == 0
                    && ((1L << (_la - 419))
                            & ((1L << (OJ - 419))
                                | (1L << (OLD_PASSWORD - 419))
                                | (1L << (ONE - 419))
                                | (1L << (ONLINE - 419))
                                | (1L << (ONLY - 419))
                                | (1L << (OPTIMIZER_COSTS - 419))
                                | (1L << (OPTIONS - 419))
                                | (1L << (OWNER - 419))
                                | (1L << (PACK_KEYS - 419))
                                | (1L << (PAGE - 419))
                                | (1L << (PARSER - 419))
                                | (1L << (PARTIAL - 419))
                                | (1L << (PARTITIONING - 419))
                                | (1L << (PARTITIONS - 419))
                                | (1L << (PASSWORD - 419))
                                | (1L << (PHASE - 419))
                                | (1L << (PLUGIN_DIR - 419))
                                | (1L << (PLUGINS - 419))
                                | (1L << (PORT - 419))
                                | (1L << (PRECEDES - 419))
                                | (1L << (PREPARE - 419))
                                | (1L << (PRESERVE - 419))
                                | (1L << (PREV - 419))
                                | (1L << (PROCESSLIST - 419))
                                | (1L << (PROFILE - 419))
                                | (1L << (PROFILES - 419))
                                | (1L << (PROXY - 419))
                                | (1L << (QUERY - 419))
                                | (1L << (QUICK - 419))
                                | (1L << (REBUILD - 419))
                                | (1L << (RECOVER - 419))
                                | (1L << (REDO_BUFFER_SIZE - 419))
                                | (1L << (REDUNDANT - 419))
                                | (1L << (RELAY_LOG_FILE - 419))
                                | (1L << (RELAY_LOG_POS - 419))
                                | (1L << (RELAYLOG - 419))
                                | (1L << (REMOVE - 419))
                                | (1L << (REORGANIZE - 419))
                                | (1L << (REPAIR - 419))
                                | (1L << (REPLICATE_DO_DB - 419))
                                | (1L << (REPLICATE_DO_TABLE - 419))
                                | (1L << (REPLICATE_IGNORE_DB - 419))
                                | (1L << (REPLICATE_IGNORE_TABLE - 419))
                                | (1L << (REPLICATE_REWRITE_DB - 419))
                                | (1L << (REPLICATE_WILD_DO_TABLE - 419))
                                | (1L << (REPLICATE_WILD_IGNORE_TABLE - 419))
                                | (1L << (REPLICATION - 419))
                                | (1L << (RESUME - 419))
                                | (1L << (RETURNS - 419))
                                | (1L << (ROLLBACK - 419))
                                | (1L << (ROLLUP - 419))
                                | (1L << (ROTATE - 419))
                                | (1L << (ROW - 419))
                                | (1L << (ROWS - 419))
                                | (1L << (ROW_FORMAT - 419))
                                | (1L << (SAVEPOINT - 419))
                                | (1L << (SCHEDULE - 419))
                                | (1L << (SECURITY - 419))
                                | (1L << (SERVER - 419))
                                | (1L << (SESSION - 419))))
                        != 0)
                || ((((_la - 483)) & ~0x3f) == 0
                    && ((1L << (_la - 483))
                            & ((1L << (SHARE - 483))
                                | (1L << (SHARED - 483))
                                | (1L << (SIGNED - 483))
                                | (1L << (SIMPLE - 483))
                                | (1L << (SLAVE - 483))
                                | (1L << (SNAPSHOT - 483))
                                | (1L << (SOCKET - 483))
                                | (1L << (SOME - 483))
                                | (1L << (SOUNDS - 483))
                                | (1L << (SOURCE - 483))
                                | (1L << (SQL_AFTER_GTIDS - 483))
                                | (1L << (SQL_AFTER_MTS_GAPS - 483))
                                | (1L << (SQL_BEFORE_GTIDS - 483))
                                | (1L << (SQL_BUFFER_RESULT - 483))
                                | (1L << (SQL_CACHE - 483))
                                | (1L << (SQL_NO_CACHE - 483))
                                | (1L << (SQL_THREAD - 483))
                                | (1L << (START - 483))
                                | (1L << (STARTS - 483))
                                | (1L << (STATS_AUTO_RECALC - 483))
                                | (1L << (STATS_PERSISTENT - 483))
                                | (1L << (STATS_SAMPLE_PAGES - 483))
                                | (1L << (STATUS - 483))
                                | (1L << (STOP - 483))
                                | (1L << (STORAGE - 483))
                                | (1L << (STRING - 483))
                                | (1L << (SUBJECT - 483))
                                | (1L << (SUBPARTITION - 483))
                                | (1L << (SUBPARTITIONS - 483))
                                | (1L << (SUSPEND - 483))
                                | (1L << (SWAPS - 483))
                                | (1L << (SWITCHES - 483))
                                | (1L << (TABLESPACE - 483))
                                | (1L << (TEMPORARY - 483))
                                | (1L << (TEMPTABLE - 483))
                                | (1L << (THAN - 483))
                                | (1L << (TRANSACTION - 483))
                                | (1L << (TRUNCATE - 483))
                                | (1L << (UNDEFINED - 483))
                                | (1L << (UNDOFILE - 483))
                                | (1L << (UNDO_BUFFER_SIZE - 483))
                                | (1L << (UNKNOWN - 483))
                                | (1L << (UPGRADE - 483))
                                | (1L << (USER - 483))
                                | (1L << (VALIDATION - 483))
                                | (1L << (VALUE - 483))
                                | (1L << (VARIABLES - 483))
                                | (1L << (VIEW - 483))
                                | (1L << (WAIT - 483))
                                | (1L << (WARNINGS - 483))
                                | (1L << (WITHOUT - 483))
                                | (1L << (WORK - 483))
                                | (1L << (WRAPPER - 483))
                                | (1L << (X509 - 483))))
                        != 0)
                || ((((_la - 547)) & ~0x3f) == 0
                    && ((1L << (_la - 547))
                            & ((1L << (XA - 547))
                                | (1L << (XML - 547))
                                | (1L << (QUARTER - 547))
                                | (1L << (MONTH - 547))
                                | (1L << (DAY - 547))
                                | (1L << (HOUR - 547))
                                | (1L << (MINUTE - 547))
                                | (1L << (WEEK - 547))
                                | (1L << (SECOND - 547))
                                | (1L << (MICROSECOND - 547))
                                | (1L << (ARMSCII8 - 547))
                                | (1L << (ASCII - 547))
                                | (1L << (BIG5 - 547))
                                | (1L << (CP1250 - 547))
                                | (1L << (CP1251 - 547))
                                | (1L << (CP1256 - 547))
                                | (1L << (CP1257 - 547))
                                | (1L << (CP850 - 547))
                                | (1L << (CP852 - 547))
                                | (1L << (CP866 - 547))
                                | (1L << (CP932 - 547))
                                | (1L << (DEC8 - 547))
                                | (1L << (EUCJPMS - 547))
                                | (1L << (EUCKR - 547))
                                | (1L << (GB2312 - 547))
                                | (1L << (GBK - 547))
                                | (1L << (GEOSTD8 - 547))
                                | (1L << (GREEK - 547))
                                | (1L << (HEBREW - 547))
                                | (1L << (HP8 - 547))
                                | (1L << (KEYBCS2 - 547))
                                | (1L << (KOI8R - 547))
                                | (1L << (KOI8U - 547))
                                | (1L << (LATIN1 - 547))
                                | (1L << (LATIN2 - 547))
                                | (1L << (LATIN5 - 547))
                                | (1L << (LATIN7 - 547))
                                | (1L << (MACCE - 547))
                                | (1L << (MACROMAN - 547))
                                | (1L << (SJIS - 547))
                                | (1L << (SWE7 - 547))
                                | (1L << (TIS620 - 547))
                                | (1L << (UCS2 - 547))
                                | (1L << (UJIS - 547))
                                | (1L << (UTF16 - 547))
                                | (1L << (UTF16LE - 547))
                                | (1L << (UTF32 - 547))
                                | (1L << (UTF8 - 547))
                                | (1L << (UTF8MB3 - 547))
                                | (1L << (UTF8MB4 - 547))))
                        != 0)
                || ((((_la - 616)) & ~0x3f) == 0
                    && ((1L << (_la - 616))
                            & ((1L << (MEMORY - 616))
                                | (1L << (GEOMETRYCOLLECTION - 616))
                                | (1L << (LINESTRING - 616))
                                | (1L << (MULTILINESTRING - 616))
                                | (1L << (MULTIPOINT - 616))
                                | (1L << (MULTIPOLYGON - 616))
                                | (1L << (POINT - 616))
                                | (1L << (POLYGON - 616))
                                | (1L << (ABS - 616))
                                | (1L << (ACOS - 616))
                                | (1L << (ADDDATE - 616))
                                | (1L << (ADDTIME - 616))
                                | (1L << (AES_DECRYPT - 616))
                                | (1L << (AES_ENCRYPT - 616))
                                | (1L << (AREA - 616))
                                | (1L << (ASBINARY - 616))
                                | (1L << (ASIN - 616))
                                | (1L << (ASTEXT - 616))
                                | (1L << (ASWKB - 616))
                                | (1L << (ASWKT - 616))
                                | (1L << (ASYMMETRIC_DECRYPT - 616))
                                | (1L << (ASYMMETRIC_DERIVE - 616))
                                | (1L << (ASYMMETRIC_ENCRYPT - 616))
                                | (1L << (ASYMMETRIC_SIGN - 616))
                                | (1L << (ASYMMETRIC_VERIFY - 616))
                                | (1L << (ATAN - 616))
                                | (1L << (ATAN2 - 616))
                                | (1L << (BENCHMARK - 616))
                                | (1L << (BIN - 616))
                                | (1L << (BIT_COUNT - 616))
                                | (1L << (BIT_LENGTH - 616))
                                | (1L << (BUFFER - 616))
                                | (1L << (CEIL - 616))
                                | (1L << (CEILING - 616))
                                | (1L << (CENTROID - 616))
                                | (1L << (CHARACTER_LENGTH - 616))
                                | (1L << (CHARSET - 616))
                                | (1L << (CHAR_LENGTH - 616))
                                | (1L << (COERCIBILITY - 616))
                                | (1L << (COLLATION - 616))
                                | (1L << (COMPRESS - 616))
                                | (1L << (CONCAT - 616))
                                | (1L << (CONCAT_WS - 616))
                                | (1L << (CONNECTION_ID - 616))
                                | (1L << (CONV - 616))
                                | (1L << (CONVERT_TZ - 616))
                                | (1L << (COS - 616))
                                | (1L << (COT - 616))
                                | (1L << (CRC32 - 616))
                                | (1L << (CREATE_ASYMMETRIC_PRIV_KEY - 616))
                                | (1L << (CREATE_ASYMMETRIC_PUB_KEY - 616))
                                | (1L << (CREATE_DH_PARAMETERS - 616))
                                | (1L << (CREATE_DIGEST - 616))
                                | (1L << (CROSSES - 616))
                                | (1L << (DATEDIFF - 616))))
                        != 0)
                || ((((_la - 680)) & ~0x3f) == 0
                    && ((1L << (_la - 680))
                            & ((1L << (DATE_FORMAT - 680))
                                | (1L << (DAYNAME - 680))
                                | (1L << (DAYOFMONTH - 680))
                                | (1L << (DAYOFWEEK - 680))
                                | (1L << (DAYOFYEAR - 680))
                                | (1L << (DECODE - 680))
                                | (1L << (DEGREES - 680))
                                | (1L << (DES_DECRYPT - 680))
                                | (1L << (DES_ENCRYPT - 680))
                                | (1L << (DIMENSION - 680))
                                | (1L << (DISJOINT - 680))
                                | (1L << (ELT - 680))
                                | (1L << (ENCODE - 680))
                                | (1L << (ENCRYPT - 680))
                                | (1L << (ENDPOINT - 680))
                                | (1L << (ENVELOPE - 680))
                                | (1L << (EQUALS - 680))
                                | (1L << (EXP - 680))
                                | (1L << (EXPORT_SET - 680))
                                | (1L << (EXTERIORRING - 680))
                                | (1L << (EXTRACTVALUE - 680))
                                | (1L << (FIELD - 680))
                                | (1L << (FIND_IN_SET - 680))
                                | (1L << (FLOOR - 680))
                                | (1L << (FORMAT - 680))
                                | (1L << (FOUND_ROWS - 680))
                                | (1L << (FROM_BASE64 - 680))
                                | (1L << (FROM_DAYS - 680))
                                | (1L << (FROM_UNIXTIME - 680))
                                | (1L << (GEOMCOLLFROMTEXT - 680))
                                | (1L << (GEOMCOLLFROMWKB - 680))
                                | (1L << (GEOMETRYCOLLECTIONFROMTEXT - 680))
                                | (1L << (GEOMETRYCOLLECTIONFROMWKB - 680))
                                | (1L << (GEOMETRYFROMTEXT - 680))
                                | (1L << (GEOMETRYFROMWKB - 680))
                                | (1L << (GEOMETRYN - 680))
                                | (1L << (GEOMETRYTYPE - 680))
                                | (1L << (GEOMFROMTEXT - 680))
                                | (1L << (GEOMFROMWKB - 680))
                                | (1L << (GET_FORMAT - 680))
                                | (1L << (GET_LOCK - 680))
                                | (1L << (GLENGTH - 680))
                                | (1L << (GREATEST - 680))
                                | (1L << (GTID_SUBSET - 680))
                                | (1L << (GTID_SUBTRACT - 680))
                                | (1L << (HEX - 680))
                                | (1L << (IFNULL - 680))
                                | (1L << (INET6_ATON - 680))
                                | (1L << (INET6_NTOA - 680))
                                | (1L << (INET_ATON - 680))
                                | (1L << (INET_NTOA - 680))
                                | (1L << (INSTR - 680))
                                | (1L << (INTERIORRINGN - 680))
                                | (1L << (INTERSECTS - 680))
                                | (1L << (ISCLOSED - 680))
                                | (1L << (ISEMPTY - 680))
                                | (1L << (ISNULL - 680))
                                | (1L << (ISSIMPLE - 680))
                                | (1L << (IS_FREE_LOCK - 680))
                                | (1L << (IS_IPV4 - 680))
                                | (1L << (IS_IPV4_COMPAT - 680))
                                | (1L << (IS_IPV4_MAPPED - 680))
                                | (1L << (IS_IPV6 - 680))
                                | (1L << (IS_USED_LOCK - 680))))
                        != 0)
                || ((((_la - 744)) & ~0x3f) == 0
                    && ((1L << (_la - 744))
                            & ((1L << (LAST_INSERT_ID - 744))
                                | (1L << (LCASE - 744))
                                | (1L << (LEAST - 744))
                                | (1L << (LENGTH - 744))
                                | (1L << (LINEFROMTEXT - 744))
                                | (1L << (LINEFROMWKB - 744))
                                | (1L << (LINESTRINGFROMTEXT - 744))
                                | (1L << (LINESTRINGFROMWKB - 744))
                                | (1L << (LN - 744))
                                | (1L << (LOAD_FILE - 744))
                                | (1L << (LOCATE - 744))
                                | (1L << (LOG - 744))
                                | (1L << (LOG10 - 744))
                                | (1L << (LOG2 - 744))
                                | (1L << (LOWER - 744))
                                | (1L << (LPAD - 744))
                                | (1L << (LTRIM - 744))
                                | (1L << (MAKEDATE - 744))
                                | (1L << (MAKETIME - 744))
                                | (1L << (MAKE_SET - 744))
                                | (1L << (MASTER_POS_WAIT - 744))
                                | (1L << (MBRCONTAINS - 744))
                                | (1L << (MBRDISJOINT - 744))
                                | (1L << (MBREQUAL - 744))
                                | (1L << (MBRINTERSECTS - 744))
                                | (1L << (MBROVERLAPS - 744))
                                | (1L << (MBRTOUCHES - 744))
                                | (1L << (MBRWITHIN - 744))
                                | (1L << (MD5 - 744))
                                | (1L << (MLINEFROMTEXT - 744))
                                | (1L << (MLINEFROMWKB - 744))
                                | (1L << (MONTHNAME - 744))
                                | (1L << (MPOINTFROMTEXT - 744))
                                | (1L << (MPOINTFROMWKB - 744))
                                | (1L << (MPOLYFROMTEXT - 744))
                                | (1L << (MPOLYFROMWKB - 744))
                                | (1L << (MULTILINESTRINGFROMTEXT - 744))
                                | (1L << (MULTILINESTRINGFROMWKB - 744))
                                | (1L << (MULTIPOINTFROMTEXT - 744))
                                | (1L << (MULTIPOINTFROMWKB - 744))
                                | (1L << (MULTIPOLYGONFROMTEXT - 744))
                                | (1L << (MULTIPOLYGONFROMWKB - 744))
                                | (1L << (NAME_CONST - 744))
                                | (1L << (NULLIF - 744))
                                | (1L << (NUMGEOMETRIES - 744))
                                | (1L << (NUMINTERIORRINGS - 744))
                                | (1L << (NUMPOINTS - 744))
                                | (1L << (OCT - 744))
                                | (1L << (OCTET_LENGTH - 744))
                                | (1L << (ORD - 744))
                                | (1L << (OVERLAPS - 744))
                                | (1L << (PERIOD_ADD - 744))
                                | (1L << (PERIOD_DIFF - 744))
                                | (1L << (PI - 744))
                                | (1L << (POINTFROMTEXT - 744))
                                | (1L << (POINTFROMWKB - 744))
                                | (1L << (POINTN - 744))
                                | (1L << (POLYFROMTEXT - 744))
                                | (1L << (POLYFROMWKB - 744))
                                | (1L << (POLYGONFROMTEXT - 744))
                                | (1L << (POLYGONFROMWKB - 744))
                                | (1L << (POW - 744))
                                | (1L << (POWER - 744))
                                | (1L << (QUOTE - 744))))
                        != 0)
                || ((((_la - 808)) & ~0x3f) == 0
                    && ((1L << (_la - 808))
                            & ((1L << (RADIANS - 808))
                                | (1L << (RAND - 808))
                                | (1L << (RANDOM_BYTES - 808))
                                | (1L << (RELEASE_LOCK - 808))
                                | (1L << (REVERSE - 808))
                                | (1L << (ROUND - 808))
                                | (1L << (ROW_COUNT - 808))
                                | (1L << (RPAD - 808))
                                | (1L << (RTRIM - 808))
                                | (1L << (SEC_TO_TIME - 808))
                                | (1L << (SESSION_USER - 808))
                                | (1L << (SHA - 808))
                                | (1L << (SHA1 - 808))
                                | (1L << (SHA2 - 808))
                                | (1L << (SIGN - 808))
                                | (1L << (SIN - 808))
                                | (1L << (SLEEP - 808))
                                | (1L << (SOUNDEX - 808))
                                | (1L << (SQL_THREAD_WAIT_AFTER_GTIDS - 808))
                                | (1L << (SQRT - 808))
                                | (1L << (SRID - 808))
                                | (1L << (STARTPOINT - 808))
                                | (1L << (STRCMP - 808))
                                | (1L << (STR_TO_DATE - 808))
                                | (1L << (ST_AREA - 808))
                                | (1L << (ST_ASBINARY - 808))
                                | (1L << (ST_ASTEXT - 808))
                                | (1L << (ST_ASWKB - 808))
                                | (1L << (ST_ASWKT - 808))
                                | (1L << (ST_BUFFER - 808))
                                | (1L << (ST_CENTROID - 808))
                                | (1L << (ST_CONTAINS - 808))
                                | (1L << (ST_CROSSES - 808))
                                | (1L << (ST_DIFFERENCE - 808))
                                | (1L << (ST_DIMENSION - 808))
                                | (1L << (ST_DISJOINT - 808))
                                | (1L << (ST_DISTANCE - 808))
                                | (1L << (ST_ENDPOINT - 808))
                                | (1L << (ST_ENVELOPE - 808))
                                | (1L << (ST_EQUALS - 808))
                                | (1L << (ST_EXTERIORRING - 808))
                                | (1L << (ST_GEOMCOLLFROMTEXT - 808))
                                | (1L << (ST_GEOMCOLLFROMTXT - 808))
                                | (1L << (ST_GEOMCOLLFROMWKB - 808))
                                | (1L << (ST_GEOMETRYCOLLECTIONFROMTEXT - 808))
                                | (1L << (ST_GEOMETRYCOLLECTIONFROMWKB - 808))
                                | (1L << (ST_GEOMETRYFROMTEXT - 808))
                                | (1L << (ST_GEOMETRYFROMWKB - 808))
                                | (1L << (ST_GEOMETRYN - 808))
                                | (1L << (ST_GEOMETRYTYPE - 808))
                                | (1L << (ST_GEOMFROMTEXT - 808))
                                | (1L << (ST_GEOMFROMWKB - 808))
                                | (1L << (ST_INTERIORRINGN - 808))
                                | (1L << (ST_INTERSECTION - 808))
                                | (1L << (ST_INTERSECTS - 808))
                                | (1L << (ST_ISCLOSED - 808))
                                | (1L << (ST_ISEMPTY - 808))
                                | (1L << (ST_ISSIMPLE - 808))
                                | (1L << (ST_LINEFROMTEXT - 808))
                                | (1L << (ST_LINEFROMWKB - 808))
                                | (1L << (ST_LINESTRINGFROMTEXT - 808))
                                | (1L << (ST_LINESTRINGFROMWKB - 808))
                                | (1L << (ST_NUMGEOMETRIES - 808))
                                | (1L << (ST_NUMINTERIORRING - 808))))
                        != 0)
                || ((((_la - 872)) & ~0x3f) == 0
                    && ((1L << (_la - 872))
                            & ((1L << (ST_NUMINTERIORRINGS - 872))
                                | (1L << (ST_NUMPOINTS - 872))
                                | (1L << (ST_OVERLAPS - 872))
                                | (1L << (ST_POINTFROMTEXT - 872))
                                | (1L << (ST_POINTFROMWKB - 872))
                                | (1L << (ST_POINTN - 872))
                                | (1L << (ST_POLYFROMTEXT - 872))
                                | (1L << (ST_POLYFROMWKB - 872))
                                | (1L << (ST_POLYGONFROMTEXT - 872))
                                | (1L << (ST_POLYGONFROMWKB - 872))
                                | (1L << (ST_SRID - 872))
                                | (1L << (ST_STARTPOINT - 872))
                                | (1L << (ST_SYMDIFFERENCE - 872))
                                | (1L << (ST_TOUCHES - 872))
                                | (1L << (ST_UNION - 872))
                                | (1L << (ST_WITHIN - 872))
                                | (1L << (ST_X - 872))
                                | (1L << (ST_Y - 872))
                                | (1L << (SUBDATE - 872))
                                | (1L << (SUBSTRING_INDEX - 872))
                                | (1L << (SUBTIME - 872))
                                | (1L << (SYSTEM_USER - 872))
                                | (1L << (TAN - 872))
                                | (1L << (TIMEDIFF - 872))
                                | (1L << (TIMESTAMPADD - 872))
                                | (1L << (TIMESTAMPDIFF - 872))
                                | (1L << (TIME_FORMAT - 872))
                                | (1L << (TIME_TO_SEC - 872))
                                | (1L << (TOUCHES - 872))
                                | (1L << (TO_BASE64 - 872))
                                | (1L << (TO_DAYS - 872))
                                | (1L << (TO_SECONDS - 872))
                                | (1L << (UCASE - 872))
                                | (1L << (UNCOMPRESS - 872))
                                | (1L << (UNCOMPRESSED_LENGTH - 872))
                                | (1L << (UNHEX - 872))
                                | (1L << (UNIX_TIMESTAMP - 872))
                                | (1L << (UPDATEXML - 872))
                                | (1L << (UPPER - 872))
                                | (1L << (UUID - 872))
                                | (1L << (UUID_SHORT - 872))
                                | (1L << (VALIDATE_PASSWORD_STRENGTH - 872))
                                | (1L << (VERSION - 872))
                                | (1L << (WAIT_UNTIL_SQL_THREAD_AFTER_GTIDS - 872))
                                | (1L << (WEEKDAY - 872))
                                | (1L << (WEEKOFYEAR - 872))
                                | (1L << (WEIGHT_STRING - 872))
                                | (1L << (WITHIN - 872))
                                | (1L << (YEARWEEK - 872))
                                | (1L << (Y_FUNCTION - 872))
                                | (1L << (X_FUNCTION - 872))
                                | (1L << (PLUS - 872))))
                        != 0)
                || ((((_la - 937)) & ~0x3f) == 0
                    && ((1L << (_la - 937))
                            & ((1L << (MINUS - 937))
                                | (1L << (EXCLAMATION_SYMBOL - 937))
                                | (1L << (BIT_NOT_OP - 937))
                                | (1L << (LR_BRACKET - 937))
                                | (1L << (ZERO_DECIMAL - 937))
                                | (1L << (ONE_DECIMAL - 937))
                                | (1L << (TWO_DECIMAL - 937))
                                | (1L << (CHARSET_REVERSE_QOUTE_STRING - 937))
                                | (1L << (STRING_LITERAL - 937))
                                | (1L << (DECIMAL_LITERAL - 937))
                                | (1L << (HEXADECIMAL_LITERAL - 937))
                                | (1L << (REAL_LITERAL - 937))
                                | (1L << (NULL_SPEC_LITERAL - 937))
                                | (1L << (BIT_STRING - 937))
                                | (1L << (STRING_CHARSET_NAME - 937))
                                | (1L << (ID - 937))
                                | (1L << (REVERSE_QUOTE_ID - 937))
                                | (1L << (LOCAL_ID - 937))))
                        != 0)) {
              {
                setState(230);
                functionArgs();
              }
            }

            setState(233);
            match(RR_BRACKET);
          }
          break;
        case 3:
          _localctx = new UdfFunctionCallContext(_localctx);
          enterOuterAlt(_localctx, 3);
          {
            setState(235);
            fullId();
            setState(236);
            match(LR_BRACKET);
            setState(238);
            _errHandler.sync(this);
            _la = _input.LA(1);
            if ((((_la) & ~0x3f) == 0
                    && ((1L << _la)
                            & ((1L << CASE)
                                | (1L << CAST)
                                | (1L << CONVERT)
                                | (1L << CURRENT_USER)
                                | (1L << DATABASE)
                                | (1L << FALSE)))
                        != 0)
                || ((((_la - 66)) & ~0x3f) == 0
                    && ((1L << (_la - 66))
                            & ((1L << (IF - 66))
                                | (1L << (INSERT - 66))
                                | (1L << (INTERVAL - 66))
                                | (1L << (LEFT - 66))
                                | (1L << (NOT - 66))
                                | (1L << (NULL_LITERAL - 66))
                                | (1L << (REPLACE - 66))
                                | (1L << (RIGHT - 66))))
                        != 0)
                || ((((_la - 153)) & ~0x3f) == 0
                    && ((1L << (_la - 153))
                            & ((1L << (TRUE - 153))
                                | (1L << (VALUES - 153))
                                | (1L << (DATE - 153))
                                | (1L << (TIME - 153))
                                | (1L << (TIMESTAMP - 153))
                                | (1L << (DATETIME - 153))
                                | (1L << (YEAR - 153))
                                | (1L << (CHAR - 153))
                                | (1L << (BINARY - 153))
                                | (1L << (TEXT - 153))
                                | (1L << (ENUM - 153))
                                | (1L << (COUNT - 153))))
                        != 0)
                || ((((_la - 227)) & ~0x3f) == 0
                    && ((1L << (_la - 227))
                            & ((1L << (CURRENT_DATE - 227))
                                | (1L << (CURRENT_TIME - 227))
                                | (1L << (CURRENT_TIMESTAMP - 227))
                                | (1L << (LOCALTIME - 227))
                                | (1L << (CURDATE - 227))
                                | (1L << (CURTIME - 227))
                                | (1L << (DATE_ADD - 227))
                                | (1L << (DATE_SUB - 227))
                                | (1L << (EXTRACT - 227))
                                | (1L << (LOCALTIMESTAMP - 227))
                                | (1L << (NOW - 227))
                                | (1L << (POSITION - 227))
                                | (1L << (SUBSTR - 227))
                                | (1L << (SUBSTRING - 227))
                                | (1L << (SYSDATE - 227))
                                | (1L << (TRIM - 227))
                                | (1L << (UTC_DATE - 227))
                                | (1L << (UTC_TIME - 227))
                                | (1L << (UTC_TIMESTAMP - 227))
                                | (1L << (ACCOUNT - 227))
                                | (1L << (ACTION - 227))
                                | (1L << (AFTER - 227))
                                | (1L << (AGGREGATE - 227))
                                | (1L << (ALGORITHM - 227))
                                | (1L << (ANY - 227))
                                | (1L << (AT - 227))
                                | (1L << (AUTHORS - 227))
                                | (1L << (AUTOCOMMIT - 227))
                                | (1L << (AUTOEXTEND_SIZE - 227))
                                | (1L << (AUTO_INCREMENT - 227))
                                | (1L << (AVG_ROW_LENGTH - 227))
                                | (1L << (BEGIN - 227))
                                | (1L << (BINLOG - 227))
                                | (1L << (BIT - 227))
                                | (1L << (BLOCK - 227))
                                | (1L << (BOOL - 227))
                                | (1L << (BOOLEAN - 227))
                                | (1L << (BTREE - 227))
                                | (1L << (CASCADED - 227))
                                | (1L << (CHAIN - 227))
                                | (1L << (CHANGED - 227))
                                | (1L << (CHANNEL - 227))
                                | (1L << (CHECKSUM - 227))
                                | (1L << (CIPHER - 227))
                                | (1L << (CLIENT - 227))
                                | (1L << (COALESCE - 227))
                                | (1L << (CODE - 227))
                                | (1L << (COLUMNS - 227))
                                | (1L << (COLUMN_FORMAT - 227))
                                | (1L << (COMMENT - 227))
                                | (1L << (COMMIT - 227))
                                | (1L << (COMPACT - 227))
                                | (1L << (COMPLETION - 227))
                                | (1L << (COMPRESSED - 227))
                                | (1L << (COMPRESSION - 227))
                                | (1L << (CONCURRENT - 227))
                                | (1L << (CONNECTION - 227))
                                | (1L << (CONSISTENT - 227))
                                | (1L << (CONTAINS - 227))
                                | (1L << (CONTEXT - 227))
                                | (1L << (CONTRIBUTORS - 227))
                                | (1L << (COPY - 227))))
                        != 0)
                || ((((_la - 291)) & ~0x3f) == 0
                    && ((1L << (_la - 291))
                            & ((1L << (CPU - 291))
                                | (1L << (DATA - 291))
                                | (1L << (DATAFILE - 291))
                                | (1L << (DEALLOCATE - 291))
                                | (1L << (DEFAULT_AUTH - 291))
                                | (1L << (DEFINER - 291))
                                | (1L << (DELAY_KEY_WRITE - 291))
                                | (1L << (DIRECTORY - 291))
                                | (1L << (DISABLE - 291))
                                | (1L << (DISCARD - 291))
                                | (1L << (DISK - 291))
                                | (1L << (DO - 291))
                                | (1L << (DUMPFILE - 291))
                                | (1L << (DUPLICATE - 291))
                                | (1L << (DYNAMIC - 291))
                                | (1L << (ENABLE - 291))
                                | (1L << (ENCRYPTION - 291))
                                | (1L << (ENDS - 291))
                                | (1L << (ENGINE - 291))
                                | (1L << (ENGINES - 291))
                                | (1L << (ERROR - 291))
                                | (1L << (ERRORS - 291))
                                | (1L << (ESCAPE - 291))
                                | (1L << (EVEN - 291))
                                | (1L << (EVENT - 291))
                                | (1L << (EVENTS - 291))
                                | (1L << (EVERY - 291))
                                | (1L << (EXCHANGE - 291))
                                | (1L << (EXCLUSIVE - 291))
                                | (1L << (EXPIRE - 291))
                                | (1L << (EXTENDED - 291))
                                | (1L << (EXTENT_SIZE - 291))
                                | (1L << (FAST - 291))
                                | (1L << (FAULTS - 291))
                                | (1L << (FIELDS - 291))
                                | (1L << (FILE_BLOCK_SIZE - 291))
                                | (1L << (FILTER - 291))
                                | (1L << (FIRST - 291))
                                | (1L << (FIXED - 291))
                                | (1L << (FOLLOWS - 291))
                                | (1L << (FULL - 291))
                                | (1L << (FUNCTION - 291))
                                | (1L << (GLOBAL - 291))
                                | (1L << (GRANTS - 291))
                                | (1L << (GROUP_REPLICATION - 291))
                                | (1L << (HASH - 291))
                                | (1L << (HOST - 291))
                                | (1L << (IDENTIFIED - 291))
                                | (1L << (IGNORE_SERVER_IDS - 291))
                                | (1L << (IMPORT - 291))
                                | (1L << (INDEXES - 291))
                                | (1L << (INITIAL_SIZE - 291))
                                | (1L << (INPLACE - 291))
                                | (1L << (INSERT_METHOD - 291))))
                        != 0)
                || ((((_la - 355)) & ~0x3f) == 0
                    && ((1L << (_la - 355))
                            & ((1L << (INSTANCE - 355))
                                | (1L << (INVOKER - 355))
                                | (1L << (IO - 355))
                                | (1L << (IO_THREAD - 355))
                                | (1L << (IPC - 355))
                                | (1L << (ISOLATION - 355))
                                | (1L << (ISSUER - 355))
                                | (1L << (KEY_BLOCK_SIZE - 355))
                                | (1L << (LANGUAGE - 355))
                                | (1L << (LAST - 355))
                                | (1L << (LEAVES - 355))
                                | (1L << (LESS - 355))
                                | (1L << (LEVEL - 355))
                                | (1L << (LIST - 355))
                                | (1L << (LOCAL - 355))
                                | (1L << (LOGFILE - 355))
                                | (1L << (LOGS - 355))
                                | (1L << (MASTER - 355))
                                | (1L << (MASTER_AUTO_POSITION - 355))
                                | (1L << (MASTER_CONNECT_RETRY - 355))
                                | (1L << (MASTER_DELAY - 355))
                                | (1L << (MASTER_HEARTBEAT_PERIOD - 355))
                                | (1L << (MASTER_HOST - 355))
                                | (1L << (MASTER_LOG_FILE - 355))
                                | (1L << (MASTER_LOG_POS - 355))
                                | (1L << (MASTER_PASSWORD - 355))
                                | (1L << (MASTER_PORT - 355))
                                | (1L << (MASTER_RETRY_COUNT - 355))
                                | (1L << (MASTER_SSL - 355))
                                | (1L << (MASTER_SSL_CA - 355))
                                | (1L << (MASTER_SSL_CAPATH - 355))
                                | (1L << (MASTER_SSL_CERT - 355))
                                | (1L << (MASTER_SSL_CIPHER - 355))
                                | (1L << (MASTER_SSL_CRL - 355))
                                | (1L << (MASTER_SSL_CRLPATH - 355))
                                | (1L << (MASTER_SSL_KEY - 355))
                                | (1L << (MASTER_TLS_VERSION - 355))
                                | (1L << (MASTER_USER - 355))
                                | (1L << (MAX_CONNECTIONS_PER_HOUR - 355))
                                | (1L << (MAX_QUERIES_PER_HOUR - 355))
                                | (1L << (MAX_ROWS - 355))
                                | (1L << (MAX_SIZE - 355))
                                | (1L << (MAX_UPDATES_PER_HOUR - 355))
                                | (1L << (MAX_USER_CONNECTIONS - 355))
                                | (1L << (MEDIUM - 355))
                                | (1L << (MERGE - 355))
                                | (1L << (MID - 355))
                                | (1L << (MIGRATE - 355))
                                | (1L << (MIN_ROWS - 355))
                                | (1L << (MODIFY - 355))
                                | (1L << (MUTEX - 355))
                                | (1L << (MYSQL - 355))
                                | (1L << (NAME - 355))
                                | (1L << (NAMES - 355))
                                | (1L << (NCHAR - 355))
                                | (1L << (NEVER - 355))
                                | (1L << (NO - 355))
                                | (1L << (NODEGROUP - 355))
                                | (1L << (NONE - 355))
                                | (1L << (OFFLINE - 355))
                                | (1L << (OFFSET - 355))))
                        != 0)
                || ((((_la - 419)) & ~0x3f) == 0
                    && ((1L << (_la - 419))
                            & ((1L << (OJ - 419))
                                | (1L << (OLD_PASSWORD - 419))
                                | (1L << (ONE - 419))
                                | (1L << (ONLINE - 419))
                                | (1L << (ONLY - 419))
                                | (1L << (OPTIMIZER_COSTS - 419))
                                | (1L << (OPTIONS - 419))
                                | (1L << (OWNER - 419))
                                | (1L << (PACK_KEYS - 419))
                                | (1L << (PAGE - 419))
                                | (1L << (PARSER - 419))
                                | (1L << (PARTIAL - 419))
                                | (1L << (PARTITIONING - 419))
                                | (1L << (PARTITIONS - 419))
                                | (1L << (PASSWORD - 419))
                                | (1L << (PHASE - 419))
                                | (1L << (PLUGIN_DIR - 419))
                                | (1L << (PLUGINS - 419))
                                | (1L << (PORT - 419))
                                | (1L << (PRECEDES - 419))
                                | (1L << (PREPARE - 419))
                                | (1L << (PRESERVE - 419))
                                | (1L << (PREV - 419))
                                | (1L << (PROCESSLIST - 419))
                                | (1L << (PROFILE - 419))
                                | (1L << (PROFILES - 419))
                                | (1L << (PROXY - 419))
                                | (1L << (QUERY - 419))
                                | (1L << (QUICK - 419))
                                | (1L << (REBUILD - 419))
                                | (1L << (RECOVER - 419))
                                | (1L << (REDO_BUFFER_SIZE - 419))
                                | (1L << (REDUNDANT - 419))
                                | (1L << (RELAY_LOG_FILE - 419))
                                | (1L << (RELAY_LOG_POS - 419))
                                | (1L << (RELAYLOG - 419))
                                | (1L << (REMOVE - 419))
                                | (1L << (REORGANIZE - 419))
                                | (1L << (REPAIR - 419))
                                | (1L << (REPLICATE_DO_DB - 419))
                                | (1L << (REPLICATE_DO_TABLE - 419))
                                | (1L << (REPLICATE_IGNORE_DB - 419))
                                | (1L << (REPLICATE_IGNORE_TABLE - 419))
                                | (1L << (REPLICATE_REWRITE_DB - 419))
                                | (1L << (REPLICATE_WILD_DO_TABLE - 419))
                                | (1L << (REPLICATE_WILD_IGNORE_TABLE - 419))
                                | (1L << (REPLICATION - 419))
                                | (1L << (RESUME - 419))
                                | (1L << (RETURNS - 419))
                                | (1L << (ROLLBACK - 419))
                                | (1L << (ROLLUP - 419))
                                | (1L << (ROTATE - 419))
                                | (1L << (ROW - 419))
                                | (1L << (ROWS - 419))
                                | (1L << (ROW_FORMAT - 419))
                                | (1L << (SAVEPOINT - 419))
                                | (1L << (SCHEDULE - 419))
                                | (1L << (SECURITY - 419))
                                | (1L << (SERVER - 419))
                                | (1L << (SESSION - 419))))
                        != 0)
                || ((((_la - 483)) & ~0x3f) == 0
                    && ((1L << (_la - 483))
                            & ((1L << (SHARE - 483))
                                | (1L << (SHARED - 483))
                                | (1L << (SIGNED - 483))
                                | (1L << (SIMPLE - 483))
                                | (1L << (SLAVE - 483))
                                | (1L << (SNAPSHOT - 483))
                                | (1L << (SOCKET - 483))
                                | (1L << (SOME - 483))
                                | (1L << (SOUNDS - 483))
                                | (1L << (SOURCE - 483))
                                | (1L << (SQL_AFTER_GTIDS - 483))
                                | (1L << (SQL_AFTER_MTS_GAPS - 483))
                                | (1L << (SQL_BEFORE_GTIDS - 483))
                                | (1L << (SQL_BUFFER_RESULT - 483))
                                | (1L << (SQL_CACHE - 483))
                                | (1L << (SQL_NO_CACHE - 483))
                                | (1L << (SQL_THREAD - 483))
                                | (1L << (START - 483))
                                | (1L << (STARTS - 483))
                                | (1L << (STATS_AUTO_RECALC - 483))
                                | (1L << (STATS_PERSISTENT - 483))
                                | (1L << (STATS_SAMPLE_PAGES - 483))
                                | (1L << (STATUS - 483))
                                | (1L << (STOP - 483))
                                | (1L << (STORAGE - 483))
                                | (1L << (STRING - 483))
                                | (1L << (SUBJECT - 483))
                                | (1L << (SUBPARTITION - 483))
                                | (1L << (SUBPARTITIONS - 483))
                                | (1L << (SUSPEND - 483))
                                | (1L << (SWAPS - 483))
                                | (1L << (SWITCHES - 483))
                                | (1L << (TABLESPACE - 483))
                                | (1L << (TEMPORARY - 483))
                                | (1L << (TEMPTABLE - 483))
                                | (1L << (THAN - 483))
                                | (1L << (TRANSACTION - 483))
                                | (1L << (TRUNCATE - 483))
                                | (1L << (UNDEFINED - 483))
                                | (1L << (UNDOFILE - 483))
                                | (1L << (UNDO_BUFFER_SIZE - 483))
                                | (1L << (UNKNOWN - 483))
                                | (1L << (UPGRADE - 483))
                                | (1L << (USER - 483))
                                | (1L << (VALIDATION - 483))
                                | (1L << (VALUE - 483))
                                | (1L << (VARIABLES - 483))
                                | (1L << (VIEW - 483))
                                | (1L << (WAIT - 483))
                                | (1L << (WARNINGS - 483))
                                | (1L << (WITHOUT - 483))
                                | (1L << (WORK - 483))
                                | (1L << (WRAPPER - 483))
                                | (1L << (X509 - 483))))
                        != 0)
                || ((((_la - 547)) & ~0x3f) == 0
                    && ((1L << (_la - 547))
                            & ((1L << (XA - 547))
                                | (1L << (XML - 547))
                                | (1L << (QUARTER - 547))
                                | (1L << (MONTH - 547))
                                | (1L << (DAY - 547))
                                | (1L << (HOUR - 547))
                                | (1L << (MINUTE - 547))
                                | (1L << (WEEK - 547))
                                | (1L << (SECOND - 547))
                                | (1L << (MICROSECOND - 547))
                                | (1L << (ARMSCII8 - 547))
                                | (1L << (ASCII - 547))
                                | (1L << (BIG5 - 547))
                                | (1L << (CP1250 - 547))
                                | (1L << (CP1251 - 547))
                                | (1L << (CP1256 - 547))
                                | (1L << (CP1257 - 547))
                                | (1L << (CP850 - 547))
                                | (1L << (CP852 - 547))
                                | (1L << (CP866 - 547))
                                | (1L << (CP932 - 547))
                                | (1L << (DEC8 - 547))
                                | (1L << (EUCJPMS - 547))
                                | (1L << (EUCKR - 547))
                                | (1L << (GB2312 - 547))
                                | (1L << (GBK - 547))
                                | (1L << (GEOSTD8 - 547))
                                | (1L << (GREEK - 547))
                                | (1L << (HEBREW - 547))
                                | (1L << (HP8 - 547))
                                | (1L << (KEYBCS2 - 547))
                                | (1L << (KOI8R - 547))
                                | (1L << (KOI8U - 547))
                                | (1L << (LATIN1 - 547))
                                | (1L << (LATIN2 - 547))
                                | (1L << (LATIN5 - 547))
                                | (1L << (LATIN7 - 547))
                                | (1L << (MACCE - 547))
                                | (1L << (MACROMAN - 547))
                                | (1L << (SJIS - 547))
                                | (1L << (SWE7 - 547))
                                | (1L << (TIS620 - 547))
                                | (1L << (UCS2 - 547))
                                | (1L << (UJIS - 547))
                                | (1L << (UTF16 - 547))
                                | (1L << (UTF16LE - 547))
                                | (1L << (UTF32 - 547))
                                | (1L << (UTF8 - 547))
                                | (1L << (UTF8MB3 - 547))
                                | (1L << (UTF8MB4 - 547))))
                        != 0)
                || ((((_la - 616)) & ~0x3f) == 0
                    && ((1L << (_la - 616))
                            & ((1L << (MEMORY - 616))
                                | (1L << (GEOMETRYCOLLECTION - 616))
                                | (1L << (LINESTRING - 616))
                                | (1L << (MULTILINESTRING - 616))
                                | (1L << (MULTIPOINT - 616))
                                | (1L << (MULTIPOLYGON - 616))
                                | (1L << (POINT - 616))
                                | (1L << (POLYGON - 616))
                                | (1L << (ABS - 616))
                                | (1L << (ACOS - 616))
                                | (1L << (ADDDATE - 616))
                                | (1L << (ADDTIME - 616))
                                | (1L << (AES_DECRYPT - 616))
                                | (1L << (AES_ENCRYPT - 616))
                                | (1L << (AREA - 616))
                                | (1L << (ASBINARY - 616))
                                | (1L << (ASIN - 616))
                                | (1L << (ASTEXT - 616))
                                | (1L << (ASWKB - 616))
                                | (1L << (ASWKT - 616))
                                | (1L << (ASYMMETRIC_DECRYPT - 616))
                                | (1L << (ASYMMETRIC_DERIVE - 616))
                                | (1L << (ASYMMETRIC_ENCRYPT - 616))
                                | (1L << (ASYMMETRIC_SIGN - 616))
                                | (1L << (ASYMMETRIC_VERIFY - 616))
                                | (1L << (ATAN - 616))
                                | (1L << (ATAN2 - 616))
                                | (1L << (BENCHMARK - 616))
                                | (1L << (BIN - 616))
                                | (1L << (BIT_COUNT - 616))
                                | (1L << (BIT_LENGTH - 616))
                                | (1L << (BUFFER - 616))
                                | (1L << (CEIL - 616))
                                | (1L << (CEILING - 616))
                                | (1L << (CENTROID - 616))
                                | (1L << (CHARACTER_LENGTH - 616))
                                | (1L << (CHARSET - 616))
                                | (1L << (CHAR_LENGTH - 616))
                                | (1L << (COERCIBILITY - 616))
                                | (1L << (COLLATION - 616))
                                | (1L << (COMPRESS - 616))
                                | (1L << (CONCAT - 616))
                                | (1L << (CONCAT_WS - 616))
                                | (1L << (CONNECTION_ID - 616))
                                | (1L << (CONV - 616))
                                | (1L << (CONVERT_TZ - 616))
                                | (1L << (COS - 616))
                                | (1L << (COT - 616))
                                | (1L << (CRC32 - 616))
                                | (1L << (CREATE_ASYMMETRIC_PRIV_KEY - 616))
                                | (1L << (CREATE_ASYMMETRIC_PUB_KEY - 616))
                                | (1L << (CREATE_DH_PARAMETERS - 616))
                                | (1L << (CREATE_DIGEST - 616))
                                | (1L << (CROSSES - 616))
                                | (1L << (DATEDIFF - 616))))
                        != 0)
                || ((((_la - 680)) & ~0x3f) == 0
                    && ((1L << (_la - 680))
                            & ((1L << (DATE_FORMAT - 680))
                                | (1L << (DAYNAME - 680))
                                | (1L << (DAYOFMONTH - 680))
                                | (1L << (DAYOFWEEK - 680))
                                | (1L << (DAYOFYEAR - 680))
                                | (1L << (DECODE - 680))
                                | (1L << (DEGREES - 680))
                                | (1L << (DES_DECRYPT - 680))
                                | (1L << (DES_ENCRYPT - 680))
                                | (1L << (DIMENSION - 680))
                                | (1L << (DISJOINT - 680))
                                | (1L << (ELT - 680))
                                | (1L << (ENCODE - 680))
                                | (1L << (ENCRYPT - 680))
                                | (1L << (ENDPOINT - 680))
                                | (1L << (ENVELOPE - 680))
                                | (1L << (EQUALS - 680))
                                | (1L << (EXP - 680))
                                | (1L << (EXPORT_SET - 680))
                                | (1L << (EXTERIORRING - 680))
                                | (1L << (EXTRACTVALUE - 680))
                                | (1L << (FIELD - 680))
                                | (1L << (FIND_IN_SET - 680))
                                | (1L << (FLOOR - 680))
                                | (1L << (FORMAT - 680))
                                | (1L << (FOUND_ROWS - 680))
                                | (1L << (FROM_BASE64 - 680))
                                | (1L << (FROM_DAYS - 680))
                                | (1L << (FROM_UNIXTIME - 680))
                                | (1L << (GEOMCOLLFROMTEXT - 680))
                                | (1L << (GEOMCOLLFROMWKB - 680))
                                | (1L << (GEOMETRYCOLLECTIONFROMTEXT - 680))
                                | (1L << (GEOMETRYCOLLECTIONFROMWKB - 680))
                                | (1L << (GEOMETRYFROMTEXT - 680))
                                | (1L << (GEOMETRYFROMWKB - 680))
                                | (1L << (GEOMETRYN - 680))
                                | (1L << (GEOMETRYTYPE - 680))
                                | (1L << (GEOMFROMTEXT - 680))
                                | (1L << (GEOMFROMWKB - 680))
                                | (1L << (GET_FORMAT - 680))
                                | (1L << (GET_LOCK - 680))
                                | (1L << (GLENGTH - 680))
                                | (1L << (GREATEST - 680))
                                | (1L << (GTID_SUBSET - 680))
                                | (1L << (GTID_SUBTRACT - 680))
                                | (1L << (HEX - 680))
                                | (1L << (IFNULL - 680))
                                | (1L << (INET6_ATON - 680))
                                | (1L << (INET6_NTOA - 680))
                                | (1L << (INET_ATON - 680))
                                | (1L << (INET_NTOA - 680))
                                | (1L << (INSTR - 680))
                                | (1L << (INTERIORRINGN - 680))
                                | (1L << (INTERSECTS - 680))
                                | (1L << (ISCLOSED - 680))
                                | (1L << (ISEMPTY - 680))
                                | (1L << (ISNULL - 680))
                                | (1L << (ISSIMPLE - 680))
                                | (1L << (IS_FREE_LOCK - 680))
                                | (1L << (IS_IPV4 - 680))
                                | (1L << (IS_IPV4_COMPAT - 680))
                                | (1L << (IS_IPV4_MAPPED - 680))
                                | (1L << (IS_IPV6 - 680))
                                | (1L << (IS_USED_LOCK - 680))))
                        != 0)
                || ((((_la - 744)) & ~0x3f) == 0
                    && ((1L << (_la - 744))
                            & ((1L << (LAST_INSERT_ID - 744))
                                | (1L << (LCASE - 744))
                                | (1L << (LEAST - 744))
                                | (1L << (LENGTH - 744))
                                | (1L << (LINEFROMTEXT - 744))
                                | (1L << (LINEFROMWKB - 744))
                                | (1L << (LINESTRINGFROMTEXT - 744))
                                | (1L << (LINESTRINGFROMWKB - 744))
                                | (1L << (LN - 744))
                                | (1L << (LOAD_FILE - 744))
                                | (1L << (LOCATE - 744))
                                | (1L << (LOG - 744))
                                | (1L << (LOG10 - 744))
                                | (1L << (LOG2 - 744))
                                | (1L << (LOWER - 744))
                                | (1L << (LPAD - 744))
                                | (1L << (LTRIM - 744))
                                | (1L << (MAKEDATE - 744))
                                | (1L << (MAKETIME - 744))
                                | (1L << (MAKE_SET - 744))
                                | (1L << (MASTER_POS_WAIT - 744))
                                | (1L << (MBRCONTAINS - 744))
                                | (1L << (MBRDISJOINT - 744))
                                | (1L << (MBREQUAL - 744))
                                | (1L << (MBRINTERSECTS - 744))
                                | (1L << (MBROVERLAPS - 744))
                                | (1L << (MBRTOUCHES - 744))
                                | (1L << (MBRWITHIN - 744))
                                | (1L << (MD5 - 744))
                                | (1L << (MLINEFROMTEXT - 744))
                                | (1L << (MLINEFROMWKB - 744))
                                | (1L << (MONTHNAME - 744))
                                | (1L << (MPOINTFROMTEXT - 744))
                                | (1L << (MPOINTFROMWKB - 744))
                                | (1L << (MPOLYFROMTEXT - 744))
                                | (1L << (MPOLYFROMWKB - 744))
                                | (1L << (MULTILINESTRINGFROMTEXT - 744))
                                | (1L << (MULTILINESTRINGFROMWKB - 744))
                                | (1L << (MULTIPOINTFROMTEXT - 744))
                                | (1L << (MULTIPOINTFROMWKB - 744))
                                | (1L << (MULTIPOLYGONFROMTEXT - 744))
                                | (1L << (MULTIPOLYGONFROMWKB - 744))
                                | (1L << (NAME_CONST - 744))
                                | (1L << (NULLIF - 744))
                                | (1L << (NUMGEOMETRIES - 744))
                                | (1L << (NUMINTERIORRINGS - 744))
                                | (1L << (NUMPOINTS - 744))
                                | (1L << (OCT - 744))
                                | (1L << (OCTET_LENGTH - 744))
                                | (1L << (ORD - 744))
                                | (1L << (OVERLAPS - 744))
                                | (1L << (PERIOD_ADD - 744))
                                | (1L << (PERIOD_DIFF - 744))
                                | (1L << (PI - 744))
                                | (1L << (POINTFROMTEXT - 744))
                                | (1L << (POINTFROMWKB - 744))
                                | (1L << (POINTN - 744))
                                | (1L << (POLYFROMTEXT - 744))
                                | (1L << (POLYFROMWKB - 744))
                                | (1L << (POLYGONFROMTEXT - 744))
                                | (1L << (POLYGONFROMWKB - 744))
                                | (1L << (POW - 744))
                                | (1L << (POWER - 744))
                                | (1L << (QUOTE - 744))))
                        != 0)
                || ((((_la - 808)) & ~0x3f) == 0
                    && ((1L << (_la - 808))
                            & ((1L << (RADIANS - 808))
                                | (1L << (RAND - 808))
                                | (1L << (RANDOM_BYTES - 808))
                                | (1L << (RELEASE_LOCK - 808))
                                | (1L << (REVERSE - 808))
                                | (1L << (ROUND - 808))
                                | (1L << (ROW_COUNT - 808))
                                | (1L << (RPAD - 808))
                                | (1L << (RTRIM - 808))
                                | (1L << (SEC_TO_TIME - 808))
                                | (1L << (SESSION_USER - 808))
                                | (1L << (SHA - 808))
                                | (1L << (SHA1 - 808))
                                | (1L << (SHA2 - 808))
                                | (1L << (SIGN - 808))
                                | (1L << (SIN - 808))
                                | (1L << (SLEEP - 808))
                                | (1L << (SOUNDEX - 808))
                                | (1L << (SQL_THREAD_WAIT_AFTER_GTIDS - 808))
                                | (1L << (SQRT - 808))
                                | (1L << (SRID - 808))
                                | (1L << (STARTPOINT - 808))
                                | (1L << (STRCMP - 808))
                                | (1L << (STR_TO_DATE - 808))
                                | (1L << (ST_AREA - 808))
                                | (1L << (ST_ASBINARY - 808))
                                | (1L << (ST_ASTEXT - 808))
                                | (1L << (ST_ASWKB - 808))
                                | (1L << (ST_ASWKT - 808))
                                | (1L << (ST_BUFFER - 808))
                                | (1L << (ST_CENTROID - 808))
                                | (1L << (ST_CONTAINS - 808))
                                | (1L << (ST_CROSSES - 808))
                                | (1L << (ST_DIFFERENCE - 808))
                                | (1L << (ST_DIMENSION - 808))
                                | (1L << (ST_DISJOINT - 808))
                                | (1L << (ST_DISTANCE - 808))
                                | (1L << (ST_ENDPOINT - 808))
                                | (1L << (ST_ENVELOPE - 808))
                                | (1L << (ST_EQUALS - 808))
                                | (1L << (ST_EXTERIORRING - 808))
                                | (1L << (ST_GEOMCOLLFROMTEXT - 808))
                                | (1L << (ST_GEOMCOLLFROMTXT - 808))
                                | (1L << (ST_GEOMCOLLFROMWKB - 808))
                                | (1L << (ST_GEOMETRYCOLLECTIONFROMTEXT - 808))
                                | (1L << (ST_GEOMETRYCOLLECTIONFROMWKB - 808))
                                | (1L << (ST_GEOMETRYFROMTEXT - 808))
                                | (1L << (ST_GEOMETRYFROMWKB - 808))
                                | (1L << (ST_GEOMETRYN - 808))
                                | (1L << (ST_GEOMETRYTYPE - 808))
                                | (1L << (ST_GEOMFROMTEXT - 808))
                                | (1L << (ST_GEOMFROMWKB - 808))
                                | (1L << (ST_INTERIORRINGN - 808))
                                | (1L << (ST_INTERSECTION - 808))
                                | (1L << (ST_INTERSECTS - 808))
                                | (1L << (ST_ISCLOSED - 808))
                                | (1L << (ST_ISEMPTY - 808))
                                | (1L << (ST_ISSIMPLE - 808))
                                | (1L << (ST_LINEFROMTEXT - 808))
                                | (1L << (ST_LINEFROMWKB - 808))
                                | (1L << (ST_LINESTRINGFROMTEXT - 808))
                                | (1L << (ST_LINESTRINGFROMWKB - 808))
                                | (1L << (ST_NUMGEOMETRIES - 808))
                                | (1L << (ST_NUMINTERIORRING - 808))))
                        != 0)
                || ((((_la - 872)) & ~0x3f) == 0
                    && ((1L << (_la - 872))
                            & ((1L << (ST_NUMINTERIORRINGS - 872))
                                | (1L << (ST_NUMPOINTS - 872))
                                | (1L << (ST_OVERLAPS - 872))
                                | (1L << (ST_POINTFROMTEXT - 872))
                                | (1L << (ST_POINTFROMWKB - 872))
                                | (1L << (ST_POINTN - 872))
                                | (1L << (ST_POLYFROMTEXT - 872))
                                | (1L << (ST_POLYFROMWKB - 872))
                                | (1L << (ST_POLYGONFROMTEXT - 872))
                                | (1L << (ST_POLYGONFROMWKB - 872))
                                | (1L << (ST_SRID - 872))
                                | (1L << (ST_STARTPOINT - 872))
                                | (1L << (ST_SYMDIFFERENCE - 872))
                                | (1L << (ST_TOUCHES - 872))
                                | (1L << (ST_UNION - 872))
                                | (1L << (ST_WITHIN - 872))
                                | (1L << (ST_X - 872))
                                | (1L << (ST_Y - 872))
                                | (1L << (SUBDATE - 872))
                                | (1L << (SUBSTRING_INDEX - 872))
                                | (1L << (SUBTIME - 872))
                                | (1L << (SYSTEM_USER - 872))
                                | (1L << (TAN - 872))
                                | (1L << (TIMEDIFF - 872))
                                | (1L << (TIMESTAMPADD - 872))
                                | (1L << (TIMESTAMPDIFF - 872))
                                | (1L << (TIME_FORMAT - 872))
                                | (1L << (TIME_TO_SEC - 872))
                                | (1L << (TOUCHES - 872))
                                | (1L << (TO_BASE64 - 872))
                                | (1L << (TO_DAYS - 872))
                                | (1L << (TO_SECONDS - 872))
                                | (1L << (UCASE - 872))
                                | (1L << (UNCOMPRESS - 872))
                                | (1L << (UNCOMPRESSED_LENGTH - 872))
                                | (1L << (UNHEX - 872))
                                | (1L << (UNIX_TIMESTAMP - 872))
                                | (1L << (UPDATEXML - 872))
                                | (1L << (UPPER - 872))
                                | (1L << (UUID - 872))
                                | (1L << (UUID_SHORT - 872))
                                | (1L << (VALIDATE_PASSWORD_STRENGTH - 872))
                                | (1L << (VERSION - 872))
                                | (1L << (WAIT_UNTIL_SQL_THREAD_AFTER_GTIDS - 872))
                                | (1L << (WEEKDAY - 872))
                                | (1L << (WEEKOFYEAR - 872))
                                | (1L << (WEIGHT_STRING - 872))
                                | (1L << (WITHIN - 872))
                                | (1L << (YEARWEEK - 872))
                                | (1L << (Y_FUNCTION - 872))
                                | (1L << (X_FUNCTION - 872))
                                | (1L << (PLUS - 872))))
                        != 0)
                || ((((_la - 937)) & ~0x3f) == 0
                    && ((1L << (_la - 937))
                            & ((1L << (MINUS - 937))
                                | (1L << (EXCLAMATION_SYMBOL - 937))
                                | (1L << (BIT_NOT_OP - 937))
                                | (1L << (LR_BRACKET - 937))
                                | (1L << (ZERO_DECIMAL - 937))
                                | (1L << (ONE_DECIMAL - 937))
                                | (1L << (TWO_DECIMAL - 937))
                                | (1L << (CHARSET_REVERSE_QOUTE_STRING - 937))
                                | (1L << (STRING_LITERAL - 937))
                                | (1L << (DECIMAL_LITERAL - 937))
                                | (1L << (HEXADECIMAL_LITERAL - 937))
                                | (1L << (REAL_LITERAL - 937))
                                | (1L << (NULL_SPEC_LITERAL - 937))
                                | (1L << (BIT_STRING - 937))
                                | (1L << (STRING_CHARSET_NAME - 937))
                                | (1L << (ID - 937))
                                | (1L << (REVERSE_QUOTE_ID - 937))
                                | (1L << (LOCAL_ID - 937))))
                        != 0)) {
              {
                setState(237);
                functionArgs();
              }
            }

            setState(240);
            match(RR_BRACKET);
          }
          break;
      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  public static class SpecificFunctionContext extends ParserRuleContext {
    public SpecificFunctionContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_specificFunction;
    }

    public SpecificFunctionContext() {}

    public void copyFrom(SpecificFunctionContext ctx) {
      super.copyFrom(ctx);
    }
  }

  public static class PositionFunctionCallContext extends SpecificFunctionContext {
    public StringLiteralContext positionString;
    public ExpressionContext positionExpression;
    public StringLiteralContext inString;
    public ExpressionContext inExpression;

    public TerminalNode POSITION() {
      return getToken(MySqlParser.POSITION, 0);
    }

    public TerminalNode IN() {
      return getToken(MySqlParser.IN, 0);
    }

    public List<StringLiteralContext> stringLiteral() {
      return getRuleContexts(StringLiteralContext.class);
    }

    public StringLiteralContext stringLiteral(int i) {
      return getRuleContext(StringLiteralContext.class, i);
    }

    public List<ExpressionContext> expression() {
      return getRuleContexts(ExpressionContext.class);
    }

    public ExpressionContext expression(int i) {
      return getRuleContext(ExpressionContext.class, i);
    }

    public PositionFunctionCallContext(SpecificFunctionContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public void enterRule(ParseTreeListener listener) {
      if (listener instanceof MySqlParserListener)
        ((MySqlParserListener) listener).enterPositionFunctionCall(this);
    }

    @Override
    public void exitRule(ParseTreeListener listener) {
      if (listener instanceof MySqlParserListener)
        ((MySqlParserListener) listener).exitPositionFunctionCall(this);
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof MySqlParserVisitor)
        return ((MySqlParserVisitor<? extends T>) visitor).visitPositionFunctionCall(this);
      else return visitor.visitChildren(this);
    }
  }

  public static class TrimFunctionCallContext extends SpecificFunctionContext {
    public Token positioinForm;
    public StringLiteralContext sourceString;
    public ExpressionContext sourceExpression;
    public StringLiteralContext fromString;
    public ExpressionContext fromExpression;

    public TerminalNode TRIM() {
      return getToken(MySqlParser.TRIM, 0);
    }

    public TerminalNode FROM() {
      return getToken(MySqlParser.FROM, 0);
    }

    public TerminalNode BOTH() {
      return getToken(MySqlParser.BOTH, 0);
    }

    public TerminalNode LEADING() {
      return getToken(MySqlParser.LEADING, 0);
    }

    public TerminalNode TRAILING() {
      return getToken(MySqlParser.TRAILING, 0);
    }

    public List<StringLiteralContext> stringLiteral() {
      return getRuleContexts(StringLiteralContext.class);
    }

    public StringLiteralContext stringLiteral(int i) {
      return getRuleContext(StringLiteralContext.class, i);
    }

    public List<ExpressionContext> expression() {
      return getRuleContexts(ExpressionContext.class);
    }

    public ExpressionContext expression(int i) {
      return getRuleContext(ExpressionContext.class, i);
    }

    public TrimFunctionCallContext(SpecificFunctionContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public void enterRule(ParseTreeListener listener) {
      if (listener instanceof MySqlParserListener)
        ((MySqlParserListener) listener).enterTrimFunctionCall(this);
    }

    @Override
    public void exitRule(ParseTreeListener listener) {
      if (listener instanceof MySqlParserListener)
        ((MySqlParserListener) listener).exitTrimFunctionCall(this);
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof MySqlParserVisitor)
        return ((MySqlParserVisitor<? extends T>) visitor).visitTrimFunctionCall(this);
      else return visitor.visitChildren(this);
    }
  }

  public static class SimpleFunctionCallContext extends SpecificFunctionContext {
    public TerminalNode CURRENT_DATE() {
      return getToken(MySqlParser.CURRENT_DATE, 0);
    }

    public TerminalNode CURRENT_TIME() {
      return getToken(MySqlParser.CURRENT_TIME, 0);
    }

    public TerminalNode CURRENT_TIMESTAMP() {
      return getToken(MySqlParser.CURRENT_TIMESTAMP, 0);
    }

    public TerminalNode CURRENT_USER() {
      return getToken(MySqlParser.CURRENT_USER, 0);
    }

    public TerminalNode LOCALTIME() {
      return getToken(MySqlParser.LOCALTIME, 0);
    }

    public SimpleFunctionCallContext(SpecificFunctionContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public void enterRule(ParseTreeListener listener) {
      if (listener instanceof MySqlParserListener)
        ((MySqlParserListener) listener).enterSimpleFunctionCall(this);
    }

    @Override
    public void exitRule(ParseTreeListener listener) {
      if (listener instanceof MySqlParserListener)
        ((MySqlParserListener) listener).exitSimpleFunctionCall(this);
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof MySqlParserVisitor)
        return ((MySqlParserVisitor<? extends T>) visitor).visitSimpleFunctionCall(this);
      else return visitor.visitChildren(this);
    }
  }

  public static class CharFunctionCallContext extends SpecificFunctionContext {
    public TerminalNode CHAR() {
      return getToken(MySqlParser.CHAR, 0);
    }

    public FunctionArgsContext functionArgs() {
      return getRuleContext(FunctionArgsContext.class, 0);
    }

    public TerminalNode USING() {
      return getToken(MySqlParser.USING, 0);
    }

    public CharsetNameContext charsetName() {
      return getRuleContext(CharsetNameContext.class, 0);
    }

    public CharFunctionCallContext(SpecificFunctionContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public void enterRule(ParseTreeListener listener) {
      if (listener instanceof MySqlParserListener)
        ((MySqlParserListener) listener).enterCharFunctionCall(this);
    }

    @Override
    public void exitRule(ParseTreeListener listener) {
      if (listener instanceof MySqlParserListener)
        ((MySqlParserListener) listener).exitCharFunctionCall(this);
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof MySqlParserVisitor)
        return ((MySqlParserVisitor<? extends T>) visitor).visitCharFunctionCall(this);
      else return visitor.visitChildren(this);
    }
  }

  public static class WeightFunctionCallContext extends SpecificFunctionContext {
    public Token stringFormat;

    public TerminalNode WEIGHT_STRING() {
      return getToken(MySqlParser.WEIGHT_STRING, 0);
    }

    public StringLiteralContext stringLiteral() {
      return getRuleContext(StringLiteralContext.class, 0);
    }

    public ExpressionContext expression() {
      return getRuleContext(ExpressionContext.class, 0);
    }

    public TerminalNode AS() {
      return getToken(MySqlParser.AS, 0);
    }

    public DecimalLiteralContext decimalLiteral() {
      return getRuleContext(DecimalLiteralContext.class, 0);
    }

    public LevelsInWeightStringContext levelsInWeightString() {
      return getRuleContext(LevelsInWeightStringContext.class, 0);
    }

    public TerminalNode CHAR() {
      return getToken(MySqlParser.CHAR, 0);
    }

    public TerminalNode BINARY() {
      return getToken(MySqlParser.BINARY, 0);
    }

    public WeightFunctionCallContext(SpecificFunctionContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public void enterRule(ParseTreeListener listener) {
      if (listener instanceof MySqlParserListener)
        ((MySqlParserListener) listener).enterWeightFunctionCall(this);
    }

    @Override
    public void exitRule(ParseTreeListener listener) {
      if (listener instanceof MySqlParserListener)
        ((MySqlParserListener) listener).exitWeightFunctionCall(this);
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof MySqlParserVisitor)
        return ((MySqlParserVisitor<? extends T>) visitor).visitWeightFunctionCall(this);
      else return visitor.visitChildren(this);
    }
  }

  public static class GetFormatFunctionCallContext extends SpecificFunctionContext {
    public Token datetimeFormat;

    public TerminalNode GET_FORMAT() {
      return getToken(MySqlParser.GET_FORMAT, 0);
    }

    public StringLiteralContext stringLiteral() {
      return getRuleContext(StringLiteralContext.class, 0);
    }

    public TerminalNode DATE() {
      return getToken(MySqlParser.DATE, 0);
    }

    public TerminalNode TIME() {
      return getToken(MySqlParser.TIME, 0);
    }

    public TerminalNode DATETIME() {
      return getToken(MySqlParser.DATETIME, 0);
    }

    public GetFormatFunctionCallContext(SpecificFunctionContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public void enterRule(ParseTreeListener listener) {
      if (listener instanceof MySqlParserListener)
        ((MySqlParserListener) listener).enterGetFormatFunctionCall(this);
    }

    @Override
    public void exitRule(ParseTreeListener listener) {
      if (listener instanceof MySqlParserListener)
        ((MySqlParserListener) listener).exitGetFormatFunctionCall(this);
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof MySqlParserVisitor)
        return ((MySqlParserVisitor<? extends T>) visitor).visitGetFormatFunctionCall(this);
      else return visitor.visitChildren(this);
    }
  }

  public static class CaseFunctionCallContext extends SpecificFunctionContext {
    public FunctionArgContext elseArg;

    public TerminalNode CASE() {
      return getToken(MySqlParser.CASE, 0);
    }

    public ExpressionContext expression() {
      return getRuleContext(ExpressionContext.class, 0);
    }

    public TerminalNode END() {
      return getToken(MySqlParser.END, 0);
    }

    public List<CaseFuncAlternativeContext> caseFuncAlternative() {
      return getRuleContexts(CaseFuncAlternativeContext.class);
    }

    public CaseFuncAlternativeContext caseFuncAlternative(int i) {
      return getRuleContext(CaseFuncAlternativeContext.class, i);
    }

    public TerminalNode ELSE() {
      return getToken(MySqlParser.ELSE, 0);
    }

    public FunctionArgContext functionArg() {
      return getRuleContext(FunctionArgContext.class, 0);
    }

    public CaseFunctionCallContext(SpecificFunctionContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public void enterRule(ParseTreeListener listener) {
      if (listener instanceof MySqlParserListener)
        ((MySqlParserListener) listener).enterCaseFunctionCall(this);
    }

    @Override
    public void exitRule(ParseTreeListener listener) {
      if (listener instanceof MySqlParserListener)
        ((MySqlParserListener) listener).exitCaseFunctionCall(this);
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof MySqlParserVisitor)
        return ((MySqlParserVisitor<? extends T>) visitor).visitCaseFunctionCall(this);
      else return visitor.visitChildren(this);
    }
  }

  public static class ExtractFunctionCallContext extends SpecificFunctionContext {
    public StringLiteralContext sourceString;
    public ExpressionContext sourceExpression;

    public TerminalNode EXTRACT() {
      return getToken(MySqlParser.EXTRACT, 0);
    }

    public IntervalTypeContext intervalType() {
      return getRuleContext(IntervalTypeContext.class, 0);
    }

    public TerminalNode FROM() {
      return getToken(MySqlParser.FROM, 0);
    }

    public StringLiteralContext stringLiteral() {
      return getRuleContext(StringLiteralContext.class, 0);
    }

    public ExpressionContext expression() {
      return getRuleContext(ExpressionContext.class, 0);
    }

    public ExtractFunctionCallContext(SpecificFunctionContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public void enterRule(ParseTreeListener listener) {
      if (listener instanceof MySqlParserListener)
        ((MySqlParserListener) listener).enterExtractFunctionCall(this);
    }

    @Override
    public void exitRule(ParseTreeListener listener) {
      if (listener instanceof MySqlParserListener)
        ((MySqlParserListener) listener).exitExtractFunctionCall(this);
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof MySqlParserVisitor)
        return ((MySqlParserVisitor<? extends T>) visitor).visitExtractFunctionCall(this);
      else return visitor.visitChildren(this);
    }
  }

  public static class DataTypeFunctionCallContext extends SpecificFunctionContext {
    public Token separator;

    public TerminalNode CONVERT() {
      return getToken(MySqlParser.CONVERT, 0);
    }

    public ExpressionContext expression() {
      return getRuleContext(ExpressionContext.class, 0);
    }

    public ConvertedDataTypeContext convertedDataType() {
      return getRuleContext(ConvertedDataTypeContext.class, 0);
    }

    public TerminalNode USING() {
      return getToken(MySqlParser.USING, 0);
    }

    public CharsetNameContext charsetName() {
      return getRuleContext(CharsetNameContext.class, 0);
    }

    public TerminalNode CAST() {
      return getToken(MySqlParser.CAST, 0);
    }

    public TerminalNode AS() {
      return getToken(MySqlParser.AS, 0);
    }

    public DataTypeFunctionCallContext(SpecificFunctionContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public void enterRule(ParseTreeListener listener) {
      if (listener instanceof MySqlParserListener)
        ((MySqlParserListener) listener).enterDataTypeFunctionCall(this);
    }

    @Override
    public void exitRule(ParseTreeListener listener) {
      if (listener instanceof MySqlParserListener)
        ((MySqlParserListener) listener).exitDataTypeFunctionCall(this);
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof MySqlParserVisitor)
        return ((MySqlParserVisitor<? extends T>) visitor).visitDataTypeFunctionCall(this);
      else return visitor.visitChildren(this);
    }
  }

  public static class ValuesFunctionCallContext extends SpecificFunctionContext {
    public TerminalNode VALUES() {
      return getToken(MySqlParser.VALUES, 0);
    }

    public FullColumnNameContext fullColumnName() {
      return getRuleContext(FullColumnNameContext.class, 0);
    }

    public ValuesFunctionCallContext(SpecificFunctionContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public void enterRule(ParseTreeListener listener) {
      if (listener instanceof MySqlParserListener)
        ((MySqlParserListener) listener).enterValuesFunctionCall(this);
    }

    @Override
    public void exitRule(ParseTreeListener listener) {
      if (listener instanceof MySqlParserListener)
        ((MySqlParserListener) listener).exitValuesFunctionCall(this);
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof MySqlParserVisitor)
        return ((MySqlParserVisitor<? extends T>) visitor).visitValuesFunctionCall(this);
      else return visitor.visitChildren(this);
    }
  }

  public static class SubstrFunctionCallContext extends SpecificFunctionContext {
    public StringLiteralContext sourceString;
    public ExpressionContext sourceExpression;
    public DecimalLiteralContext fromDecimal;
    public ExpressionContext fromExpression;
    public DecimalLiteralContext forDecimal;
    public ExpressionContext forExpression;

    public TerminalNode FROM() {
      return getToken(MySqlParser.FROM, 0);
    }

    public TerminalNode SUBSTR() {
      return getToken(MySqlParser.SUBSTR, 0);
    }

    public TerminalNode SUBSTRING() {
      return getToken(MySqlParser.SUBSTRING, 0);
    }

    public StringLiteralContext stringLiteral() {
      return getRuleContext(StringLiteralContext.class, 0);
    }

    public List<ExpressionContext> expression() {
      return getRuleContexts(ExpressionContext.class);
    }

    public ExpressionContext expression(int i) {
      return getRuleContext(ExpressionContext.class, i);
    }

    public List<DecimalLiteralContext> decimalLiteral() {
      return getRuleContexts(DecimalLiteralContext.class);
    }

    public DecimalLiteralContext decimalLiteral(int i) {
      return getRuleContext(DecimalLiteralContext.class, i);
    }

    public TerminalNode FOR() {
      return getToken(MySqlParser.FOR, 0);
    }

    public SubstrFunctionCallContext(SpecificFunctionContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public void enterRule(ParseTreeListener listener) {
      if (listener instanceof MySqlParserListener)
        ((MySqlParserListener) listener).enterSubstrFunctionCall(this);
    }

    @Override
    public void exitRule(ParseTreeListener listener) {
      if (listener instanceof MySqlParserListener)
        ((MySqlParserListener) listener).exitSubstrFunctionCall(this);
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof MySqlParserVisitor)
        return ((MySqlParserVisitor<? extends T>) visitor).visitSubstrFunctionCall(this);
      else return visitor.visitChildren(this);
    }
  }

  public final SpecificFunctionContext specificFunction() throws RecognitionException {
    SpecificFunctionContext _localctx = new SpecificFunctionContext(_ctx, getState());
    enterRule(_localctx, 40, RULE_specificFunction);
    int _la;
    try {
      setState(401);
      _errHandler.sync(this);
      switch (getInterpreter().adaptivePredict(_input, 47, _ctx)) {
        case 1:
          _localctx = new SimpleFunctionCallContext(_localctx);
          enterOuterAlt(_localctx, 1);
          {
            setState(244);
            _la = _input.LA(1);
            if (!(_la == CURRENT_USER
                || ((((_la - 227)) & ~0x3f) == 0
                    && ((1L << (_la - 227))
                            & ((1L << (CURRENT_DATE - 227))
                                | (1L << (CURRENT_TIME - 227))
                                | (1L << (CURRENT_TIMESTAMP - 227))
                                | (1L << (LOCALTIME - 227))))
                        != 0))) {
              _errHandler.recoverInline(this);
            } else {
              if (_input.LA(1) == Token.EOF) matchedEOF = true;
              _errHandler.reportMatch(this);
              consume();
            }
          }
          break;
        case 2:
          _localctx = new DataTypeFunctionCallContext(_localctx);
          enterOuterAlt(_localctx, 2);
          {
            setState(245);
            match(CONVERT);
            setState(246);
            match(LR_BRACKET);
            setState(247);
            expression(0);
            setState(248);
            ((DataTypeFunctionCallContext) _localctx).separator = match(COMMA);
            setState(249);
            convertedDataType();
            setState(250);
            match(RR_BRACKET);
          }
          break;
        case 3:
          _localctx = new DataTypeFunctionCallContext(_localctx);
          enterOuterAlt(_localctx, 3);
          {
            setState(252);
            match(CONVERT);
            setState(253);
            match(LR_BRACKET);
            setState(254);
            expression(0);
            setState(255);
            match(USING);
            setState(256);
            charsetName();
            setState(257);
            match(RR_BRACKET);
          }
          break;
        case 4:
          _localctx = new DataTypeFunctionCallContext(_localctx);
          enterOuterAlt(_localctx, 4);
          {
            setState(259);
            match(CAST);
            setState(260);
            match(LR_BRACKET);
            setState(261);
            expression(0);
            setState(262);
            match(AS);
            setState(263);
            convertedDataType();
            setState(264);
            match(RR_BRACKET);
          }
          break;
        case 5:
          _localctx = new ValuesFunctionCallContext(_localctx);
          enterOuterAlt(_localctx, 5);
          {
            setState(266);
            match(VALUES);
            setState(267);
            match(LR_BRACKET);
            setState(268);
            fullColumnName();
            setState(269);
            match(RR_BRACKET);
          }
          break;
        case 6:
          _localctx = new CaseFunctionCallContext(_localctx);
          enterOuterAlt(_localctx, 6);
          {
            setState(271);
            match(CASE);
            setState(272);
            expression(0);
            setState(274);
            _errHandler.sync(this);
            _la = _input.LA(1);
            do {
              {
                {
                  setState(273);
                  caseFuncAlternative();
                }
              }
              setState(276);
              _errHandler.sync(this);
              _la = _input.LA(1);
            } while (_la == WHEN);
            setState(280);
            _errHandler.sync(this);
            _la = _input.LA(1);
            if (_la == ELSE) {
              {
                setState(278);
                match(ELSE);
                setState(279);
                ((CaseFunctionCallContext) _localctx).elseArg = functionArg();
              }
            }

            setState(282);
            match(END);
          }
          break;
        case 7:
          _localctx = new CaseFunctionCallContext(_localctx);
          enterOuterAlt(_localctx, 7);
          {
            setState(284);
            match(CASE);
            setState(286);
            _errHandler.sync(this);
            _la = _input.LA(1);
            do {
              {
                {
                  setState(285);
                  caseFuncAlternative();
                }
              }
              setState(288);
              _errHandler.sync(this);
              _la = _input.LA(1);
            } while (_la == WHEN);
            setState(292);
            _errHandler.sync(this);
            _la = _input.LA(1);
            if (_la == ELSE) {
              {
                setState(290);
                match(ELSE);
                setState(291);
                ((CaseFunctionCallContext) _localctx).elseArg = functionArg();
              }
            }

            setState(294);
            match(END);
          }
          break;
        case 8:
          _localctx = new CharFunctionCallContext(_localctx);
          enterOuterAlt(_localctx, 8);
          {
            setState(296);
            match(CHAR);
            setState(297);
            match(LR_BRACKET);
            setState(298);
            functionArgs();
            setState(301);
            _errHandler.sync(this);
            _la = _input.LA(1);
            if (_la == USING) {
              {
                setState(299);
                match(USING);
                setState(300);
                charsetName();
              }
            }

            setState(303);
            match(RR_BRACKET);
          }
          break;
        case 9:
          _localctx = new PositionFunctionCallContext(_localctx);
          enterOuterAlt(_localctx, 9);
          {
            setState(305);
            match(POSITION);
            setState(306);
            match(LR_BRACKET);
            setState(309);
            _errHandler.sync(this);
            switch (getInterpreter().adaptivePredict(_input, 33, _ctx)) {
              case 1:
                {
                  setState(307);
                  ((PositionFunctionCallContext) _localctx).positionString = stringLiteral();
                }
                break;
              case 2:
                {
                  setState(308);
                  ((PositionFunctionCallContext) _localctx).positionExpression = expression(0);
                }
                break;
            }
            setState(311);
            match(IN);
            setState(314);
            _errHandler.sync(this);
            switch (getInterpreter().adaptivePredict(_input, 34, _ctx)) {
              case 1:
                {
                  setState(312);
                  ((PositionFunctionCallContext) _localctx).inString = stringLiteral();
                }
                break;
              case 2:
                {
                  setState(313);
                  ((PositionFunctionCallContext) _localctx).inExpression = expression(0);
                }
                break;
            }
            setState(316);
            match(RR_BRACKET);
          }
          break;
        case 10:
          _localctx = new SubstrFunctionCallContext(_localctx);
          enterOuterAlt(_localctx, 10);
          {
            setState(318);
            _la = _input.LA(1);
            if (!(_la == SUBSTR || _la == SUBSTRING)) {
              _errHandler.recoverInline(this);
            } else {
              if (_input.LA(1) == Token.EOF) matchedEOF = true;
              _errHandler.reportMatch(this);
              consume();
            }
            setState(319);
            match(LR_BRACKET);
            setState(322);
            _errHandler.sync(this);
            switch (getInterpreter().adaptivePredict(_input, 35, _ctx)) {
              case 1:
                {
                  setState(320);
                  ((SubstrFunctionCallContext) _localctx).sourceString = stringLiteral();
                }
                break;
              case 2:
                {
                  setState(321);
                  ((SubstrFunctionCallContext) _localctx).sourceExpression = expression(0);
                }
                break;
            }
            setState(324);
            match(FROM);
            setState(327);
            _errHandler.sync(this);
            switch (getInterpreter().adaptivePredict(_input, 36, _ctx)) {
              case 1:
                {
                  setState(325);
                  ((SubstrFunctionCallContext) _localctx).fromDecimal = decimalLiteral();
                }
                break;
              case 2:
                {
                  setState(326);
                  ((SubstrFunctionCallContext) _localctx).fromExpression = expression(0);
                }
                break;
            }
            setState(334);
            _errHandler.sync(this);
            _la = _input.LA(1);
            if (_la == FOR) {
              {
                setState(329);
                match(FOR);
                setState(332);
                _errHandler.sync(this);
                switch (getInterpreter().adaptivePredict(_input, 37, _ctx)) {
                  case 1:
                    {
                      setState(330);
                      ((SubstrFunctionCallContext) _localctx).forDecimal = decimalLiteral();
                    }
                    break;
                  case 2:
                    {
                      setState(331);
                      ((SubstrFunctionCallContext) _localctx).forExpression = expression(0);
                    }
                    break;
                }
              }
            }

            setState(336);
            match(RR_BRACKET);
          }
          break;
        case 11:
          _localctx = new TrimFunctionCallContext(_localctx);
          enterOuterAlt(_localctx, 11);
          {
            setState(338);
            match(TRIM);
            setState(339);
            match(LR_BRACKET);
            setState(340);
            ((TrimFunctionCallContext) _localctx).positioinForm = _input.LT(1);
            _la = _input.LA(1);
            if (!(_la == BOTH || _la == LEADING || _la == TRAILING)) {
              ((TrimFunctionCallContext) _localctx).positioinForm =
                  (Token) _errHandler.recoverInline(this);
            } else {
              if (_input.LA(1) == Token.EOF) matchedEOF = true;
              _errHandler.reportMatch(this);
              consume();
            }
            setState(343);
            _errHandler.sync(this);
            switch (getInterpreter().adaptivePredict(_input, 39, _ctx)) {
              case 1:
                {
                  setState(341);
                  ((TrimFunctionCallContext) _localctx).sourceString = stringLiteral();
                }
                break;
              case 2:
                {
                  setState(342);
                  ((TrimFunctionCallContext) _localctx).sourceExpression = expression(0);
                }
                break;
            }
            setState(345);
            match(FROM);
            setState(348);
            _errHandler.sync(this);
            switch (getInterpreter().adaptivePredict(_input, 40, _ctx)) {
              case 1:
                {
                  setState(346);
                  ((TrimFunctionCallContext) _localctx).fromString = stringLiteral();
                }
                break;
              case 2:
                {
                  setState(347);
                  ((TrimFunctionCallContext) _localctx).fromExpression = expression(0);
                }
                break;
            }
            setState(350);
            match(RR_BRACKET);
          }
          break;
        case 12:
          _localctx = new TrimFunctionCallContext(_localctx);
          enterOuterAlt(_localctx, 12);
          {
            setState(352);
            match(TRIM);
            setState(353);
            match(LR_BRACKET);
            setState(356);
            _errHandler.sync(this);
            switch (getInterpreter().adaptivePredict(_input, 41, _ctx)) {
              case 1:
                {
                  setState(354);
                  ((TrimFunctionCallContext) _localctx).sourceString = stringLiteral();
                }
                break;
              case 2:
                {
                  setState(355);
                  ((TrimFunctionCallContext) _localctx).sourceExpression = expression(0);
                }
                break;
            }
            setState(358);
            match(FROM);
            setState(361);
            _errHandler.sync(this);
            switch (getInterpreter().adaptivePredict(_input, 42, _ctx)) {
              case 1:
                {
                  setState(359);
                  ((TrimFunctionCallContext) _localctx).fromString = stringLiteral();
                }
                break;
              case 2:
                {
                  setState(360);
                  ((TrimFunctionCallContext) _localctx).fromExpression = expression(0);
                }
                break;
            }
            setState(363);
            match(RR_BRACKET);
          }
          break;
        case 13:
          _localctx = new WeightFunctionCallContext(_localctx);
          enterOuterAlt(_localctx, 13);
          {
            setState(365);
            match(WEIGHT_STRING);
            setState(366);
            match(LR_BRACKET);
            setState(369);
            _errHandler.sync(this);
            switch (getInterpreter().adaptivePredict(_input, 43, _ctx)) {
              case 1:
                {
                  setState(367);
                  stringLiteral();
                }
                break;
              case 2:
                {
                  setState(368);
                  expression(0);
                }
                break;
            }
            setState(377);
            _errHandler.sync(this);
            _la = _input.LA(1);
            if (_la == AS) {
              {
                setState(371);
                match(AS);
                setState(372);
                ((WeightFunctionCallContext) _localctx).stringFormat = _input.LT(1);
                _la = _input.LA(1);
                if (!(_la == CHAR || _la == BINARY)) {
                  ((WeightFunctionCallContext) _localctx).stringFormat =
                      (Token) _errHandler.recoverInline(this);
                } else {
                  if (_input.LA(1) == Token.EOF) matchedEOF = true;
                  _errHandler.reportMatch(this);
                  consume();
                }
                setState(373);
                match(LR_BRACKET);
                setState(374);
                decimalLiteral();
                setState(375);
                match(RR_BRACKET);
              }
            }

            setState(380);
            _errHandler.sync(this);
            _la = _input.LA(1);
            if (_la == LEVEL) {
              {
                setState(379);
                levelsInWeightString();
              }
            }

            setState(382);
            match(RR_BRACKET);
          }
          break;
        case 14:
          _localctx = new ExtractFunctionCallContext(_localctx);
          enterOuterAlt(_localctx, 14);
          {
            setState(384);
            match(EXTRACT);
            setState(385);
            match(LR_BRACKET);
            setState(386);
            intervalType();
            setState(387);
            match(FROM);
            setState(390);
            _errHandler.sync(this);
            switch (getInterpreter().adaptivePredict(_input, 46, _ctx)) {
              case 1:
                {
                  setState(388);
                  ((ExtractFunctionCallContext) _localctx).sourceString = stringLiteral();
                }
                break;
              case 2:
                {
                  setState(389);
                  ((ExtractFunctionCallContext) _localctx).sourceExpression = expression(0);
                }
                break;
            }
            setState(392);
            match(RR_BRACKET);
          }
          break;
        case 15:
          _localctx = new GetFormatFunctionCallContext(_localctx);
          enterOuterAlt(_localctx, 15);
          {
            setState(394);
            match(GET_FORMAT);
            setState(395);
            match(LR_BRACKET);
            setState(396);
            ((GetFormatFunctionCallContext) _localctx).datetimeFormat = _input.LT(1);
            _la = _input.LA(1);
            if (!(((((_la - 182)) & ~0x3f) == 0
                && ((1L << (_la - 182))
                        & ((1L << (DATE - 182)) | (1L << (TIME - 182)) | (1L << (DATETIME - 182))))
                    != 0))) {
              ((GetFormatFunctionCallContext) _localctx).datetimeFormat =
                  (Token) _errHandler.recoverInline(this);
            } else {
              if (_input.LA(1) == Token.EOF) matchedEOF = true;
              _errHandler.reportMatch(this);
              consume();
            }
            setState(397);
            match(COMMA);
            setState(398);
            stringLiteral();
            setState(399);
            match(RR_BRACKET);
          }
          break;
      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  public static class CaseFuncAlternativeContext extends ParserRuleContext {
    public FunctionArgContext condition;
    public FunctionArgContext consequent;

    public TerminalNode WHEN() {
      return getToken(MySqlParser.WHEN, 0);
    }

    public TerminalNode THEN() {
      return getToken(MySqlParser.THEN, 0);
    }

    public List<FunctionArgContext> functionArg() {
      return getRuleContexts(FunctionArgContext.class);
    }

    public FunctionArgContext functionArg(int i) {
      return getRuleContext(FunctionArgContext.class, i);
    }

    public CaseFuncAlternativeContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_caseFuncAlternative;
    }

    @Override
    public void enterRule(ParseTreeListener listener) {
      if (listener instanceof MySqlParserListener)
        ((MySqlParserListener) listener).enterCaseFuncAlternative(this);
    }

    @Override
    public void exitRule(ParseTreeListener listener) {
      if (listener instanceof MySqlParserListener)
        ((MySqlParserListener) listener).exitCaseFuncAlternative(this);
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof MySqlParserVisitor)
        return ((MySqlParserVisitor<? extends T>) visitor).visitCaseFuncAlternative(this);
      else return visitor.visitChildren(this);
    }
  }

  public final CaseFuncAlternativeContext caseFuncAlternative() throws RecognitionException {
    CaseFuncAlternativeContext _localctx = new CaseFuncAlternativeContext(_ctx, getState());
    enterRule(_localctx, 42, RULE_caseFuncAlternative);
    try {
      enterOuterAlt(_localctx, 1);
      {
        setState(403);
        match(WHEN);
        setState(404);
        ((CaseFuncAlternativeContext) _localctx).condition = functionArg();
        setState(405);
        match(THEN);
        setState(406);
        ((CaseFuncAlternativeContext) _localctx).consequent = functionArg();
      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  public static class LevelsInWeightStringContext extends ParserRuleContext {
    public LevelsInWeightStringContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_levelsInWeightString;
    }

    public LevelsInWeightStringContext() {}

    public void copyFrom(LevelsInWeightStringContext ctx) {
      super.copyFrom(ctx);
    }
  }

  public static class LevelWeightRangeContext extends LevelsInWeightStringContext {
    public DecimalLiteralContext firstLevel;
    public DecimalLiteralContext lastLevel;

    public TerminalNode LEVEL() {
      return getToken(MySqlParser.LEVEL, 0);
    }

    public List<DecimalLiteralContext> decimalLiteral() {
      return getRuleContexts(DecimalLiteralContext.class);
    }

    public DecimalLiteralContext decimalLiteral(int i) {
      return getRuleContext(DecimalLiteralContext.class, i);
    }

    public LevelWeightRangeContext(LevelsInWeightStringContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public void enterRule(ParseTreeListener listener) {
      if (listener instanceof MySqlParserListener)
        ((MySqlParserListener) listener).enterLevelWeightRange(this);
    }

    @Override
    public void exitRule(ParseTreeListener listener) {
      if (listener instanceof MySqlParserListener)
        ((MySqlParserListener) listener).exitLevelWeightRange(this);
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof MySqlParserVisitor)
        return ((MySqlParserVisitor<? extends T>) visitor).visitLevelWeightRange(this);
      else return visitor.visitChildren(this);
    }
  }

  public static class LevelWeightListContext extends LevelsInWeightStringContext {
    public TerminalNode LEVEL() {
      return getToken(MySqlParser.LEVEL, 0);
    }

    public List<LevelInWeightListElementContext> levelInWeightListElement() {
      return getRuleContexts(LevelInWeightListElementContext.class);
    }

    public LevelInWeightListElementContext levelInWeightListElement(int i) {
      return getRuleContext(LevelInWeightListElementContext.class, i);
    }

    public LevelWeightListContext(LevelsInWeightStringContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public void enterRule(ParseTreeListener listener) {
      if (listener instanceof MySqlParserListener)
        ((MySqlParserListener) listener).enterLevelWeightList(this);
    }

    @Override
    public void exitRule(ParseTreeListener listener) {
      if (listener instanceof MySqlParserListener)
        ((MySqlParserListener) listener).exitLevelWeightList(this);
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof MySqlParserVisitor)
        return ((MySqlParserVisitor<? extends T>) visitor).visitLevelWeightList(this);
      else return visitor.visitChildren(this);
    }
  }

  public final LevelsInWeightStringContext levelsInWeightString() throws RecognitionException {
    LevelsInWeightStringContext _localctx = new LevelsInWeightStringContext(_ctx, getState());
    enterRule(_localctx, 44, RULE_levelsInWeightString);
    int _la;
    try {
      setState(422);
      _errHandler.sync(this);
      switch (getInterpreter().adaptivePredict(_input, 49, _ctx)) {
        case 1:
          _localctx = new LevelWeightListContext(_localctx);
          enterOuterAlt(_localctx, 1);
          {
            setState(408);
            match(LEVEL);
            setState(409);
            levelInWeightListElement();
            setState(414);
            _errHandler.sync(this);
            _la = _input.LA(1);
            while (_la == COMMA) {
              {
                {
                  setState(410);
                  match(COMMA);
                  setState(411);
                  levelInWeightListElement();
                }
              }
              setState(416);
              _errHandler.sync(this);
              _la = _input.LA(1);
            }
          }
          break;
        case 2:
          _localctx = new LevelWeightRangeContext(_localctx);
          enterOuterAlt(_localctx, 2);
          {
            setState(417);
            match(LEVEL);
            setState(418);
            ((LevelWeightRangeContext) _localctx).firstLevel = decimalLiteral();
            setState(419);
            match(MINUS);
            setState(420);
            ((LevelWeightRangeContext) _localctx).lastLevel = decimalLiteral();
          }
          break;
      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  public static class LevelInWeightListElementContext extends ParserRuleContext {
    public Token orderType;

    public DecimalLiteralContext decimalLiteral() {
      return getRuleContext(DecimalLiteralContext.class, 0);
    }

    public TerminalNode ASC() {
      return getToken(MySqlParser.ASC, 0);
    }

    public TerminalNode DESC() {
      return getToken(MySqlParser.DESC, 0);
    }

    public TerminalNode REVERSE() {
      return getToken(MySqlParser.REVERSE, 0);
    }

    public LevelInWeightListElementContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_levelInWeightListElement;
    }

    @Override
    public void enterRule(ParseTreeListener listener) {
      if (listener instanceof MySqlParserListener)
        ((MySqlParserListener) listener).enterLevelInWeightListElement(this);
    }

    @Override
    public void exitRule(ParseTreeListener listener) {
      if (listener instanceof MySqlParserListener)
        ((MySqlParserListener) listener).exitLevelInWeightListElement(this);
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof MySqlParserVisitor)
        return ((MySqlParserVisitor<? extends T>) visitor).visitLevelInWeightListElement(this);
      else return visitor.visitChildren(this);
    }
  }

  public final LevelInWeightListElementContext levelInWeightListElement()
      throws RecognitionException {
    LevelInWeightListElementContext _localctx =
        new LevelInWeightListElementContext(_ctx, getState());
    enterRule(_localctx, 46, RULE_levelInWeightListElement);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
        setState(424);
        decimalLiteral();
        setState(426);
        _errHandler.sync(this);
        _la = _input.LA(1);
        if (_la == ASC || _la == DESC || _la == REVERSE) {
          {
            setState(425);
            ((LevelInWeightListElementContext) _localctx).orderType = _input.LT(1);
            _la = _input.LA(1);
            if (!(_la == ASC || _la == DESC || _la == REVERSE)) {
              ((LevelInWeightListElementContext) _localctx).orderType =
                  (Token) _errHandler.recoverInline(this);
            } else {
              if (_input.LA(1) == Token.EOF) matchedEOF = true;
              _errHandler.reportMatch(this);
              consume();
            }
          }
        }
      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  public static class ScalarFunctionNameContext extends ParserRuleContext {
    public FunctionNameBaseContext functionNameBase() {
      return getRuleContext(FunctionNameBaseContext.class, 0);
    }

    public TerminalNode ASCII() {
      return getToken(MySqlParser.ASCII, 0);
    }

    public TerminalNode CURDATE() {
      return getToken(MySqlParser.CURDATE, 0);
    }

    public TerminalNode CURRENT_DATE() {
      return getToken(MySqlParser.CURRENT_DATE, 0);
    }

    public TerminalNode CURRENT_TIME() {
      return getToken(MySqlParser.CURRENT_TIME, 0);
    }

    public TerminalNode CURRENT_TIMESTAMP() {
      return getToken(MySqlParser.CURRENT_TIMESTAMP, 0);
    }

    public TerminalNode CURTIME() {
      return getToken(MySqlParser.CURTIME, 0);
    }

    public TerminalNode DATE_ADD() {
      return getToken(MySqlParser.DATE_ADD, 0);
    }

    public TerminalNode DATE_SUB() {
      return getToken(MySqlParser.DATE_SUB, 0);
    }

    public TerminalNode IF() {
      return getToken(MySqlParser.IF, 0);
    }

    public TerminalNode INSERT() {
      return getToken(MySqlParser.INSERT, 0);
    }

    public TerminalNode LOCALTIME() {
      return getToken(MySqlParser.LOCALTIME, 0);
    }

    public TerminalNode LOCALTIMESTAMP() {
      return getToken(MySqlParser.LOCALTIMESTAMP, 0);
    }

    public TerminalNode MID() {
      return getToken(MySqlParser.MID, 0);
    }

    public TerminalNode NOW() {
      return getToken(MySqlParser.NOW, 0);
    }

    public TerminalNode REPLACE() {
      return getToken(MySqlParser.REPLACE, 0);
    }

    public TerminalNode SUBSTR() {
      return getToken(MySqlParser.SUBSTR, 0);
    }

    public TerminalNode SUBSTRING() {
      return getToken(MySqlParser.SUBSTRING, 0);
    }

    public TerminalNode SYSDATE() {
      return getToken(MySqlParser.SYSDATE, 0);
    }

    public TerminalNode TRIM() {
      return getToken(MySqlParser.TRIM, 0);
    }

    public TerminalNode UTC_DATE() {
      return getToken(MySqlParser.UTC_DATE, 0);
    }

    public TerminalNode UTC_TIME() {
      return getToken(MySqlParser.UTC_TIME, 0);
    }

    public TerminalNode UTC_TIMESTAMP() {
      return getToken(MySqlParser.UTC_TIMESTAMP, 0);
    }

    public ScalarFunctionNameContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_scalarFunctionName;
    }

    @Override
    public void enterRule(ParseTreeListener listener) {
      if (listener instanceof MySqlParserListener)
        ((MySqlParserListener) listener).enterScalarFunctionName(this);
    }

    @Override
    public void exitRule(ParseTreeListener listener) {
      if (listener instanceof MySqlParserListener)
        ((MySqlParserListener) listener).exitScalarFunctionName(this);
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof MySqlParserVisitor)
        return ((MySqlParserVisitor<? extends T>) visitor).visitScalarFunctionName(this);
      else return visitor.visitChildren(this);
    }
  }

  public final ScalarFunctionNameContext scalarFunctionName() throws RecognitionException {
    ScalarFunctionNameContext _localctx = new ScalarFunctionNameContext(_ctx, getState());
    enterRule(_localctx, 48, RULE_scalarFunctionName);
    try {
      setState(451);
      _errHandler.sync(this);
      switch (_input.LA(1)) {
        case DATABASE:
        case LEFT:
        case RIGHT:
        case DATE:
        case TIME:
        case TIMESTAMP:
        case YEAR:
        case COUNT:
        case POSITION:
        case QUARTER:
        case MONTH:
        case DAY:
        case HOUR:
        case MINUTE:
        case WEEK:
        case SECOND:
        case MICROSECOND:
        case GEOMETRYCOLLECTION:
        case LINESTRING:
        case MULTILINESTRING:
        case MULTIPOINT:
        case MULTIPOLYGON:
        case POINT:
        case POLYGON:
        case ABS:
        case ACOS:
        case ADDDATE:
        case ADDTIME:
        case AES_DECRYPT:
        case AES_ENCRYPT:
        case AREA:
        case ASBINARY:
        case ASIN:
        case ASTEXT:
        case ASWKB:
        case ASWKT:
        case ASYMMETRIC_DECRYPT:
        case ASYMMETRIC_DERIVE:
        case ASYMMETRIC_ENCRYPT:
        case ASYMMETRIC_SIGN:
        case ASYMMETRIC_VERIFY:
        case ATAN:
        case ATAN2:
        case BENCHMARK:
        case BIN:
        case BIT_COUNT:
        case BIT_LENGTH:
        case BUFFER:
        case CEIL:
        case CEILING:
        case CENTROID:
        case CHARACTER_LENGTH:
        case CHARSET:
        case CHAR_LENGTH:
        case COERCIBILITY:
        case COLLATION:
        case COMPRESS:
        case CONCAT:
        case CONCAT_WS:
        case CONNECTION_ID:
        case CONV:
        case CONVERT_TZ:
        case COS:
        case COT:
        case CRC32:
        case CREATE_ASYMMETRIC_PRIV_KEY:
        case CREATE_ASYMMETRIC_PUB_KEY:
        case CREATE_DH_PARAMETERS:
        case CREATE_DIGEST:
        case CROSSES:
        case DATEDIFF:
        case DATE_FORMAT:
        case DAYNAME:
        case DAYOFMONTH:
        case DAYOFWEEK:
        case DAYOFYEAR:
        case DECODE:
        case DEGREES:
        case DES_DECRYPT:
        case DES_ENCRYPT:
        case DIMENSION:
        case DISJOINT:
        case ELT:
        case ENCODE:
        case ENCRYPT:
        case ENDPOINT:
        case ENVELOPE:
        case EQUALS:
        case EXP:
        case EXPORT_SET:
        case EXTERIORRING:
        case EXTRACTVALUE:
        case FIELD:
        case FIND_IN_SET:
        case FLOOR:
        case FORMAT:
        case FOUND_ROWS:
        case FROM_BASE64:
        case FROM_DAYS:
        case FROM_UNIXTIME:
        case GEOMCOLLFROMTEXT:
        case GEOMCOLLFROMWKB:
        case GEOMETRYCOLLECTIONFROMTEXT:
        case GEOMETRYCOLLECTIONFROMWKB:
        case GEOMETRYFROMTEXT:
        case GEOMETRYFROMWKB:
        case GEOMETRYN:
        case GEOMETRYTYPE:
        case GEOMFROMTEXT:
        case GEOMFROMWKB:
        case GET_FORMAT:
        case GET_LOCK:
        case GLENGTH:
        case GREATEST:
        case GTID_SUBSET:
        case GTID_SUBTRACT:
        case HEX:
        case IFNULL:
        case INET6_ATON:
        case INET6_NTOA:
        case INET_ATON:
        case INET_NTOA:
        case INSTR:
        case INTERIORRINGN:
        case INTERSECTS:
        case ISCLOSED:
        case ISEMPTY:
        case ISNULL:
        case ISSIMPLE:
        case IS_FREE_LOCK:
        case IS_IPV4:
        case IS_IPV4_COMPAT:
        case IS_IPV4_MAPPED:
        case IS_IPV6:
        case IS_USED_LOCK:
        case LAST_INSERT_ID:
        case LCASE:
        case LEAST:
        case LENGTH:
        case LINEFROMTEXT:
        case LINEFROMWKB:
        case LINESTRINGFROMTEXT:
        case LINESTRINGFROMWKB:
        case LN:
        case LOAD_FILE:
        case LOCATE:
        case LOG:
        case LOG10:
        case LOG2:
        case LOWER:
        case LPAD:
        case LTRIM:
        case MAKEDATE:
        case MAKETIME:
        case MAKE_SET:
        case MASTER_POS_WAIT:
        case MBRCONTAINS:
        case MBRDISJOINT:
        case MBREQUAL:
        case MBRINTERSECTS:
        case MBROVERLAPS:
        case MBRTOUCHES:
        case MBRWITHIN:
        case MD5:
        case MLINEFROMTEXT:
        case MLINEFROMWKB:
        case MONTHNAME:
        case MPOINTFROMTEXT:
        case MPOINTFROMWKB:
        case MPOLYFROMTEXT:
        case MPOLYFROMWKB:
        case MULTILINESTRINGFROMTEXT:
        case MULTILINESTRINGFROMWKB:
        case MULTIPOINTFROMTEXT:
        case MULTIPOINTFROMWKB:
        case MULTIPOLYGONFROMTEXT:
        case MULTIPOLYGONFROMWKB:
        case NAME_CONST:
        case NULLIF:
        case NUMGEOMETRIES:
        case NUMINTERIORRINGS:
        case NUMPOINTS:
        case OCT:
        case OCTET_LENGTH:
        case ORD:
        case OVERLAPS:
        case PERIOD_ADD:
        case PERIOD_DIFF:
        case PI:
        case POINTFROMTEXT:
        case POINTFROMWKB:
        case POINTN:
        case POLYFROMTEXT:
        case POLYFROMWKB:
        case POLYGONFROMTEXT:
        case POLYGONFROMWKB:
        case POW:
        case POWER:
        case QUOTE:
        case RADIANS:
        case RAND:
        case RANDOM_BYTES:
        case RELEASE_LOCK:
        case REVERSE:
        case ROUND:
        case ROW_COUNT:
        case RPAD:
        case RTRIM:
        case SEC_TO_TIME:
        case SESSION_USER:
        case SHA:
        case SHA1:
        case SHA2:
        case SIGN:
        case SIN:
        case SLEEP:
        case SOUNDEX:
        case SQL_THREAD_WAIT_AFTER_GTIDS:
        case SQRT:
        case SRID:
        case STARTPOINT:
        case STRCMP:
        case STR_TO_DATE:
        case ST_AREA:
        case ST_ASBINARY:
        case ST_ASTEXT:
        case ST_ASWKB:
        case ST_ASWKT:
        case ST_BUFFER:
        case ST_CENTROID:
        case ST_CONTAINS:
        case ST_CROSSES:
        case ST_DIFFERENCE:
        case ST_DIMENSION:
        case ST_DISJOINT:
        case ST_DISTANCE:
        case ST_ENDPOINT:
        case ST_ENVELOPE:
        case ST_EQUALS:
        case ST_EXTERIORRING:
        case ST_GEOMCOLLFROMTEXT:
        case ST_GEOMCOLLFROMTXT:
        case ST_GEOMCOLLFROMWKB:
        case ST_GEOMETRYCOLLECTIONFROMTEXT:
        case ST_GEOMETRYCOLLECTIONFROMWKB:
        case ST_GEOMETRYFROMTEXT:
        case ST_GEOMETRYFROMWKB:
        case ST_GEOMETRYN:
        case ST_GEOMETRYTYPE:
        case ST_GEOMFROMTEXT:
        case ST_GEOMFROMWKB:
        case ST_INTERIORRINGN:
        case ST_INTERSECTION:
        case ST_INTERSECTS:
        case ST_ISCLOSED:
        case ST_ISEMPTY:
        case ST_ISSIMPLE:
        case ST_LINEFROMTEXT:
        case ST_LINEFROMWKB:
        case ST_LINESTRINGFROMTEXT:
        case ST_LINESTRINGFROMWKB:
        case ST_NUMGEOMETRIES:
        case ST_NUMINTERIORRING:
        case ST_NUMINTERIORRINGS:
        case ST_NUMPOINTS:
        case ST_OVERLAPS:
        case ST_POINTFROMTEXT:
        case ST_POINTFROMWKB:
        case ST_POINTN:
        case ST_POLYFROMTEXT:
        case ST_POLYFROMWKB:
        case ST_POLYGONFROMTEXT:
        case ST_POLYGONFROMWKB:
        case ST_SRID:
        case ST_STARTPOINT:
        case ST_SYMDIFFERENCE:
        case ST_TOUCHES:
        case ST_UNION:
        case ST_WITHIN:
        case ST_X:
        case ST_Y:
        case SUBDATE:
        case SUBSTRING_INDEX:
        case SUBTIME:
        case SYSTEM_USER:
        case TAN:
        case TIMEDIFF:
        case TIMESTAMPADD:
        case TIMESTAMPDIFF:
        case TIME_FORMAT:
        case TIME_TO_SEC:
        case TOUCHES:
        case TO_BASE64:
        case TO_DAYS:
        case TO_SECONDS:
        case UCASE:
        case UNCOMPRESS:
        case UNCOMPRESSED_LENGTH:
        case UNHEX:
        case UNIX_TIMESTAMP:
        case UPDATEXML:
        case UPPER:
        case UUID:
        case UUID_SHORT:
        case VALIDATE_PASSWORD_STRENGTH:
        case VERSION:
        case WAIT_UNTIL_SQL_THREAD_AFTER_GTIDS:
        case WEEKDAY:
        case WEEKOFYEAR:
        case WEIGHT_STRING:
        case WITHIN:
        case YEARWEEK:
        case Y_FUNCTION:
        case X_FUNCTION:
          enterOuterAlt(_localctx, 1);
          {
            setState(428);
            functionNameBase();
          }
          break;
        case ASCII:
          enterOuterAlt(_localctx, 2);
          {
            setState(429);
            match(ASCII);
          }
          break;
        case CURDATE:
          enterOuterAlt(_localctx, 3);
          {
            setState(430);
            match(CURDATE);
          }
          break;
        case CURRENT_DATE:
          enterOuterAlt(_localctx, 4);
          {
            setState(431);
            match(CURRENT_DATE);
          }
          break;
        case CURRENT_TIME:
          enterOuterAlt(_localctx, 5);
          {
            setState(432);
            match(CURRENT_TIME);
          }
          break;
        case CURRENT_TIMESTAMP:
          enterOuterAlt(_localctx, 6);
          {
            setState(433);
            match(CURRENT_TIMESTAMP);
          }
          break;
        case CURTIME:
          enterOuterAlt(_localctx, 7);
          {
            setState(434);
            match(CURTIME);
          }
          break;
        case DATE_ADD:
          enterOuterAlt(_localctx, 8);
          {
            setState(435);
            match(DATE_ADD);
          }
          break;
        case DATE_SUB:
          enterOuterAlt(_localctx, 9);
          {
            setState(436);
            match(DATE_SUB);
          }
          break;
        case IF:
          enterOuterAlt(_localctx, 10);
          {
            setState(437);
            match(IF);
          }
          break;
        case INSERT:
          enterOuterAlt(_localctx, 11);
          {
            setState(438);
            match(INSERT);
          }
          break;
        case LOCALTIME:
          enterOuterAlt(_localctx, 12);
          {
            setState(439);
            match(LOCALTIME);
          }
          break;
        case LOCALTIMESTAMP:
          enterOuterAlt(_localctx, 13);
          {
            setState(440);
            match(LOCALTIMESTAMP);
          }
          break;
        case MID:
          enterOuterAlt(_localctx, 14);
          {
            setState(441);
            match(MID);
          }
          break;
        case NOW:
          enterOuterAlt(_localctx, 15);
          {
            setState(442);
            match(NOW);
          }
          break;
        case REPLACE:
          enterOuterAlt(_localctx, 16);
          {
            setState(443);
            match(REPLACE);
          }
          break;
        case SUBSTR:
          enterOuterAlt(_localctx, 17);
          {
            setState(444);
            match(SUBSTR);
          }
          break;
        case SUBSTRING:
          enterOuterAlt(_localctx, 18);
          {
            setState(445);
            match(SUBSTRING);
          }
          break;
        case SYSDATE:
          enterOuterAlt(_localctx, 19);
          {
            setState(446);
            match(SYSDATE);
          }
          break;
        case TRIM:
          enterOuterAlt(_localctx, 20);
          {
            setState(447);
            match(TRIM);
          }
          break;
        case UTC_DATE:
          enterOuterAlt(_localctx, 21);
          {
            setState(448);
            match(UTC_DATE);
          }
          break;
        case UTC_TIME:
          enterOuterAlt(_localctx, 22);
          {
            setState(449);
            match(UTC_TIME);
          }
          break;
        case UTC_TIMESTAMP:
          enterOuterAlt(_localctx, 23);
          {
            setState(450);
            match(UTC_TIMESTAMP);
          }
          break;
        default:
          throw new NoViableAltException(this);
      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  public static class FunctionArgsContext extends ParserRuleContext {
    public List<ConstantContext> constant() {
      return getRuleContexts(ConstantContext.class);
    }

    public ConstantContext constant(int i) {
      return getRuleContext(ConstantContext.class, i);
    }

    public List<FullColumnNameContext> fullColumnName() {
      return getRuleContexts(FullColumnNameContext.class);
    }

    public FullColumnNameContext fullColumnName(int i) {
      return getRuleContext(FullColumnNameContext.class, i);
    }

    public List<FunctionCallContext> functionCall() {
      return getRuleContexts(FunctionCallContext.class);
    }

    public FunctionCallContext functionCall(int i) {
      return getRuleContext(FunctionCallContext.class, i);
    }

    public List<ExpressionContext> expression() {
      return getRuleContexts(ExpressionContext.class);
    }

    public ExpressionContext expression(int i) {
      return getRuleContext(ExpressionContext.class, i);
    }

    public FunctionArgsContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_functionArgs;
    }

    @Override
    public void enterRule(ParseTreeListener listener) {
      if (listener instanceof MySqlParserListener)
        ((MySqlParserListener) listener).enterFunctionArgs(this);
    }

    @Override
    public void exitRule(ParseTreeListener listener) {
      if (listener instanceof MySqlParserListener)
        ((MySqlParserListener) listener).exitFunctionArgs(this);
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof MySqlParserVisitor)
        return ((MySqlParserVisitor<? extends T>) visitor).visitFunctionArgs(this);
      else return visitor.visitChildren(this);
    }
  }

  public final FunctionArgsContext functionArgs() throws RecognitionException {
    FunctionArgsContext _localctx = new FunctionArgsContext(_ctx, getState());
    enterRule(_localctx, 50, RULE_functionArgs);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
        setState(457);
        _errHandler.sync(this);
        switch (getInterpreter().adaptivePredict(_input, 52, _ctx)) {
          case 1:
            {
              setState(453);
              constant();
            }
            break;
          case 2:
            {
              setState(454);
              fullColumnName();
            }
            break;
          case 3:
            {
              setState(455);
              functionCall();
            }
            break;
          case 4:
            {
              setState(456);
              expression(0);
            }
            break;
        }
        setState(468);
        _errHandler.sync(this);
        _la = _input.LA(1);
        while (_la == COMMA) {
          {
            {
              setState(459);
              match(COMMA);
              setState(464);
              _errHandler.sync(this);
              switch (getInterpreter().adaptivePredict(_input, 53, _ctx)) {
                case 1:
                  {
                    setState(460);
                    constant();
                  }
                  break;
                case 2:
                  {
                    setState(461);
                    fullColumnName();
                  }
                  break;
                case 3:
                  {
                    setState(462);
                    functionCall();
                  }
                  break;
                case 4:
                  {
                    setState(463);
                    expression(0);
                  }
                  break;
              }
            }
          }
          setState(470);
          _errHandler.sync(this);
          _la = _input.LA(1);
        }
      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  public static class FunctionArgContext extends ParserRuleContext {
    public ConstantContext constant() {
      return getRuleContext(ConstantContext.class, 0);
    }

    public FullColumnNameContext fullColumnName() {
      return getRuleContext(FullColumnNameContext.class, 0);
    }

    public FunctionCallContext functionCall() {
      return getRuleContext(FunctionCallContext.class, 0);
    }

    public ExpressionContext expression() {
      return getRuleContext(ExpressionContext.class, 0);
    }

    public FunctionArgContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_functionArg;
    }

    @Override
    public void enterRule(ParseTreeListener listener) {
      if (listener instanceof MySqlParserListener)
        ((MySqlParserListener) listener).enterFunctionArg(this);
    }

    @Override
    public void exitRule(ParseTreeListener listener) {
      if (listener instanceof MySqlParserListener)
        ((MySqlParserListener) listener).exitFunctionArg(this);
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof MySqlParserVisitor)
        return ((MySqlParserVisitor<? extends T>) visitor).visitFunctionArg(this);
      else return visitor.visitChildren(this);
    }
  }

  public final FunctionArgContext functionArg() throws RecognitionException {
    FunctionArgContext _localctx = new FunctionArgContext(_ctx, getState());
    enterRule(_localctx, 52, RULE_functionArg);
    try {
      setState(475);
      _errHandler.sync(this);
      switch (getInterpreter().adaptivePredict(_input, 55, _ctx)) {
        case 1:
          enterOuterAlt(_localctx, 1);
          {
            setState(471);
            constant();
          }
          break;
        case 2:
          enterOuterAlt(_localctx, 2);
          {
            setState(472);
            fullColumnName();
          }
          break;
        case 3:
          enterOuterAlt(_localctx, 3);
          {
            setState(473);
            functionCall();
          }
          break;
        case 4:
          enterOuterAlt(_localctx, 4);
          {
            setState(474);
            expression(0);
          }
          break;
      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  public static class ExpressionContext extends ParserRuleContext {
    public ExpressionContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_expression;
    }

    public ExpressionContext() {}

    public void copyFrom(ExpressionContext ctx) {
      super.copyFrom(ctx);
    }
  }

  public static class IsExpressionContext extends ExpressionContext {
    public Token testValue;

    public PredicateContext predicate() {
      return getRuleContext(PredicateContext.class, 0);
    }

    public TerminalNode IS() {
      return getToken(MySqlParser.IS, 0);
    }

    public TerminalNode TRUE() {
      return getToken(MySqlParser.TRUE, 0);
    }

    public TerminalNode FALSE() {
      return getToken(MySqlParser.FALSE, 0);
    }

    public TerminalNode UNKNOWN() {
      return getToken(MySqlParser.UNKNOWN, 0);
    }

    public TerminalNode NOT() {
      return getToken(MySqlParser.NOT, 0);
    }

    public IsExpressionContext(ExpressionContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public void enterRule(ParseTreeListener listener) {
      if (listener instanceof MySqlParserListener)
        ((MySqlParserListener) listener).enterIsExpression(this);
    }

    @Override
    public void exitRule(ParseTreeListener listener) {
      if (listener instanceof MySqlParserListener)
        ((MySqlParserListener) listener).exitIsExpression(this);
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof MySqlParserVisitor)
        return ((MySqlParserVisitor<? extends T>) visitor).visitIsExpression(this);
      else return visitor.visitChildren(this);
    }
  }

  public static class NotExpressionContext extends ExpressionContext {
    public Token notOperator;

    public ExpressionContext expression() {
      return getRuleContext(ExpressionContext.class, 0);
    }

    public TerminalNode NOT() {
      return getToken(MySqlParser.NOT, 0);
    }

    public NotExpressionContext(ExpressionContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public void enterRule(ParseTreeListener listener) {
      if (listener instanceof MySqlParserListener)
        ((MySqlParserListener) listener).enterNotExpression(this);
    }

    @Override
    public void exitRule(ParseTreeListener listener) {
      if (listener instanceof MySqlParserListener)
        ((MySqlParserListener) listener).exitNotExpression(this);
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof MySqlParserVisitor)
        return ((MySqlParserVisitor<? extends T>) visitor).visitNotExpression(this);
      else return visitor.visitChildren(this);
    }
  }

  public static class LogicalExpressionContext extends ExpressionContext {
    public List<ExpressionContext> expression() {
      return getRuleContexts(ExpressionContext.class);
    }

    public ExpressionContext expression(int i) {
      return getRuleContext(ExpressionContext.class, i);
    }

    public LogicalOperatorContext logicalOperator() {
      return getRuleContext(LogicalOperatorContext.class, 0);
    }

    public LogicalExpressionContext(ExpressionContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public void enterRule(ParseTreeListener listener) {
      if (listener instanceof MySqlParserListener)
        ((MySqlParserListener) listener).enterLogicalExpression(this);
    }

    @Override
    public void exitRule(ParseTreeListener listener) {
      if (listener instanceof MySqlParserListener)
        ((MySqlParserListener) listener).exitLogicalExpression(this);
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof MySqlParserVisitor)
        return ((MySqlParserVisitor<? extends T>) visitor).visitLogicalExpression(this);
      else return visitor.visitChildren(this);
    }
  }

  public static class PredicateExpressionContext extends ExpressionContext {
    public PredicateContext predicate() {
      return getRuleContext(PredicateContext.class, 0);
    }

    public PredicateExpressionContext(ExpressionContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public void enterRule(ParseTreeListener listener) {
      if (listener instanceof MySqlParserListener)
        ((MySqlParserListener) listener).enterPredicateExpression(this);
    }

    @Override
    public void exitRule(ParseTreeListener listener) {
      if (listener instanceof MySqlParserListener)
        ((MySqlParserListener) listener).exitPredicateExpression(this);
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof MySqlParserVisitor)
        return ((MySqlParserVisitor<? extends T>) visitor).visitPredicateExpression(this);
      else return visitor.visitChildren(this);
    }
  }

  public final ExpressionContext expression() throws RecognitionException {
    return expression(0);
  }

  private ExpressionContext expression(int _p) throws RecognitionException {
    ParserRuleContext _parentctx = _ctx;
    int _parentState = getState();
    ExpressionContext _localctx = new ExpressionContext(_ctx, _parentState);
    ExpressionContext _prevctx = _localctx;
    int _startState = 54;
    enterRecursionRule(_localctx, 54, RULE_expression, _p);
    int _la;
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      {
        setState(488);
        _errHandler.sync(this);
        switch (getInterpreter().adaptivePredict(_input, 57, _ctx)) {
          case 1:
            {
              _localctx = new NotExpressionContext(_localctx);
              _ctx = _localctx;
              _prevctx = _localctx;

              setState(478);
              ((NotExpressionContext) _localctx).notOperator = _input.LT(1);
              _la = _input.LA(1);
              if (!(_la == NOT || _la == EXCLAMATION_SYMBOL)) {
                ((NotExpressionContext) _localctx).notOperator =
                    (Token) _errHandler.recoverInline(this);
              } else {
                if (_input.LA(1) == Token.EOF) matchedEOF = true;
                _errHandler.reportMatch(this);
                consume();
              }
              setState(479);
              expression(4);
            }
            break;
          case 2:
            {
              _localctx = new IsExpressionContext(_localctx);
              _ctx = _localctx;
              _prevctx = _localctx;
              setState(480);
              predicate(0);
              setState(481);
              match(IS);
              setState(483);
              _errHandler.sync(this);
              _la = _input.LA(1);
              if (_la == NOT) {
                {
                  setState(482);
                  match(NOT);
                }
              }

              setState(485);
              ((IsExpressionContext) _localctx).testValue = _input.LT(1);
              _la = _input.LA(1);
              if (!(_la == FALSE || _la == TRUE || _la == UNKNOWN)) {
                ((IsExpressionContext) _localctx).testValue =
                    (Token) _errHandler.recoverInline(this);
              } else {
                if (_input.LA(1) == Token.EOF) matchedEOF = true;
                _errHandler.reportMatch(this);
                consume();
              }
            }
            break;
          case 3:
            {
              _localctx = new PredicateExpressionContext(_localctx);
              _ctx = _localctx;
              _prevctx = _localctx;
              setState(487);
              predicate(0);
            }
            break;
        }
        _ctx.stop = _input.LT(-1);
        setState(496);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input, 58, _ctx);
        while (_alt != 2 && _alt != org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER) {
          if (_alt == 1) {
            if (_parseListeners != null) triggerExitRuleEvent();
            _prevctx = _localctx;
            {
              {
                _localctx =
                    new LogicalExpressionContext(new ExpressionContext(_parentctx, _parentState));
                pushNewRecursionContext(_localctx, _startState, RULE_expression);
                setState(490);
                if (!(precpred(_ctx, 3)))
                  throw new FailedPredicateException(this, "precpred(_ctx, 3)");
                setState(491);
                logicalOperator();
                setState(492);
                expression(4);
              }
            }
          }
          setState(498);
          _errHandler.sync(this);
          _alt = getInterpreter().adaptivePredict(_input, 58, _ctx);
        }
      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      unrollRecursionContexts(_parentctx);
    }
    return _localctx;
  }

  public static class PredicateContext extends ParserRuleContext {
    public PredicateContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_predicate;
    }

    public PredicateContext() {}

    public void copyFrom(PredicateContext ctx) {
      super.copyFrom(ctx);
    }
  }

  public static class SoundsLikePredicateContext extends PredicateContext {
    public List<PredicateContext> predicate() {
      return getRuleContexts(PredicateContext.class);
    }

    public PredicateContext predicate(int i) {
      return getRuleContext(PredicateContext.class, i);
    }

    public TerminalNode SOUNDS() {
      return getToken(MySqlParser.SOUNDS, 0);
    }

    public TerminalNode LIKE() {
      return getToken(MySqlParser.LIKE, 0);
    }

    public SoundsLikePredicateContext(PredicateContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public void enterRule(ParseTreeListener listener) {
      if (listener instanceof MySqlParserListener)
        ((MySqlParserListener) listener).enterSoundsLikePredicate(this);
    }

    @Override
    public void exitRule(ParseTreeListener listener) {
      if (listener instanceof MySqlParserListener)
        ((MySqlParserListener) listener).exitSoundsLikePredicate(this);
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof MySqlParserVisitor)
        return ((MySqlParserVisitor<? extends T>) visitor).visitSoundsLikePredicate(this);
      else return visitor.visitChildren(this);
    }
  }

  public static class ExpressionAtomPredicateContext extends PredicateContext {
    public ExpressionAtomContext expressionAtom() {
      return getRuleContext(ExpressionAtomContext.class, 0);
    }

    public TerminalNode LOCAL_ID() {
      return getToken(MySqlParser.LOCAL_ID, 0);
    }

    public TerminalNode VAR_ASSIGN() {
      return getToken(MySqlParser.VAR_ASSIGN, 0);
    }

    public ExpressionAtomPredicateContext(PredicateContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public void enterRule(ParseTreeListener listener) {
      if (listener instanceof MySqlParserListener)
        ((MySqlParserListener) listener).enterExpressionAtomPredicate(this);
    }

    @Override
    public void exitRule(ParseTreeListener listener) {
      if (listener instanceof MySqlParserListener)
        ((MySqlParserListener) listener).exitExpressionAtomPredicate(this);
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof MySqlParserVisitor)
        return ((MySqlParserVisitor<? extends T>) visitor).visitExpressionAtomPredicate(this);
      else return visitor.visitChildren(this);
    }
  }

  public static class BinaryComparisonPredicateContext extends PredicateContext {
    public PredicateContext left;
    public PredicateContext right;

    public ComparisonOperatorContext comparisonOperator() {
      return getRuleContext(ComparisonOperatorContext.class, 0);
    }

    public List<PredicateContext> predicate() {
      return getRuleContexts(PredicateContext.class);
    }

    public PredicateContext predicate(int i) {
      return getRuleContext(PredicateContext.class, i);
    }

    public BinaryComparisonPredicateContext(PredicateContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public void enterRule(ParseTreeListener listener) {
      if (listener instanceof MySqlParserListener)
        ((MySqlParserListener) listener).enterBinaryComparisonPredicate(this);
    }

    @Override
    public void exitRule(ParseTreeListener listener) {
      if (listener instanceof MySqlParserListener)
        ((MySqlParserListener) listener).exitBinaryComparisonPredicate(this);
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof MySqlParserVisitor)
        return ((MySqlParserVisitor<? extends T>) visitor).visitBinaryComparisonPredicate(this);
      else return visitor.visitChildren(this);
    }
  }

  public static class InPredicateContext extends PredicateContext {
    public PredicateContext predicate() {
      return getRuleContext(PredicateContext.class, 0);
    }

    public TerminalNode IN() {
      return getToken(MySqlParser.IN, 0);
    }

    public ExpressionsContext expressions() {
      return getRuleContext(ExpressionsContext.class, 0);
    }

    public TerminalNode NOT() {
      return getToken(MySqlParser.NOT, 0);
    }

    public InPredicateContext(PredicateContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public void enterRule(ParseTreeListener listener) {
      if (listener instanceof MySqlParserListener)
        ((MySqlParserListener) listener).enterInPredicate(this);
    }

    @Override
    public void exitRule(ParseTreeListener listener) {
      if (listener instanceof MySqlParserListener)
        ((MySqlParserListener) listener).exitInPredicate(this);
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof MySqlParserVisitor)
        return ((MySqlParserVisitor<? extends T>) visitor).visitInPredicate(this);
      else return visitor.visitChildren(this);
    }
  }

  public static class BetweenPredicateContext extends PredicateContext {
    public List<PredicateContext> predicate() {
      return getRuleContexts(PredicateContext.class);
    }

    public PredicateContext predicate(int i) {
      return getRuleContext(PredicateContext.class, i);
    }

    public TerminalNode BETWEEN() {
      return getToken(MySqlParser.BETWEEN, 0);
    }

    public TerminalNode AND() {
      return getToken(MySqlParser.AND, 0);
    }

    public TerminalNode NOT() {
      return getToken(MySqlParser.NOT, 0);
    }

    public BetweenPredicateContext(PredicateContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public void enterRule(ParseTreeListener listener) {
      if (listener instanceof MySqlParserListener)
        ((MySqlParserListener) listener).enterBetweenPredicate(this);
    }

    @Override
    public void exitRule(ParseTreeListener listener) {
      if (listener instanceof MySqlParserListener)
        ((MySqlParserListener) listener).exitBetweenPredicate(this);
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof MySqlParserVisitor)
        return ((MySqlParserVisitor<? extends T>) visitor).visitBetweenPredicate(this);
      else return visitor.visitChildren(this);
    }
  }

  public static class IsNullPredicateContext extends PredicateContext {
    public PredicateContext predicate() {
      return getRuleContext(PredicateContext.class, 0);
    }

    public TerminalNode IS() {
      return getToken(MySqlParser.IS, 0);
    }

    public NullNotnullContext nullNotnull() {
      return getRuleContext(NullNotnullContext.class, 0);
    }

    public IsNullPredicateContext(PredicateContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public void enterRule(ParseTreeListener listener) {
      if (listener instanceof MySqlParserListener)
        ((MySqlParserListener) listener).enterIsNullPredicate(this);
    }

    @Override
    public void exitRule(ParseTreeListener listener) {
      if (listener instanceof MySqlParserListener)
        ((MySqlParserListener) listener).exitIsNullPredicate(this);
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof MySqlParserVisitor)
        return ((MySqlParserVisitor<? extends T>) visitor).visitIsNullPredicate(this);
      else return visitor.visitChildren(this);
    }
  }

  public static class LikePredicateContext extends PredicateContext {
    public List<PredicateContext> predicate() {
      return getRuleContexts(PredicateContext.class);
    }

    public PredicateContext predicate(int i) {
      return getRuleContext(PredicateContext.class, i);
    }

    public TerminalNode LIKE() {
      return getToken(MySqlParser.LIKE, 0);
    }

    public TerminalNode NOT() {
      return getToken(MySqlParser.NOT, 0);
    }

    public TerminalNode ESCAPE() {
      return getToken(MySqlParser.ESCAPE, 0);
    }

    public TerminalNode STRING_LITERAL() {
      return getToken(MySqlParser.STRING_LITERAL, 0);
    }

    public LikePredicateContext(PredicateContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public void enterRule(ParseTreeListener listener) {
      if (listener instanceof MySqlParserListener)
        ((MySqlParserListener) listener).enterLikePredicate(this);
    }

    @Override
    public void exitRule(ParseTreeListener listener) {
      if (listener instanceof MySqlParserListener)
        ((MySqlParserListener) listener).exitLikePredicate(this);
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof MySqlParserVisitor)
        return ((MySqlParserVisitor<? extends T>) visitor).visitLikePredicate(this);
      else return visitor.visitChildren(this);
    }
  }

  public static class RegexpPredicateContext extends PredicateContext {
    public Token regex;

    public List<PredicateContext> predicate() {
      return getRuleContexts(PredicateContext.class);
    }

    public PredicateContext predicate(int i) {
      return getRuleContext(PredicateContext.class, i);
    }

    public TerminalNode REGEXP() {
      return getToken(MySqlParser.REGEXP, 0);
    }

    public TerminalNode RLIKE() {
      return getToken(MySqlParser.RLIKE, 0);
    }

    public TerminalNode NOT() {
      return getToken(MySqlParser.NOT, 0);
    }

    public RegexpPredicateContext(PredicateContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public void enterRule(ParseTreeListener listener) {
      if (listener instanceof MySqlParserListener)
        ((MySqlParserListener) listener).enterRegexpPredicate(this);
    }

    @Override
    public void exitRule(ParseTreeListener listener) {
      if (listener instanceof MySqlParserListener)
        ((MySqlParserListener) listener).exitRegexpPredicate(this);
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof MySqlParserVisitor)
        return ((MySqlParserVisitor<? extends T>) visitor).visitRegexpPredicate(this);
      else return visitor.visitChildren(this);
    }
  }

  public final PredicateContext predicate() throws RecognitionException {
    return predicate(0);
  }

  private PredicateContext predicate(int _p) throws RecognitionException {
    ParserRuleContext _parentctx = _ctx;
    int _parentState = getState();
    PredicateContext _localctx = new PredicateContext(_ctx, _parentState);
    PredicateContext _prevctx = _localctx;
    int _startState = 56;
    enterRecursionRule(_localctx, 56, RULE_predicate, _p);
    int _la;
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      {
        {
          _localctx = new ExpressionAtomPredicateContext(_localctx);
          _ctx = _localctx;
          _prevctx = _localctx;

          setState(502);
          _errHandler.sync(this);
          _la = _input.LA(1);
          if (_la == LOCAL_ID) {
            {
              setState(500);
              match(LOCAL_ID);
              setState(501);
              match(VAR_ASSIGN);
            }
          }

          setState(504);
          expressionAtom(0);
        }
        _ctx.stop = _input.LT(-1);
        setState(553);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input, 66, _ctx);
        while (_alt != 2 && _alt != org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER) {
          if (_alt == 1) {
            if (_parseListeners != null) triggerExitRuleEvent();
            _prevctx = _localctx;
            {
              setState(551);
              _errHandler.sync(this);
              switch (getInterpreter().adaptivePredict(_input, 65, _ctx)) {
                case 1:
                  {
                    _localctx =
                        new BinaryComparisonPredicateContext(
                            new PredicateContext(_parentctx, _parentState));
                    ((BinaryComparisonPredicateContext) _localctx).left = _prevctx;
                    pushNewRecursionContext(_localctx, _startState, RULE_predicate);
                    setState(506);
                    if (!(precpred(_ctx, 6)))
                      throw new FailedPredicateException(this, "precpred(_ctx, 6)");
                    setState(507);
                    comparisonOperator();
                    setState(508);
                    ((BinaryComparisonPredicateContext) _localctx).right = predicate(7);
                  }
                  break;
                case 2:
                  {
                    _localctx =
                        new BetweenPredicateContext(new PredicateContext(_parentctx, _parentState));
                    pushNewRecursionContext(_localctx, _startState, RULE_predicate);
                    setState(510);
                    if (!(precpred(_ctx, 5)))
                      throw new FailedPredicateException(this, "precpred(_ctx, 5)");
                    setState(512);
                    _errHandler.sync(this);
                    _la = _input.LA(1);
                    if (_la == NOT) {
                      {
                        setState(511);
                        match(NOT);
                      }
                    }

                    setState(514);
                    match(BETWEEN);
                    setState(515);
                    predicate(0);
                    setState(516);
                    match(AND);
                    setState(517);
                    predicate(6);
                  }
                  break;
                case 3:
                  {
                    _localctx =
                        new SoundsLikePredicateContext(
                            new PredicateContext(_parentctx, _parentState));
                    pushNewRecursionContext(_localctx, _startState, RULE_predicate);
                    setState(519);
                    if (!(precpred(_ctx, 4)))
                      throw new FailedPredicateException(this, "precpred(_ctx, 4)");
                    setState(520);
                    match(SOUNDS);
                    setState(521);
                    match(LIKE);
                    setState(522);
                    predicate(5);
                  }
                  break;
                case 4:
                  {
                    _localctx =
                        new RegexpPredicateContext(new PredicateContext(_parentctx, _parentState));
                    pushNewRecursionContext(_localctx, _startState, RULE_predicate);
                    setState(523);
                    if (!(precpred(_ctx, 2)))
                      throw new FailedPredicateException(this, "precpred(_ctx, 2)");
                    setState(525);
                    _errHandler.sync(this);
                    _la = _input.LA(1);
                    if (_la == NOT) {
                      {
                        setState(524);
                        match(NOT);
                      }
                    }

                    setState(527);
                    ((RegexpPredicateContext) _localctx).regex = _input.LT(1);
                    _la = _input.LA(1);
                    if (!(_la == REGEXP || _la == RLIKE)) {
                      ((RegexpPredicateContext) _localctx).regex =
                          (Token) _errHandler.recoverInline(this);
                    } else {
                      if (_input.LA(1) == Token.EOF) matchedEOF = true;
                      _errHandler.reportMatch(this);
                      consume();
                    }
                    setState(528);
                    predicate(3);
                  }
                  break;
                case 5:
                  {
                    _localctx =
                        new InPredicateContext(new PredicateContext(_parentctx, _parentState));
                    pushNewRecursionContext(_localctx, _startState, RULE_predicate);
                    setState(529);
                    if (!(precpred(_ctx, 8)))
                      throw new FailedPredicateException(this, "precpred(_ctx, 8)");
                    setState(531);
                    _errHandler.sync(this);
                    _la = _input.LA(1);
                    if (_la == NOT) {
                      {
                        setState(530);
                        match(NOT);
                      }
                    }

                    setState(533);
                    match(IN);
                    setState(534);
                    match(LR_BRACKET);
                    setState(535);
                    expressions();
                    setState(536);
                    match(RR_BRACKET);
                  }
                  break;
                case 6:
                  {
                    _localctx =
                        new IsNullPredicateContext(new PredicateContext(_parentctx, _parentState));
                    pushNewRecursionContext(_localctx, _startState, RULE_predicate);
                    setState(538);
                    if (!(precpred(_ctx, 7)))
                      throw new FailedPredicateException(this, "precpred(_ctx, 7)");
                    setState(539);
                    match(IS);
                    setState(540);
                    nullNotnull();
                  }
                  break;
                case 7:
                  {
                    _localctx =
                        new LikePredicateContext(new PredicateContext(_parentctx, _parentState));
                    pushNewRecursionContext(_localctx, _startState, RULE_predicate);
                    setState(541);
                    if (!(precpred(_ctx, 3)))
                      throw new FailedPredicateException(this, "precpred(_ctx, 3)");
                    setState(543);
                    _errHandler.sync(this);
                    _la = _input.LA(1);
                    if (_la == NOT) {
                      {
                        setState(542);
                        match(NOT);
                      }
                    }

                    setState(545);
                    match(LIKE);
                    setState(546);
                    predicate(0);
                    setState(549);
                    _errHandler.sync(this);
                    switch (getInterpreter().adaptivePredict(_input, 64, _ctx)) {
                      case 1:
                        {
                          setState(547);
                          match(ESCAPE);
                          setState(548);
                          match(STRING_LITERAL);
                        }
                        break;
                    }
                  }
                  break;
              }
            }
          }
          setState(555);
          _errHandler.sync(this);
          _alt = getInterpreter().adaptivePredict(_input, 66, _ctx);
        }
      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      unrollRecursionContexts(_parentctx);
    }
    return _localctx;
  }

  public static class ExpressionAtomContext extends ParserRuleContext {
    public ExpressionAtomContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_expressionAtom;
    }

    public ExpressionAtomContext() {}

    public void copyFrom(ExpressionAtomContext ctx) {
      super.copyFrom(ctx);
    }
  }

  public static class UnaryExpressionAtomContext extends ExpressionAtomContext {
    public UnaryOperatorContext unaryOperator() {
      return getRuleContext(UnaryOperatorContext.class, 0);
    }

    public ExpressionAtomContext expressionAtom() {
      return getRuleContext(ExpressionAtomContext.class, 0);
    }

    public UnaryExpressionAtomContext(ExpressionAtomContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public void enterRule(ParseTreeListener listener) {
      if (listener instanceof MySqlParserListener)
        ((MySqlParserListener) listener).enterUnaryExpressionAtom(this);
    }

    @Override
    public void exitRule(ParseTreeListener listener) {
      if (listener instanceof MySqlParserListener)
        ((MySqlParserListener) listener).exitUnaryExpressionAtom(this);
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof MySqlParserVisitor)
        return ((MySqlParserVisitor<? extends T>) visitor).visitUnaryExpressionAtom(this);
      else return visitor.visitChildren(this);
    }
  }

  public static class CollateExpressionAtomContext extends ExpressionAtomContext {
    public ExpressionAtomContext expressionAtom() {
      return getRuleContext(ExpressionAtomContext.class, 0);
    }

    public TerminalNode COLLATE() {
      return getToken(MySqlParser.COLLATE, 0);
    }

    public CollationNameContext collationName() {
      return getRuleContext(CollationNameContext.class, 0);
    }

    public CollateExpressionAtomContext(ExpressionAtomContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public void enterRule(ParseTreeListener listener) {
      if (listener instanceof MySqlParserListener)
        ((MySqlParserListener) listener).enterCollateExpressionAtom(this);
    }

    @Override
    public void exitRule(ParseTreeListener listener) {
      if (listener instanceof MySqlParserListener)
        ((MySqlParserListener) listener).exitCollateExpressionAtom(this);
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof MySqlParserVisitor)
        return ((MySqlParserVisitor<? extends T>) visitor).visitCollateExpressionAtom(this);
      else return visitor.visitChildren(this);
    }
  }

  public static class ConstantExpressionAtomContext extends ExpressionAtomContext {
    public ConstantContext constant() {
      return getRuleContext(ConstantContext.class, 0);
    }

    public ConstantExpressionAtomContext(ExpressionAtomContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public void enterRule(ParseTreeListener listener) {
      if (listener instanceof MySqlParserListener)
        ((MySqlParserListener) listener).enterConstantExpressionAtom(this);
    }

    @Override
    public void exitRule(ParseTreeListener listener) {
      if (listener instanceof MySqlParserListener)
        ((MySqlParserListener) listener).exitConstantExpressionAtom(this);
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof MySqlParserVisitor)
        return ((MySqlParserVisitor<? extends T>) visitor).visitConstantExpressionAtom(this);
      else return visitor.visitChildren(this);
    }
  }

  public static class FunctionCallExpressionAtomContext extends ExpressionAtomContext {
    public FunctionCallContext functionCall() {
      return getRuleContext(FunctionCallContext.class, 0);
    }

    public FunctionCallExpressionAtomContext(ExpressionAtomContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public void enterRule(ParseTreeListener listener) {
      if (listener instanceof MySqlParserListener)
        ((MySqlParserListener) listener).enterFunctionCallExpressionAtom(this);
    }

    @Override
    public void exitRule(ParseTreeListener listener) {
      if (listener instanceof MySqlParserListener)
        ((MySqlParserListener) listener).exitFunctionCallExpressionAtom(this);
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof MySqlParserVisitor)
        return ((MySqlParserVisitor<? extends T>) visitor).visitFunctionCallExpressionAtom(this);
      else return visitor.visitChildren(this);
    }
  }

  public static class BinaryExpressionAtomContext extends ExpressionAtomContext {
    public TerminalNode BINARY() {
      return getToken(MySqlParser.BINARY, 0);
    }

    public ExpressionAtomContext expressionAtom() {
      return getRuleContext(ExpressionAtomContext.class, 0);
    }

    public BinaryExpressionAtomContext(ExpressionAtomContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public void enterRule(ParseTreeListener listener) {
      if (listener instanceof MySqlParserListener)
        ((MySqlParserListener) listener).enterBinaryExpressionAtom(this);
    }

    @Override
    public void exitRule(ParseTreeListener listener) {
      if (listener instanceof MySqlParserListener)
        ((MySqlParserListener) listener).exitBinaryExpressionAtom(this);
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof MySqlParserVisitor)
        return ((MySqlParserVisitor<? extends T>) visitor).visitBinaryExpressionAtom(this);
      else return visitor.visitChildren(this);
    }
  }

  public static class FullColumnNameExpressionAtomContext extends ExpressionAtomContext {
    public FullColumnNameContext fullColumnName() {
      return getRuleContext(FullColumnNameContext.class, 0);
    }

    public FullColumnNameExpressionAtomContext(ExpressionAtomContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public void enterRule(ParseTreeListener listener) {
      if (listener instanceof MySqlParserListener)
        ((MySqlParserListener) listener).enterFullColumnNameExpressionAtom(this);
    }

    @Override
    public void exitRule(ParseTreeListener listener) {
      if (listener instanceof MySqlParserListener)
        ((MySqlParserListener) listener).exitFullColumnNameExpressionAtom(this);
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof MySqlParserVisitor)
        return ((MySqlParserVisitor<? extends T>) visitor).visitFullColumnNameExpressionAtom(this);
      else return visitor.visitChildren(this);
    }
  }

  public static class BitExpressionAtomContext extends ExpressionAtomContext {
    public ExpressionAtomContext left;
    public ExpressionAtomContext right;

    public BitOperatorContext bitOperator() {
      return getRuleContext(BitOperatorContext.class, 0);
    }

    public List<ExpressionAtomContext> expressionAtom() {
      return getRuleContexts(ExpressionAtomContext.class);
    }

    public ExpressionAtomContext expressionAtom(int i) {
      return getRuleContext(ExpressionAtomContext.class, i);
    }

    public BitExpressionAtomContext(ExpressionAtomContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public void enterRule(ParseTreeListener listener) {
      if (listener instanceof MySqlParserListener)
        ((MySqlParserListener) listener).enterBitExpressionAtom(this);
    }

    @Override
    public void exitRule(ParseTreeListener listener) {
      if (listener instanceof MySqlParserListener)
        ((MySqlParserListener) listener).exitBitExpressionAtom(this);
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof MySqlParserVisitor)
        return ((MySqlParserVisitor<? extends T>) visitor).visitBitExpressionAtom(this);
      else return visitor.visitChildren(this);
    }
  }

  public static class NestedExpressionAtomContext extends ExpressionAtomContext {
    public List<ExpressionContext> expression() {
      return getRuleContexts(ExpressionContext.class);
    }

    public ExpressionContext expression(int i) {
      return getRuleContext(ExpressionContext.class, i);
    }

    public NestedExpressionAtomContext(ExpressionAtomContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public void enterRule(ParseTreeListener listener) {
      if (listener instanceof MySqlParserListener)
        ((MySqlParserListener) listener).enterNestedExpressionAtom(this);
    }

    @Override
    public void exitRule(ParseTreeListener listener) {
      if (listener instanceof MySqlParserListener)
        ((MySqlParserListener) listener).exitNestedExpressionAtom(this);
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof MySqlParserVisitor)
        return ((MySqlParserVisitor<? extends T>) visitor).visitNestedExpressionAtom(this);
      else return visitor.visitChildren(this);
    }
  }

  public static class NestedRowExpressionAtomContext extends ExpressionAtomContext {
    public TerminalNode ROW() {
      return getToken(MySqlParser.ROW, 0);
    }

    public List<ExpressionContext> expression() {
      return getRuleContexts(ExpressionContext.class);
    }

    public ExpressionContext expression(int i) {
      return getRuleContext(ExpressionContext.class, i);
    }

    public NestedRowExpressionAtomContext(ExpressionAtomContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public void enterRule(ParseTreeListener listener) {
      if (listener instanceof MySqlParserListener)
        ((MySqlParserListener) listener).enterNestedRowExpressionAtom(this);
    }

    @Override
    public void exitRule(ParseTreeListener listener) {
      if (listener instanceof MySqlParserListener)
        ((MySqlParserListener) listener).exitNestedRowExpressionAtom(this);
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof MySqlParserVisitor)
        return ((MySqlParserVisitor<? extends T>) visitor).visitNestedRowExpressionAtom(this);
      else return visitor.visitChildren(this);
    }
  }

  public static class MathExpressionAtomContext extends ExpressionAtomContext {
    public ExpressionAtomContext left;
    public ExpressionAtomContext right;

    public MathOperatorContext mathOperator() {
      return getRuleContext(MathOperatorContext.class, 0);
    }

    public List<ExpressionAtomContext> expressionAtom() {
      return getRuleContexts(ExpressionAtomContext.class);
    }

    public ExpressionAtomContext expressionAtom(int i) {
      return getRuleContext(ExpressionAtomContext.class, i);
    }

    public MathExpressionAtomContext(ExpressionAtomContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public void enterRule(ParseTreeListener listener) {
      if (listener instanceof MySqlParserListener)
        ((MySqlParserListener) listener).enterMathExpressionAtom(this);
    }

    @Override
    public void exitRule(ParseTreeListener listener) {
      if (listener instanceof MySqlParserListener)
        ((MySqlParserListener) listener).exitMathExpressionAtom(this);
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof MySqlParserVisitor)
        return ((MySqlParserVisitor<? extends T>) visitor).visitMathExpressionAtom(this);
      else return visitor.visitChildren(this);
    }
  }

  public static class IntervalExpressionAtomContext extends ExpressionAtomContext {
    public TerminalNode INTERVAL() {
      return getToken(MySqlParser.INTERVAL, 0);
    }

    public ExpressionContext expression() {
      return getRuleContext(ExpressionContext.class, 0);
    }

    public IntervalTypeContext intervalType() {
      return getRuleContext(IntervalTypeContext.class, 0);
    }

    public IntervalExpressionAtomContext(ExpressionAtomContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public void enterRule(ParseTreeListener listener) {
      if (listener instanceof MySqlParserListener)
        ((MySqlParserListener) listener).enterIntervalExpressionAtom(this);
    }

    @Override
    public void exitRule(ParseTreeListener listener) {
      if (listener instanceof MySqlParserListener)
        ((MySqlParserListener) listener).exitIntervalExpressionAtom(this);
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof MySqlParserVisitor)
        return ((MySqlParserVisitor<? extends T>) visitor).visitIntervalExpressionAtom(this);
      else return visitor.visitChildren(this);
    }
  }

  public final ExpressionAtomContext expressionAtom() throws RecognitionException {
    return expressionAtom(0);
  }

  private ExpressionAtomContext expressionAtom(int _p) throws RecognitionException {
    ParserRuleContext _parentctx = _ctx;
    int _parentState = getState();
    ExpressionAtomContext _localctx = new ExpressionAtomContext(_ctx, _parentState);
    ExpressionAtomContext _prevctx = _localctx;
    int _startState = 58;
    enterRecursionRule(_localctx, 58, RULE_expressionAtom, _p);
    int _la;
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      {
        setState(591);
        _errHandler.sync(this);
        switch (getInterpreter().adaptivePredict(_input, 69, _ctx)) {
          case 1:
            {
              _localctx = new ConstantExpressionAtomContext(_localctx);
              _ctx = _localctx;
              _prevctx = _localctx;

              setState(557);
              constant();
            }
            break;
          case 2:
            {
              _localctx = new FullColumnNameExpressionAtomContext(_localctx);
              _ctx = _localctx;
              _prevctx = _localctx;
              setState(558);
              fullColumnName();
            }
            break;
          case 3:
            {
              _localctx = new FunctionCallExpressionAtomContext(_localctx);
              _ctx = _localctx;
              _prevctx = _localctx;
              setState(559);
              functionCall();
            }
            break;
          case 4:
            {
              _localctx = new UnaryExpressionAtomContext(_localctx);
              _ctx = _localctx;
              _prevctx = _localctx;
              setState(560);
              unaryOperator();
              setState(561);
              expressionAtom(7);
            }
            break;
          case 5:
            {
              _localctx = new BinaryExpressionAtomContext(_localctx);
              _ctx = _localctx;
              _prevctx = _localctx;
              setState(563);
              match(BINARY);
              setState(564);
              expressionAtom(6);
            }
            break;
          case 6:
            {
              _localctx = new NestedExpressionAtomContext(_localctx);
              _ctx = _localctx;
              _prevctx = _localctx;
              setState(565);
              match(LR_BRACKET);
              setState(566);
              expression(0);
              setState(571);
              _errHandler.sync(this);
              _la = _input.LA(1);
              while (_la == COMMA) {
                {
                  {
                    setState(567);
                    match(COMMA);
                    setState(568);
                    expression(0);
                  }
                }
                setState(573);
                _errHandler.sync(this);
                _la = _input.LA(1);
              }
              setState(574);
              match(RR_BRACKET);
            }
            break;
          case 7:
            {
              _localctx = new NestedRowExpressionAtomContext(_localctx);
              _ctx = _localctx;
              _prevctx = _localctx;
              setState(576);
              match(ROW);
              setState(577);
              match(LR_BRACKET);
              setState(578);
              expression(0);
              setState(581);
              _errHandler.sync(this);
              _la = _input.LA(1);
              do {
                {
                  {
                    setState(579);
                    match(COMMA);
                    setState(580);
                    expression(0);
                  }
                }
                setState(583);
                _errHandler.sync(this);
                _la = _input.LA(1);
              } while (_la == COMMA);
              setState(585);
              match(RR_BRACKET);
            }
            break;
          case 8:
            {
              _localctx = new IntervalExpressionAtomContext(_localctx);
              _ctx = _localctx;
              _prevctx = _localctx;
              setState(587);
              match(INTERVAL);
              setState(588);
              expression(0);
              setState(589);
              intervalType();
            }
            break;
        }
        _ctx.stop = _input.LT(-1);
        setState(606);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input, 71, _ctx);
        while (_alt != 2 && _alt != org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER) {
          if (_alt == 1) {
            if (_parseListeners != null) triggerExitRuleEvent();
            _prevctx = _localctx;
            {
              setState(604);
              _errHandler.sync(this);
              switch (getInterpreter().adaptivePredict(_input, 70, _ctx)) {
                case 1:
                  {
                    _localctx =
                        new BitExpressionAtomContext(
                            new ExpressionAtomContext(_parentctx, _parentState));
                    ((BitExpressionAtomContext) _localctx).left = _prevctx;
                    pushNewRecursionContext(_localctx, _startState, RULE_expressionAtom);
                    setState(593);
                    if (!(precpred(_ctx, 2)))
                      throw new FailedPredicateException(this, "precpred(_ctx, 2)");
                    setState(594);
                    bitOperator();
                    setState(595);
                    ((BitExpressionAtomContext) _localctx).right = expressionAtom(3);
                  }
                  break;
                case 2:
                  {
                    _localctx =
                        new MathExpressionAtomContext(
                            new ExpressionAtomContext(_parentctx, _parentState));
                    ((MathExpressionAtomContext) _localctx).left = _prevctx;
                    pushNewRecursionContext(_localctx, _startState, RULE_expressionAtom);
                    setState(597);
                    if (!(precpred(_ctx, 1)))
                      throw new FailedPredicateException(this, "precpred(_ctx, 1)");
                    setState(598);
                    mathOperator();
                    setState(599);
                    ((MathExpressionAtomContext) _localctx).right = expressionAtom(2);
                  }
                  break;
                case 3:
                  {
                    _localctx =
                        new CollateExpressionAtomContext(
                            new ExpressionAtomContext(_parentctx, _parentState));
                    pushNewRecursionContext(_localctx, _startState, RULE_expressionAtom);
                    setState(601);
                    if (!(precpred(_ctx, 8)))
                      throw new FailedPredicateException(this, "precpred(_ctx, 8)");
                    setState(602);
                    match(COLLATE);
                    setState(603);
                    collationName();
                  }
                  break;
              }
            }
          }
          setState(608);
          _errHandler.sync(this);
          _alt = getInterpreter().adaptivePredict(_input, 71, _ctx);
        }
      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      unrollRecursionContexts(_parentctx);
    }
    return _localctx;
  }

  public static class UnaryOperatorContext extends ParserRuleContext {
    public TerminalNode NOT() {
      return getToken(MySqlParser.NOT, 0);
    }

    public UnaryOperatorContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_unaryOperator;
    }

    @Override
    public void enterRule(ParseTreeListener listener) {
      if (listener instanceof MySqlParserListener)
        ((MySqlParserListener) listener).enterUnaryOperator(this);
    }

    @Override
    public void exitRule(ParseTreeListener listener) {
      if (listener instanceof MySqlParserListener)
        ((MySqlParserListener) listener).exitUnaryOperator(this);
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof MySqlParserVisitor)
        return ((MySqlParserVisitor<? extends T>) visitor).visitUnaryOperator(this);
      else return visitor.visitChildren(this);
    }
  }

  public final UnaryOperatorContext unaryOperator() throws RecognitionException {
    UnaryOperatorContext _localctx = new UnaryOperatorContext(_ctx, getState());
    enterRule(_localctx, 60, RULE_unaryOperator);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
        setState(609);
        _la = _input.LA(1);
        if (!(_la == NOT
            || ((((_la - 935)) & ~0x3f) == 0
                && ((1L << (_la - 935))
                        & ((1L << (PLUS - 935))
                            | (1L << (MINUS - 935))
                            | (1L << (EXCLAMATION_SYMBOL - 935))
                            | (1L << (BIT_NOT_OP - 935))))
                    != 0))) {
          _errHandler.recoverInline(this);
        } else {
          if (_input.LA(1) == Token.EOF) matchedEOF = true;
          _errHandler.reportMatch(this);
          consume();
        }
      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  public static class ComparisonOperatorContext extends ParserRuleContext {
    public ComparisonOperatorContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_comparisonOperator;
    }

    @Override
    public void enterRule(ParseTreeListener listener) {
      if (listener instanceof MySqlParserListener)
        ((MySqlParserListener) listener).enterComparisonOperator(this);
    }

    @Override
    public void exitRule(ParseTreeListener listener) {
      if (listener instanceof MySqlParserListener)
        ((MySqlParserListener) listener).exitComparisonOperator(this);
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof MySqlParserVisitor)
        return ((MySqlParserVisitor<? extends T>) visitor).visitComparisonOperator(this);
      else return visitor.visitChildren(this);
    }
  }

  public final ComparisonOperatorContext comparisonOperator() throws RecognitionException {
    ComparisonOperatorContext _localctx = new ComparisonOperatorContext(_ctx, getState());
    enterRule(_localctx, 62, RULE_comparisonOperator);
    try {
      setState(625);
      _errHandler.sync(this);
      switch (getInterpreter().adaptivePredict(_input, 72, _ctx)) {
        case 1:
          enterOuterAlt(_localctx, 1);
          {
            setState(611);
            match(EQUAL_SYMBOL);
          }
          break;
        case 2:
          enterOuterAlt(_localctx, 2);
          {
            setState(612);
            match(GREATER_SYMBOL);
          }
          break;
        case 3:
          enterOuterAlt(_localctx, 3);
          {
            setState(613);
            match(LESS_SYMBOL);
          }
          break;
        case 4:
          enterOuterAlt(_localctx, 4);
          {
            setState(614);
            match(LESS_SYMBOL);
            setState(615);
            match(EQUAL_SYMBOL);
          }
          break;
        case 5:
          enterOuterAlt(_localctx, 5);
          {
            setState(616);
            match(GREATER_SYMBOL);
            setState(617);
            match(EQUAL_SYMBOL);
          }
          break;
        case 6:
          enterOuterAlt(_localctx, 6);
          {
            setState(618);
            match(LESS_SYMBOL);
            setState(619);
            match(GREATER_SYMBOL);
          }
          break;
        case 7:
          enterOuterAlt(_localctx, 7);
          {
            setState(620);
            match(EXCLAMATION_SYMBOL);
            setState(621);
            match(EQUAL_SYMBOL);
          }
          break;
        case 8:
          enterOuterAlt(_localctx, 8);
          {
            setState(622);
            match(LESS_SYMBOL);
            setState(623);
            match(EQUAL_SYMBOL);
            setState(624);
            match(GREATER_SYMBOL);
          }
          break;
      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  public static class LogicalOperatorContext extends ParserRuleContext {
    public TerminalNode AND() {
      return getToken(MySqlParser.AND, 0);
    }

    public TerminalNode XOR() {
      return getToken(MySqlParser.XOR, 0);
    }

    public TerminalNode OR() {
      return getToken(MySqlParser.OR, 0);
    }

    public LogicalOperatorContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_logicalOperator;
    }

    @Override
    public void enterRule(ParseTreeListener listener) {
      if (listener instanceof MySqlParserListener)
        ((MySqlParserListener) listener).enterLogicalOperator(this);
    }

    @Override
    public void exitRule(ParseTreeListener listener) {
      if (listener instanceof MySqlParserListener)
        ((MySqlParserListener) listener).exitLogicalOperator(this);
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof MySqlParserVisitor)
        return ((MySqlParserVisitor<? extends T>) visitor).visitLogicalOperator(this);
      else return visitor.visitChildren(this);
    }
  }

  public final LogicalOperatorContext logicalOperator() throws RecognitionException {
    LogicalOperatorContext _localctx = new LogicalOperatorContext(_ctx, getState());
    enterRule(_localctx, 64, RULE_logicalOperator);
    try {
      setState(634);
      _errHandler.sync(this);
      switch (_input.LA(1)) {
        case AND:
          enterOuterAlt(_localctx, 1);
          {
            setState(627);
            match(AND);
          }
          break;
        case BIT_AND_OP:
          enterOuterAlt(_localctx, 2);
          {
            setState(628);
            match(BIT_AND_OP);
            setState(629);
            match(BIT_AND_OP);
          }
          break;
        case XOR:
          enterOuterAlt(_localctx, 3);
          {
            setState(630);
            match(XOR);
          }
          break;
        case OR:
          enterOuterAlt(_localctx, 4);
          {
            setState(631);
            match(OR);
          }
          break;
        case BIT_OR_OP:
          enterOuterAlt(_localctx, 5);
          {
            setState(632);
            match(BIT_OR_OP);
            setState(633);
            match(BIT_OR_OP);
          }
          break;
        default:
          throw new NoViableAltException(this);
      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  public static class BitOperatorContext extends ParserRuleContext {
    public BitOperatorContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_bitOperator;
    }

    @Override
    public void enterRule(ParseTreeListener listener) {
      if (listener instanceof MySqlParserListener)
        ((MySqlParserListener) listener).enterBitOperator(this);
    }

    @Override
    public void exitRule(ParseTreeListener listener) {
      if (listener instanceof MySqlParserListener)
        ((MySqlParserListener) listener).exitBitOperator(this);
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof MySqlParserVisitor)
        return ((MySqlParserVisitor<? extends T>) visitor).visitBitOperator(this);
      else return visitor.visitChildren(this);
    }
  }

  public final BitOperatorContext bitOperator() throws RecognitionException {
    BitOperatorContext _localctx = new BitOperatorContext(_ctx, getState());
    enterRule(_localctx, 66, RULE_bitOperator);
    try {
      setState(643);
      _errHandler.sync(this);
      switch (_input.LA(1)) {
        case LESS_SYMBOL:
          enterOuterAlt(_localctx, 1);
          {
            setState(636);
            match(LESS_SYMBOL);
            setState(637);
            match(LESS_SYMBOL);
          }
          break;
        case GREATER_SYMBOL:
          enterOuterAlt(_localctx, 2);
          {
            setState(638);
            match(GREATER_SYMBOL);
            setState(639);
            match(GREATER_SYMBOL);
          }
          break;
        case BIT_AND_OP:
          enterOuterAlt(_localctx, 3);
          {
            setState(640);
            match(BIT_AND_OP);
          }
          break;
        case BIT_XOR_OP:
          enterOuterAlt(_localctx, 4);
          {
            setState(641);
            match(BIT_XOR_OP);
          }
          break;
        case BIT_OR_OP:
          enterOuterAlt(_localctx, 5);
          {
            setState(642);
            match(BIT_OR_OP);
          }
          break;
        default:
          throw new NoViableAltException(this);
      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  public static class MathOperatorContext extends ParserRuleContext {
    public TerminalNode DIV() {
      return getToken(MySqlParser.DIV, 0);
    }

    public TerminalNode MOD() {
      return getToken(MySqlParser.MOD, 0);
    }

    public MathOperatorContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_mathOperator;
    }

    @Override
    public void enterRule(ParseTreeListener listener) {
      if (listener instanceof MySqlParserListener)
        ((MySqlParserListener) listener).enterMathOperator(this);
    }

    @Override
    public void exitRule(ParseTreeListener listener) {
      if (listener instanceof MySqlParserListener)
        ((MySqlParserListener) listener).exitMathOperator(this);
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof MySqlParserVisitor)
        return ((MySqlParserVisitor<? extends T>) visitor).visitMathOperator(this);
      else return visitor.visitChildren(this);
    }
  }

  public final MathOperatorContext mathOperator() throws RecognitionException {
    MathOperatorContext _localctx = new MathOperatorContext(_ctx, getState());
    enterRule(_localctx, 68, RULE_mathOperator);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
        setState(645);
        _la = _input.LA(1);
        if (!(((((_la - 932)) & ~0x3f) == 0
            && ((1L << (_la - 932))
                    & ((1L << (STAR - 932))
                        | (1L << (DIVIDE - 932))
                        | (1L << (MODULE - 932))
                        | (1L << (PLUS - 932))
                        | (1L << (MINUSMINUS - 932))
                        | (1L << (MINUS - 932))
                        | (1L << (DIV - 932))
                        | (1L << (MOD - 932))))
                != 0))) {
          _errHandler.recoverInline(this);
        } else {
          if (_input.LA(1) == Token.EOF) matchedEOF = true;
          _errHandler.reportMatch(this);
          consume();
        }
      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  public static class CharsetNameBaseContext extends ParserRuleContext {
    public TerminalNode ARMSCII8() {
      return getToken(MySqlParser.ARMSCII8, 0);
    }

    public TerminalNode ASCII() {
      return getToken(MySqlParser.ASCII, 0);
    }

    public TerminalNode BIG5() {
      return getToken(MySqlParser.BIG5, 0);
    }

    public TerminalNode CP1250() {
      return getToken(MySqlParser.CP1250, 0);
    }

    public TerminalNode CP1251() {
      return getToken(MySqlParser.CP1251, 0);
    }

    public TerminalNode CP1256() {
      return getToken(MySqlParser.CP1256, 0);
    }

    public TerminalNode CP1257() {
      return getToken(MySqlParser.CP1257, 0);
    }

    public TerminalNode CP850() {
      return getToken(MySqlParser.CP850, 0);
    }

    public TerminalNode CP852() {
      return getToken(MySqlParser.CP852, 0);
    }

    public TerminalNode CP866() {
      return getToken(MySqlParser.CP866, 0);
    }

    public TerminalNode CP932() {
      return getToken(MySqlParser.CP932, 0);
    }

    public TerminalNode DEC8() {
      return getToken(MySqlParser.DEC8, 0);
    }

    public TerminalNode EUCJPMS() {
      return getToken(MySqlParser.EUCJPMS, 0);
    }

    public TerminalNode EUCKR() {
      return getToken(MySqlParser.EUCKR, 0);
    }

    public TerminalNode GB2312() {
      return getToken(MySqlParser.GB2312, 0);
    }

    public TerminalNode GBK() {
      return getToken(MySqlParser.GBK, 0);
    }

    public TerminalNode GEOSTD8() {
      return getToken(MySqlParser.GEOSTD8, 0);
    }

    public TerminalNode GREEK() {
      return getToken(MySqlParser.GREEK, 0);
    }

    public TerminalNode HEBREW() {
      return getToken(MySqlParser.HEBREW, 0);
    }

    public TerminalNode HP8() {
      return getToken(MySqlParser.HP8, 0);
    }

    public TerminalNode KEYBCS2() {
      return getToken(MySqlParser.KEYBCS2, 0);
    }

    public TerminalNode KOI8R() {
      return getToken(MySqlParser.KOI8R, 0);
    }

    public TerminalNode KOI8U() {
      return getToken(MySqlParser.KOI8U, 0);
    }

    public TerminalNode LATIN1() {
      return getToken(MySqlParser.LATIN1, 0);
    }

    public TerminalNode LATIN2() {
      return getToken(MySqlParser.LATIN2, 0);
    }

    public TerminalNode LATIN5() {
      return getToken(MySqlParser.LATIN5, 0);
    }

    public TerminalNode LATIN7() {
      return getToken(MySqlParser.LATIN7, 0);
    }

    public TerminalNode MACCE() {
      return getToken(MySqlParser.MACCE, 0);
    }

    public TerminalNode MACROMAN() {
      return getToken(MySqlParser.MACROMAN, 0);
    }

    public TerminalNode SJIS() {
      return getToken(MySqlParser.SJIS, 0);
    }

    public TerminalNode SWE7() {
      return getToken(MySqlParser.SWE7, 0);
    }

    public TerminalNode TIS620() {
      return getToken(MySqlParser.TIS620, 0);
    }

    public TerminalNode UCS2() {
      return getToken(MySqlParser.UCS2, 0);
    }

    public TerminalNode UJIS() {
      return getToken(MySqlParser.UJIS, 0);
    }

    public TerminalNode UTF16() {
      return getToken(MySqlParser.UTF16, 0);
    }

    public TerminalNode UTF16LE() {
      return getToken(MySqlParser.UTF16LE, 0);
    }

    public TerminalNode UTF32() {
      return getToken(MySqlParser.UTF32, 0);
    }

    public TerminalNode UTF8() {
      return getToken(MySqlParser.UTF8, 0);
    }

    public TerminalNode UTF8MB3() {
      return getToken(MySqlParser.UTF8MB3, 0);
    }

    public TerminalNode UTF8MB4() {
      return getToken(MySqlParser.UTF8MB4, 0);
    }

    public CharsetNameBaseContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_charsetNameBase;
    }

    @Override
    public void enterRule(ParseTreeListener listener) {
      if (listener instanceof MySqlParserListener)
        ((MySqlParserListener) listener).enterCharsetNameBase(this);
    }

    @Override
    public void exitRule(ParseTreeListener listener) {
      if (listener instanceof MySqlParserListener)
        ((MySqlParserListener) listener).exitCharsetNameBase(this);
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof MySqlParserVisitor)
        return ((MySqlParserVisitor<? extends T>) visitor).visitCharsetNameBase(this);
      else return visitor.visitChildren(this);
    }
  }

  public final CharsetNameBaseContext charsetNameBase() throws RecognitionException {
    CharsetNameBaseContext _localctx = new CharsetNameBaseContext(_ctx, getState());
    enterRule(_localctx, 70, RULE_charsetNameBase);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
        setState(647);
        _la = _input.LA(1);
        if (!(((((_la - 571)) & ~0x3f) == 0
            && ((1L << (_la - 571))
                    & ((1L << (ARMSCII8 - 571))
                        | (1L << (ASCII - 571))
                        | (1L << (BIG5 - 571))
                        | (1L << (CP1250 - 571))
                        | (1L << (CP1251 - 571))
                        | (1L << (CP1256 - 571))
                        | (1L << (CP1257 - 571))
                        | (1L << (CP850 - 571))
                        | (1L << (CP852 - 571))
                        | (1L << (CP866 - 571))
                        | (1L << (CP932 - 571))
                        | (1L << (DEC8 - 571))
                        | (1L << (EUCJPMS - 571))
                        | (1L << (EUCKR - 571))
                        | (1L << (GB2312 - 571))
                        | (1L << (GBK - 571))
                        | (1L << (GEOSTD8 - 571))
                        | (1L << (GREEK - 571))
                        | (1L << (HEBREW - 571))
                        | (1L << (HP8 - 571))
                        | (1L << (KEYBCS2 - 571))
                        | (1L << (KOI8R - 571))
                        | (1L << (KOI8U - 571))
                        | (1L << (LATIN1 - 571))
                        | (1L << (LATIN2 - 571))
                        | (1L << (LATIN5 - 571))
                        | (1L << (LATIN7 - 571))
                        | (1L << (MACCE - 571))
                        | (1L << (MACROMAN - 571))
                        | (1L << (SJIS - 571))
                        | (1L << (SWE7 - 571))
                        | (1L << (TIS620 - 571))
                        | (1L << (UCS2 - 571))
                        | (1L << (UJIS - 571))
                        | (1L << (UTF16 - 571))
                        | (1L << (UTF16LE - 571))
                        | (1L << (UTF32 - 571))
                        | (1L << (UTF8 - 571))
                        | (1L << (UTF8MB3 - 571))
                        | (1L << (UTF8MB4 - 571))))
                != 0))) {
          _errHandler.recoverInline(this);
        } else {
          if (_input.LA(1) == Token.EOF) matchedEOF = true;
          _errHandler.reportMatch(this);
          consume();
        }
      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  public static class IntervalTypeBaseContext extends ParserRuleContext {
    public TerminalNode QUARTER() {
      return getToken(MySqlParser.QUARTER, 0);
    }

    public TerminalNode MONTH() {
      return getToken(MySqlParser.MONTH, 0);
    }

    public TerminalNode DAY() {
      return getToken(MySqlParser.DAY, 0);
    }

    public TerminalNode HOUR() {
      return getToken(MySqlParser.HOUR, 0);
    }

    public TerminalNode MINUTE() {
      return getToken(MySqlParser.MINUTE, 0);
    }

    public TerminalNode WEEK() {
      return getToken(MySqlParser.WEEK, 0);
    }

    public TerminalNode SECOND() {
      return getToken(MySqlParser.SECOND, 0);
    }

    public TerminalNode MICROSECOND() {
      return getToken(MySqlParser.MICROSECOND, 0);
    }

    public IntervalTypeBaseContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_intervalTypeBase;
    }

    @Override
    public void enterRule(ParseTreeListener listener) {
      if (listener instanceof MySqlParserListener)
        ((MySqlParserListener) listener).enterIntervalTypeBase(this);
    }

    @Override
    public void exitRule(ParseTreeListener listener) {
      if (listener instanceof MySqlParserListener)
        ((MySqlParserListener) listener).exitIntervalTypeBase(this);
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof MySqlParserVisitor)
        return ((MySqlParserVisitor<? extends T>) visitor).visitIntervalTypeBase(this);
      else return visitor.visitChildren(this);
    }
  }

  public final IntervalTypeBaseContext intervalTypeBase() throws RecognitionException {
    IntervalTypeBaseContext _localctx = new IntervalTypeBaseContext(_ctx, getState());
    enterRule(_localctx, 72, RULE_intervalTypeBase);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
        setState(649);
        _la = _input.LA(1);
        if (!(((((_la - 554)) & ~0x3f) == 0
            && ((1L << (_la - 554))
                    & ((1L << (QUARTER - 554))
                        | (1L << (MONTH - 554))
                        | (1L << (DAY - 554))
                        | (1L << (HOUR - 554))
                        | (1L << (MINUTE - 554))
                        | (1L << (WEEK - 554))
                        | (1L << (SECOND - 554))
                        | (1L << (MICROSECOND - 554))))
                != 0))) {
          _errHandler.recoverInline(this);
        } else {
          if (_input.LA(1) == Token.EOF) matchedEOF = true;
          _errHandler.reportMatch(this);
          consume();
        }
      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  public static class DataTypeBaseContext extends ParserRuleContext {
    public TerminalNode DATE() {
      return getToken(MySqlParser.DATE, 0);
    }

    public TerminalNode TIME() {
      return getToken(MySqlParser.TIME, 0);
    }

    public TerminalNode TIMESTAMP() {
      return getToken(MySqlParser.TIMESTAMP, 0);
    }

    public TerminalNode DATETIME() {
      return getToken(MySqlParser.DATETIME, 0);
    }

    public TerminalNode YEAR() {
      return getToken(MySqlParser.YEAR, 0);
    }

    public TerminalNode ENUM() {
      return getToken(MySqlParser.ENUM, 0);
    }

    public TerminalNode TEXT() {
      return getToken(MySqlParser.TEXT, 0);
    }

    public DataTypeBaseContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_dataTypeBase;
    }

    @Override
    public void enterRule(ParseTreeListener listener) {
      if (listener instanceof MySqlParserListener)
        ((MySqlParserListener) listener).enterDataTypeBase(this);
    }

    @Override
    public void exitRule(ParseTreeListener listener) {
      if (listener instanceof MySqlParserListener)
        ((MySqlParserListener) listener).exitDataTypeBase(this);
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof MySqlParserVisitor)
        return ((MySqlParserVisitor<? extends T>) visitor).visitDataTypeBase(this);
      else return visitor.visitChildren(this);
    }
  }

  public final DataTypeBaseContext dataTypeBase() throws RecognitionException {
    DataTypeBaseContext _localctx = new DataTypeBaseContext(_ctx, getState());
    enterRule(_localctx, 74, RULE_dataTypeBase);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
        setState(651);
        _la = _input.LA(1);
        if (!(((((_la - 182)) & ~0x3f) == 0
            && ((1L << (_la - 182))
                    & ((1L << (DATE - 182))
                        | (1L << (TIME - 182))
                        | (1L << (TIMESTAMP - 182))
                        | (1L << (DATETIME - 182))
                        | (1L << (YEAR - 182))
                        | (1L << (TEXT - 182))
                        | (1L << (ENUM - 182))))
                != 0))) {
          _errHandler.recoverInline(this);
        } else {
          if (_input.LA(1) == Token.EOF) matchedEOF = true;
          _errHandler.reportMatch(this);
          consume();
        }
      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  public static class KeywordsCanBeIdContext extends ParserRuleContext {
    public TerminalNode ACCOUNT() {
      return getToken(MySqlParser.ACCOUNT, 0);
    }

    public TerminalNode ACTION() {
      return getToken(MySqlParser.ACTION, 0);
    }

    public TerminalNode AFTER() {
      return getToken(MySqlParser.AFTER, 0);
    }

    public TerminalNode AGGREGATE() {
      return getToken(MySqlParser.AGGREGATE, 0);
    }

    public TerminalNode ALGORITHM() {
      return getToken(MySqlParser.ALGORITHM, 0);
    }

    public TerminalNode ANY() {
      return getToken(MySqlParser.ANY, 0);
    }

    public TerminalNode AT() {
      return getToken(MySqlParser.AT, 0);
    }

    public TerminalNode AUTHORS() {
      return getToken(MySqlParser.AUTHORS, 0);
    }

    public TerminalNode AUTOCOMMIT() {
      return getToken(MySqlParser.AUTOCOMMIT, 0);
    }

    public TerminalNode AUTOEXTEND_SIZE() {
      return getToken(MySqlParser.AUTOEXTEND_SIZE, 0);
    }

    public TerminalNode AUTO_INCREMENT() {
      return getToken(MySqlParser.AUTO_INCREMENT, 0);
    }

    public TerminalNode AVG_ROW_LENGTH() {
      return getToken(MySqlParser.AVG_ROW_LENGTH, 0);
    }

    public TerminalNode BEGIN() {
      return getToken(MySqlParser.BEGIN, 0);
    }

    public TerminalNode BINLOG() {
      return getToken(MySqlParser.BINLOG, 0);
    }

    public TerminalNode BIT() {
      return getToken(MySqlParser.BIT, 0);
    }

    public TerminalNode BLOCK() {
      return getToken(MySqlParser.BLOCK, 0);
    }

    public TerminalNode BOOL() {
      return getToken(MySqlParser.BOOL, 0);
    }

    public TerminalNode BOOLEAN() {
      return getToken(MySqlParser.BOOLEAN, 0);
    }

    public TerminalNode BTREE() {
      return getToken(MySqlParser.BTREE, 0);
    }

    public TerminalNode CASCADED() {
      return getToken(MySqlParser.CASCADED, 0);
    }

    public TerminalNode CHAIN() {
      return getToken(MySqlParser.CHAIN, 0);
    }

    public TerminalNode CHANGED() {
      return getToken(MySqlParser.CHANGED, 0);
    }

    public TerminalNode CHANNEL() {
      return getToken(MySqlParser.CHANNEL, 0);
    }

    public TerminalNode CHECKSUM() {
      return getToken(MySqlParser.CHECKSUM, 0);
    }

    public TerminalNode CIPHER() {
      return getToken(MySqlParser.CIPHER, 0);
    }

    public TerminalNode CLIENT() {
      return getToken(MySqlParser.CLIENT, 0);
    }

    public TerminalNode COALESCE() {
      return getToken(MySqlParser.COALESCE, 0);
    }

    public TerminalNode CODE() {
      return getToken(MySqlParser.CODE, 0);
    }

    public TerminalNode COLUMNS() {
      return getToken(MySqlParser.COLUMNS, 0);
    }

    public TerminalNode COLUMN_FORMAT() {
      return getToken(MySqlParser.COLUMN_FORMAT, 0);
    }

    public TerminalNode COMMENT() {
      return getToken(MySqlParser.COMMENT, 0);
    }

    public TerminalNode COMMIT() {
      return getToken(MySqlParser.COMMIT, 0);
    }

    public TerminalNode COMPACT() {
      return getToken(MySqlParser.COMPACT, 0);
    }

    public TerminalNode COMPLETION() {
      return getToken(MySqlParser.COMPLETION, 0);
    }

    public TerminalNode COMPRESSED() {
      return getToken(MySqlParser.COMPRESSED, 0);
    }

    public TerminalNode COMPRESSION() {
      return getToken(MySqlParser.COMPRESSION, 0);
    }

    public TerminalNode CONCURRENT() {
      return getToken(MySqlParser.CONCURRENT, 0);
    }

    public TerminalNode CONNECTION() {
      return getToken(MySqlParser.CONNECTION, 0);
    }

    public TerminalNode CONSISTENT() {
      return getToken(MySqlParser.CONSISTENT, 0);
    }

    public TerminalNode CONTAINS() {
      return getToken(MySqlParser.CONTAINS, 0);
    }

    public TerminalNode CONTEXT() {
      return getToken(MySqlParser.CONTEXT, 0);
    }

    public TerminalNode CONTRIBUTORS() {
      return getToken(MySqlParser.CONTRIBUTORS, 0);
    }

    public TerminalNode COPY() {
      return getToken(MySqlParser.COPY, 0);
    }

    public TerminalNode CPU() {
      return getToken(MySqlParser.CPU, 0);
    }

    public TerminalNode DATA() {
      return getToken(MySqlParser.DATA, 0);
    }

    public TerminalNode DATAFILE() {
      return getToken(MySqlParser.DATAFILE, 0);
    }

    public TerminalNode DEALLOCATE() {
      return getToken(MySqlParser.DEALLOCATE, 0);
    }

    public TerminalNode DEFAULT_AUTH() {
      return getToken(MySqlParser.DEFAULT_AUTH, 0);
    }

    public TerminalNode DEFINER() {
      return getToken(MySqlParser.DEFINER, 0);
    }

    public TerminalNode DELAY_KEY_WRITE() {
      return getToken(MySqlParser.DELAY_KEY_WRITE, 0);
    }

    public TerminalNode DIRECTORY() {
      return getToken(MySqlParser.DIRECTORY, 0);
    }

    public TerminalNode DISABLE() {
      return getToken(MySqlParser.DISABLE, 0);
    }

    public TerminalNode DISCARD() {
      return getToken(MySqlParser.DISCARD, 0);
    }

    public TerminalNode DISK() {
      return getToken(MySqlParser.DISK, 0);
    }

    public TerminalNode DO() {
      return getToken(MySqlParser.DO, 0);
    }

    public TerminalNode DUMPFILE() {
      return getToken(MySqlParser.DUMPFILE, 0);
    }

    public TerminalNode DUPLICATE() {
      return getToken(MySqlParser.DUPLICATE, 0);
    }

    public TerminalNode DYNAMIC() {
      return getToken(MySqlParser.DYNAMIC, 0);
    }

    public TerminalNode ENABLE() {
      return getToken(MySqlParser.ENABLE, 0);
    }

    public TerminalNode ENCRYPTION() {
      return getToken(MySqlParser.ENCRYPTION, 0);
    }

    public TerminalNode ENDS() {
      return getToken(MySqlParser.ENDS, 0);
    }

    public TerminalNode ENGINE() {
      return getToken(MySqlParser.ENGINE, 0);
    }

    public TerminalNode ENGINES() {
      return getToken(MySqlParser.ENGINES, 0);
    }

    public TerminalNode ERROR() {
      return getToken(MySqlParser.ERROR, 0);
    }

    public TerminalNode ERRORS() {
      return getToken(MySqlParser.ERRORS, 0);
    }

    public TerminalNode ESCAPE() {
      return getToken(MySqlParser.ESCAPE, 0);
    }

    public TerminalNode EVEN() {
      return getToken(MySqlParser.EVEN, 0);
    }

    public TerminalNode EVENT() {
      return getToken(MySqlParser.EVENT, 0);
    }

    public TerminalNode EVENTS() {
      return getToken(MySqlParser.EVENTS, 0);
    }

    public TerminalNode EVERY() {
      return getToken(MySqlParser.EVERY, 0);
    }

    public TerminalNode EXCHANGE() {
      return getToken(MySqlParser.EXCHANGE, 0);
    }

    public TerminalNode EXCLUSIVE() {
      return getToken(MySqlParser.EXCLUSIVE, 0);
    }

    public TerminalNode EXPIRE() {
      return getToken(MySqlParser.EXPIRE, 0);
    }

    public TerminalNode EXTENDED() {
      return getToken(MySqlParser.EXTENDED, 0);
    }

    public TerminalNode EXTENT_SIZE() {
      return getToken(MySqlParser.EXTENT_SIZE, 0);
    }

    public TerminalNode FAST() {
      return getToken(MySqlParser.FAST, 0);
    }

    public TerminalNode FAULTS() {
      return getToken(MySqlParser.FAULTS, 0);
    }

    public TerminalNode FIELDS() {
      return getToken(MySqlParser.FIELDS, 0);
    }

    public TerminalNode FILE_BLOCK_SIZE() {
      return getToken(MySqlParser.FILE_BLOCK_SIZE, 0);
    }

    public TerminalNode FILTER() {
      return getToken(MySqlParser.FILTER, 0);
    }

    public TerminalNode FIRST() {
      return getToken(MySqlParser.FIRST, 0);
    }

    public TerminalNode FIXED() {
      return getToken(MySqlParser.FIXED, 0);
    }

    public TerminalNode FOLLOWS() {
      return getToken(MySqlParser.FOLLOWS, 0);
    }

    public TerminalNode FULL() {
      return getToken(MySqlParser.FULL, 0);
    }

    public TerminalNode FUNCTION() {
      return getToken(MySqlParser.FUNCTION, 0);
    }

    public TerminalNode GLOBAL() {
      return getToken(MySqlParser.GLOBAL, 0);
    }

    public TerminalNode GRANTS() {
      return getToken(MySqlParser.GRANTS, 0);
    }

    public TerminalNode GROUP_REPLICATION() {
      return getToken(MySqlParser.GROUP_REPLICATION, 0);
    }

    public TerminalNode HASH() {
      return getToken(MySqlParser.HASH, 0);
    }

    public TerminalNode HOST() {
      return getToken(MySqlParser.HOST, 0);
    }

    public TerminalNode IDENTIFIED() {
      return getToken(MySqlParser.IDENTIFIED, 0);
    }

    public TerminalNode IGNORE_SERVER_IDS() {
      return getToken(MySqlParser.IGNORE_SERVER_IDS, 0);
    }

    public TerminalNode IMPORT() {
      return getToken(MySqlParser.IMPORT, 0);
    }

    public TerminalNode INDEXES() {
      return getToken(MySqlParser.INDEXES, 0);
    }

    public TerminalNode INITIAL_SIZE() {
      return getToken(MySqlParser.INITIAL_SIZE, 0);
    }

    public TerminalNode INPLACE() {
      return getToken(MySqlParser.INPLACE, 0);
    }

    public TerminalNode INSERT_METHOD() {
      return getToken(MySqlParser.INSERT_METHOD, 0);
    }

    public TerminalNode INSTANCE() {
      return getToken(MySqlParser.INSTANCE, 0);
    }

    public TerminalNode INVOKER() {
      return getToken(MySqlParser.INVOKER, 0);
    }

    public TerminalNode IO() {
      return getToken(MySqlParser.IO, 0);
    }

    public TerminalNode IO_THREAD() {
      return getToken(MySqlParser.IO_THREAD, 0);
    }

    public TerminalNode IPC() {
      return getToken(MySqlParser.IPC, 0);
    }

    public TerminalNode ISOLATION() {
      return getToken(MySqlParser.ISOLATION, 0);
    }

    public TerminalNode ISSUER() {
      return getToken(MySqlParser.ISSUER, 0);
    }

    public TerminalNode KEY_BLOCK_SIZE() {
      return getToken(MySqlParser.KEY_BLOCK_SIZE, 0);
    }

    public TerminalNode LANGUAGE() {
      return getToken(MySqlParser.LANGUAGE, 0);
    }

    public TerminalNode LAST() {
      return getToken(MySqlParser.LAST, 0);
    }

    public TerminalNode LEAVES() {
      return getToken(MySqlParser.LEAVES, 0);
    }

    public TerminalNode LESS() {
      return getToken(MySqlParser.LESS, 0);
    }

    public TerminalNode LEVEL() {
      return getToken(MySqlParser.LEVEL, 0);
    }

    public TerminalNode LIST() {
      return getToken(MySqlParser.LIST, 0);
    }

    public TerminalNode LOCAL() {
      return getToken(MySqlParser.LOCAL, 0);
    }

    public TerminalNode LOGFILE() {
      return getToken(MySqlParser.LOGFILE, 0);
    }

    public TerminalNode LOGS() {
      return getToken(MySqlParser.LOGS, 0);
    }

    public TerminalNode MASTER() {
      return getToken(MySqlParser.MASTER, 0);
    }

    public TerminalNode MASTER_AUTO_POSITION() {
      return getToken(MySqlParser.MASTER_AUTO_POSITION, 0);
    }

    public TerminalNode MASTER_CONNECT_RETRY() {
      return getToken(MySqlParser.MASTER_CONNECT_RETRY, 0);
    }

    public TerminalNode MASTER_DELAY() {
      return getToken(MySqlParser.MASTER_DELAY, 0);
    }

    public TerminalNode MASTER_HEARTBEAT_PERIOD() {
      return getToken(MySqlParser.MASTER_HEARTBEAT_PERIOD, 0);
    }

    public TerminalNode MASTER_HOST() {
      return getToken(MySqlParser.MASTER_HOST, 0);
    }

    public TerminalNode MASTER_LOG_FILE() {
      return getToken(MySqlParser.MASTER_LOG_FILE, 0);
    }

    public TerminalNode MASTER_LOG_POS() {
      return getToken(MySqlParser.MASTER_LOG_POS, 0);
    }

    public TerminalNode MASTER_PASSWORD() {
      return getToken(MySqlParser.MASTER_PASSWORD, 0);
    }

    public TerminalNode MASTER_PORT() {
      return getToken(MySqlParser.MASTER_PORT, 0);
    }

    public TerminalNode MASTER_RETRY_COUNT() {
      return getToken(MySqlParser.MASTER_RETRY_COUNT, 0);
    }

    public TerminalNode MASTER_SSL() {
      return getToken(MySqlParser.MASTER_SSL, 0);
    }

    public TerminalNode MASTER_SSL_CA() {
      return getToken(MySqlParser.MASTER_SSL_CA, 0);
    }

    public TerminalNode MASTER_SSL_CAPATH() {
      return getToken(MySqlParser.MASTER_SSL_CAPATH, 0);
    }

    public TerminalNode MASTER_SSL_CERT() {
      return getToken(MySqlParser.MASTER_SSL_CERT, 0);
    }

    public TerminalNode MASTER_SSL_CIPHER() {
      return getToken(MySqlParser.MASTER_SSL_CIPHER, 0);
    }

    public TerminalNode MASTER_SSL_CRL() {
      return getToken(MySqlParser.MASTER_SSL_CRL, 0);
    }

    public TerminalNode MASTER_SSL_CRLPATH() {
      return getToken(MySqlParser.MASTER_SSL_CRLPATH, 0);
    }

    public TerminalNode MASTER_SSL_KEY() {
      return getToken(MySqlParser.MASTER_SSL_KEY, 0);
    }

    public TerminalNode MASTER_TLS_VERSION() {
      return getToken(MySqlParser.MASTER_TLS_VERSION, 0);
    }

    public TerminalNode MASTER_USER() {
      return getToken(MySqlParser.MASTER_USER, 0);
    }

    public TerminalNode MAX_CONNECTIONS_PER_HOUR() {
      return getToken(MySqlParser.MAX_CONNECTIONS_PER_HOUR, 0);
    }

    public TerminalNode MAX_QUERIES_PER_HOUR() {
      return getToken(MySqlParser.MAX_QUERIES_PER_HOUR, 0);
    }

    public TerminalNode MAX_ROWS() {
      return getToken(MySqlParser.MAX_ROWS, 0);
    }

    public TerminalNode MAX_SIZE() {
      return getToken(MySqlParser.MAX_SIZE, 0);
    }

    public TerminalNode MAX_UPDATES_PER_HOUR() {
      return getToken(MySqlParser.MAX_UPDATES_PER_HOUR, 0);
    }

    public TerminalNode MAX_USER_CONNECTIONS() {
      return getToken(MySqlParser.MAX_USER_CONNECTIONS, 0);
    }

    public TerminalNode MEDIUM() {
      return getToken(MySqlParser.MEDIUM, 0);
    }

    public TerminalNode MEMORY() {
      return getToken(MySqlParser.MEMORY, 0);
    }

    public TerminalNode MERGE() {
      return getToken(MySqlParser.MERGE, 0);
    }

    public TerminalNode MID() {
      return getToken(MySqlParser.MID, 0);
    }

    public TerminalNode MIGRATE() {
      return getToken(MySqlParser.MIGRATE, 0);
    }

    public TerminalNode MIN_ROWS() {
      return getToken(MySqlParser.MIN_ROWS, 0);
    }

    public TerminalNode MODIFY() {
      return getToken(MySqlParser.MODIFY, 0);
    }

    public TerminalNode MUTEX() {
      return getToken(MySqlParser.MUTEX, 0);
    }

    public TerminalNode MYSQL() {
      return getToken(MySqlParser.MYSQL, 0);
    }

    public TerminalNode NAME() {
      return getToken(MySqlParser.NAME, 0);
    }

    public TerminalNode NAMES() {
      return getToken(MySqlParser.NAMES, 0);
    }

    public TerminalNode NCHAR() {
      return getToken(MySqlParser.NCHAR, 0);
    }

    public TerminalNode NEVER() {
      return getToken(MySqlParser.NEVER, 0);
    }

    public TerminalNode NO() {
      return getToken(MySqlParser.NO, 0);
    }

    public TerminalNode NODEGROUP() {
      return getToken(MySqlParser.NODEGROUP, 0);
    }

    public TerminalNode NONE() {
      return getToken(MySqlParser.NONE, 0);
    }

    public TerminalNode OFFLINE() {
      return getToken(MySqlParser.OFFLINE, 0);
    }

    public TerminalNode OFFSET() {
      return getToken(MySqlParser.OFFSET, 0);
    }

    public TerminalNode OJ() {
      return getToken(MySqlParser.OJ, 0);
    }

    public TerminalNode OLD_PASSWORD() {
      return getToken(MySqlParser.OLD_PASSWORD, 0);
    }

    public TerminalNode ONE() {
      return getToken(MySqlParser.ONE, 0);
    }

    public TerminalNode ONLINE() {
      return getToken(MySqlParser.ONLINE, 0);
    }

    public TerminalNode ONLY() {
      return getToken(MySqlParser.ONLY, 0);
    }

    public TerminalNode OPTIMIZER_COSTS() {
      return getToken(MySqlParser.OPTIMIZER_COSTS, 0);
    }

    public TerminalNode OPTIONS() {
      return getToken(MySqlParser.OPTIONS, 0);
    }

    public TerminalNode OWNER() {
      return getToken(MySqlParser.OWNER, 0);
    }

    public TerminalNode PACK_KEYS() {
      return getToken(MySqlParser.PACK_KEYS, 0);
    }

    public TerminalNode PAGE() {
      return getToken(MySqlParser.PAGE, 0);
    }

    public TerminalNode PARSER() {
      return getToken(MySqlParser.PARSER, 0);
    }

    public TerminalNode PARTIAL() {
      return getToken(MySqlParser.PARTIAL, 0);
    }

    public TerminalNode PARTITIONING() {
      return getToken(MySqlParser.PARTITIONING, 0);
    }

    public TerminalNode PARTITIONS() {
      return getToken(MySqlParser.PARTITIONS, 0);
    }

    public TerminalNode PASSWORD() {
      return getToken(MySqlParser.PASSWORD, 0);
    }

    public TerminalNode PHASE() {
      return getToken(MySqlParser.PHASE, 0);
    }

    public TerminalNode PLUGINS() {
      return getToken(MySqlParser.PLUGINS, 0);
    }

    public TerminalNode PLUGIN_DIR() {
      return getToken(MySqlParser.PLUGIN_DIR, 0);
    }

    public TerminalNode PORT() {
      return getToken(MySqlParser.PORT, 0);
    }

    public TerminalNode PRECEDES() {
      return getToken(MySqlParser.PRECEDES, 0);
    }

    public TerminalNode PREPARE() {
      return getToken(MySqlParser.PREPARE, 0);
    }

    public TerminalNode PRESERVE() {
      return getToken(MySqlParser.PRESERVE, 0);
    }

    public TerminalNode PREV() {
      return getToken(MySqlParser.PREV, 0);
    }

    public TerminalNode PROCESSLIST() {
      return getToken(MySqlParser.PROCESSLIST, 0);
    }

    public TerminalNode PROFILE() {
      return getToken(MySqlParser.PROFILE, 0);
    }

    public TerminalNode PROFILES() {
      return getToken(MySqlParser.PROFILES, 0);
    }

    public TerminalNode PROXY() {
      return getToken(MySqlParser.PROXY, 0);
    }

    public TerminalNode QUERY() {
      return getToken(MySqlParser.QUERY, 0);
    }

    public TerminalNode QUICK() {
      return getToken(MySqlParser.QUICK, 0);
    }

    public TerminalNode REBUILD() {
      return getToken(MySqlParser.REBUILD, 0);
    }

    public TerminalNode RECOVER() {
      return getToken(MySqlParser.RECOVER, 0);
    }

    public TerminalNode REDO_BUFFER_SIZE() {
      return getToken(MySqlParser.REDO_BUFFER_SIZE, 0);
    }

    public TerminalNode REDUNDANT() {
      return getToken(MySqlParser.REDUNDANT, 0);
    }

    public TerminalNode RELAYLOG() {
      return getToken(MySqlParser.RELAYLOG, 0);
    }

    public TerminalNode RELAY_LOG_FILE() {
      return getToken(MySqlParser.RELAY_LOG_FILE, 0);
    }

    public TerminalNode RELAY_LOG_POS() {
      return getToken(MySqlParser.RELAY_LOG_POS, 0);
    }

    public TerminalNode REMOVE() {
      return getToken(MySqlParser.REMOVE, 0);
    }

    public TerminalNode REORGANIZE() {
      return getToken(MySqlParser.REORGANIZE, 0);
    }

    public TerminalNode REPAIR() {
      return getToken(MySqlParser.REPAIR, 0);
    }

    public TerminalNode REPLICATE_DO_DB() {
      return getToken(MySqlParser.REPLICATE_DO_DB, 0);
    }

    public TerminalNode REPLICATE_DO_TABLE() {
      return getToken(MySqlParser.REPLICATE_DO_TABLE, 0);
    }

    public TerminalNode REPLICATE_IGNORE_DB() {
      return getToken(MySqlParser.REPLICATE_IGNORE_DB, 0);
    }

    public TerminalNode REPLICATE_IGNORE_TABLE() {
      return getToken(MySqlParser.REPLICATE_IGNORE_TABLE, 0);
    }

    public TerminalNode REPLICATE_REWRITE_DB() {
      return getToken(MySqlParser.REPLICATE_REWRITE_DB, 0);
    }

    public TerminalNode REPLICATE_WILD_DO_TABLE() {
      return getToken(MySqlParser.REPLICATE_WILD_DO_TABLE, 0);
    }

    public TerminalNode REPLICATE_WILD_IGNORE_TABLE() {
      return getToken(MySqlParser.REPLICATE_WILD_IGNORE_TABLE, 0);
    }

    public TerminalNode REPLICATION() {
      return getToken(MySqlParser.REPLICATION, 0);
    }

    public TerminalNode RESUME() {
      return getToken(MySqlParser.RESUME, 0);
    }

    public TerminalNode RETURNS() {
      return getToken(MySqlParser.RETURNS, 0);
    }

    public TerminalNode ROLLBACK() {
      return getToken(MySqlParser.ROLLBACK, 0);
    }

    public TerminalNode ROLLUP() {
      return getToken(MySqlParser.ROLLUP, 0);
    }

    public TerminalNode ROTATE() {
      return getToken(MySqlParser.ROTATE, 0);
    }

    public TerminalNode ROW() {
      return getToken(MySqlParser.ROW, 0);
    }

    public TerminalNode ROWS() {
      return getToken(MySqlParser.ROWS, 0);
    }

    public TerminalNode ROW_FORMAT() {
      return getToken(MySqlParser.ROW_FORMAT, 0);
    }

    public TerminalNode SAVEPOINT() {
      return getToken(MySqlParser.SAVEPOINT, 0);
    }

    public TerminalNode SCHEDULE() {
      return getToken(MySqlParser.SCHEDULE, 0);
    }

    public TerminalNode SECURITY() {
      return getToken(MySqlParser.SECURITY, 0);
    }

    public TerminalNode SERVER() {
      return getToken(MySqlParser.SERVER, 0);
    }

    public TerminalNode SESSION() {
      return getToken(MySqlParser.SESSION, 0);
    }

    public TerminalNode SHARE() {
      return getToken(MySqlParser.SHARE, 0);
    }

    public TerminalNode SHARED() {
      return getToken(MySqlParser.SHARED, 0);
    }

    public TerminalNode SIGNED() {
      return getToken(MySqlParser.SIGNED, 0);
    }

    public TerminalNode SIMPLE() {
      return getToken(MySqlParser.SIMPLE, 0);
    }

    public TerminalNode SLAVE() {
      return getToken(MySqlParser.SLAVE, 0);
    }

    public TerminalNode SNAPSHOT() {
      return getToken(MySqlParser.SNAPSHOT, 0);
    }

    public TerminalNode SOCKET() {
      return getToken(MySqlParser.SOCKET, 0);
    }

    public TerminalNode SOME() {
      return getToken(MySqlParser.SOME, 0);
    }

    public TerminalNode SOUNDS() {
      return getToken(MySqlParser.SOUNDS, 0);
    }

    public TerminalNode SOURCE() {
      return getToken(MySqlParser.SOURCE, 0);
    }

    public TerminalNode SQL_AFTER_GTIDS() {
      return getToken(MySqlParser.SQL_AFTER_GTIDS, 0);
    }

    public TerminalNode SQL_AFTER_MTS_GAPS() {
      return getToken(MySqlParser.SQL_AFTER_MTS_GAPS, 0);
    }

    public TerminalNode SQL_BEFORE_GTIDS() {
      return getToken(MySqlParser.SQL_BEFORE_GTIDS, 0);
    }

    public TerminalNode SQL_BUFFER_RESULT() {
      return getToken(MySqlParser.SQL_BUFFER_RESULT, 0);
    }

    public TerminalNode SQL_CACHE() {
      return getToken(MySqlParser.SQL_CACHE, 0);
    }

    public TerminalNode SQL_NO_CACHE() {
      return getToken(MySqlParser.SQL_NO_CACHE, 0);
    }

    public TerminalNode SQL_THREAD() {
      return getToken(MySqlParser.SQL_THREAD, 0);
    }

    public TerminalNode START() {
      return getToken(MySqlParser.START, 0);
    }

    public TerminalNode STARTS() {
      return getToken(MySqlParser.STARTS, 0);
    }

    public TerminalNode STATS_AUTO_RECALC() {
      return getToken(MySqlParser.STATS_AUTO_RECALC, 0);
    }

    public TerminalNode STATS_PERSISTENT() {
      return getToken(MySqlParser.STATS_PERSISTENT, 0);
    }

    public TerminalNode STATS_SAMPLE_PAGES() {
      return getToken(MySqlParser.STATS_SAMPLE_PAGES, 0);
    }

    public TerminalNode STATUS() {
      return getToken(MySqlParser.STATUS, 0);
    }

    public TerminalNode STOP() {
      return getToken(MySqlParser.STOP, 0);
    }

    public TerminalNode STORAGE() {
      return getToken(MySqlParser.STORAGE, 0);
    }

    public TerminalNode STRING() {
      return getToken(MySqlParser.STRING, 0);
    }

    public TerminalNode SUBJECT() {
      return getToken(MySqlParser.SUBJECT, 0);
    }

    public TerminalNode SUBPARTITION() {
      return getToken(MySqlParser.SUBPARTITION, 0);
    }

    public TerminalNode SUBPARTITIONS() {
      return getToken(MySqlParser.SUBPARTITIONS, 0);
    }

    public TerminalNode SUSPEND() {
      return getToken(MySqlParser.SUSPEND, 0);
    }

    public TerminalNode SWAPS() {
      return getToken(MySqlParser.SWAPS, 0);
    }

    public TerminalNode SWITCHES() {
      return getToken(MySqlParser.SWITCHES, 0);
    }

    public TerminalNode TABLESPACE() {
      return getToken(MySqlParser.TABLESPACE, 0);
    }

    public TerminalNode TEMPORARY() {
      return getToken(MySqlParser.TEMPORARY, 0);
    }

    public TerminalNode TEMPTABLE() {
      return getToken(MySqlParser.TEMPTABLE, 0);
    }

    public TerminalNode THAN() {
      return getToken(MySqlParser.THAN, 0);
    }

    public TerminalNode TRANSACTION() {
      return getToken(MySqlParser.TRANSACTION, 0);
    }

    public TerminalNode TRUNCATE() {
      return getToken(MySqlParser.TRUNCATE, 0);
    }

    public TerminalNode UNDEFINED() {
      return getToken(MySqlParser.UNDEFINED, 0);
    }

    public TerminalNode UNDOFILE() {
      return getToken(MySqlParser.UNDOFILE, 0);
    }

    public TerminalNode UNDO_BUFFER_SIZE() {
      return getToken(MySqlParser.UNDO_BUFFER_SIZE, 0);
    }

    public TerminalNode UNKNOWN() {
      return getToken(MySqlParser.UNKNOWN, 0);
    }

    public TerminalNode UPGRADE() {
      return getToken(MySqlParser.UPGRADE, 0);
    }

    public TerminalNode USER() {
      return getToken(MySqlParser.USER, 0);
    }

    public TerminalNode VALIDATION() {
      return getToken(MySqlParser.VALIDATION, 0);
    }

    public TerminalNode VALUE() {
      return getToken(MySqlParser.VALUE, 0);
    }

    public TerminalNode VARIABLES() {
      return getToken(MySqlParser.VARIABLES, 0);
    }

    public TerminalNode VIEW() {
      return getToken(MySqlParser.VIEW, 0);
    }

    public TerminalNode WAIT() {
      return getToken(MySqlParser.WAIT, 0);
    }

    public TerminalNode WARNINGS() {
      return getToken(MySqlParser.WARNINGS, 0);
    }

    public TerminalNode WITHOUT() {
      return getToken(MySqlParser.WITHOUT, 0);
    }

    public TerminalNode WORK() {
      return getToken(MySqlParser.WORK, 0);
    }

    public TerminalNode WRAPPER() {
      return getToken(MySqlParser.WRAPPER, 0);
    }

    public TerminalNode X509() {
      return getToken(MySqlParser.X509, 0);
    }

    public TerminalNode XA() {
      return getToken(MySqlParser.XA, 0);
    }

    public TerminalNode XML() {
      return getToken(MySqlParser.XML, 0);
    }

    public KeywordsCanBeIdContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_keywordsCanBeId;
    }

    @Override
    public void enterRule(ParseTreeListener listener) {
      if (listener instanceof MySqlParserListener)
        ((MySqlParserListener) listener).enterKeywordsCanBeId(this);
    }

    @Override
    public void exitRule(ParseTreeListener listener) {
      if (listener instanceof MySqlParserListener)
        ((MySqlParserListener) listener).exitKeywordsCanBeId(this);
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof MySqlParserVisitor)
        return ((MySqlParserVisitor<? extends T>) visitor).visitKeywordsCanBeId(this);
      else return visitor.visitChildren(this);
    }
  }

  public final KeywordsCanBeIdContext keywordsCanBeId() throws RecognitionException {
    KeywordsCanBeIdContext _localctx = new KeywordsCanBeIdContext(_ctx, getState());
    enterRule(_localctx, 76, RULE_keywordsCanBeId);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
        setState(653);
        _la = _input.LA(1);
        if (!(((((_la - 246)) & ~0x3f) == 0
                && ((1L << (_la - 246))
                        & ((1L << (ACCOUNT - 246))
                            | (1L << (ACTION - 246))
                            | (1L << (AFTER - 246))
                            | (1L << (AGGREGATE - 246))
                            | (1L << (ALGORITHM - 246))
                            | (1L << (ANY - 246))
                            | (1L << (AT - 246))
                            | (1L << (AUTHORS - 246))
                            | (1L << (AUTOCOMMIT - 246))
                            | (1L << (AUTOEXTEND_SIZE - 246))
                            | (1L << (AUTO_INCREMENT - 246))
                            | (1L << (AVG_ROW_LENGTH - 246))
                            | (1L << (BEGIN - 246))
                            | (1L << (BINLOG - 246))
                            | (1L << (BIT - 246))
                            | (1L << (BLOCK - 246))
                            | (1L << (BOOL - 246))
                            | (1L << (BOOLEAN - 246))
                            | (1L << (BTREE - 246))
                            | (1L << (CASCADED - 246))
                            | (1L << (CHAIN - 246))
                            | (1L << (CHANGED - 246))
                            | (1L << (CHANNEL - 246))
                            | (1L << (CHECKSUM - 246))
                            | (1L << (CIPHER - 246))
                            | (1L << (CLIENT - 246))
                            | (1L << (COALESCE - 246))
                            | (1L << (CODE - 246))
                            | (1L << (COLUMNS - 246))
                            | (1L << (COLUMN_FORMAT - 246))
                            | (1L << (COMMENT - 246))
                            | (1L << (COMMIT - 246))
                            | (1L << (COMPACT - 246))
                            | (1L << (COMPLETION - 246))
                            | (1L << (COMPRESSED - 246))
                            | (1L << (COMPRESSION - 246))
                            | (1L << (CONCURRENT - 246))
                            | (1L << (CONNECTION - 246))
                            | (1L << (CONSISTENT - 246))
                            | (1L << (CONTAINS - 246))
                            | (1L << (CONTEXT - 246))
                            | (1L << (CONTRIBUTORS - 246))
                            | (1L << (COPY - 246))
                            | (1L << (CPU - 246))
                            | (1L << (DATA - 246))
                            | (1L << (DATAFILE - 246))
                            | (1L << (DEALLOCATE - 246))
                            | (1L << (DEFAULT_AUTH - 246))
                            | (1L << (DEFINER - 246))
                            | (1L << (DELAY_KEY_WRITE - 246))
                            | (1L << (DIRECTORY - 246))
                            | (1L << (DISABLE - 246))
                            | (1L << (DISCARD - 246))
                            | (1L << (DISK - 246))
                            | (1L << (DO - 246))
                            | (1L << (DUMPFILE - 246))
                            | (1L << (DUPLICATE - 246))
                            | (1L << (DYNAMIC - 246))
                            | (1L << (ENABLE - 246))
                            | (1L << (ENCRYPTION - 246))))
                    != 0)
            || ((((_la - 310)) & ~0x3f) == 0
                && ((1L << (_la - 310))
                        & ((1L << (ENDS - 310))
                            | (1L << (ENGINE - 310))
                            | (1L << (ENGINES - 310))
                            | (1L << (ERROR - 310))
                            | (1L << (ERRORS - 310))
                            | (1L << (ESCAPE - 310))
                            | (1L << (EVEN - 310))
                            | (1L << (EVENT - 310))
                            | (1L << (EVENTS - 310))
                            | (1L << (EVERY - 310))
                            | (1L << (EXCHANGE - 310))
                            | (1L << (EXCLUSIVE - 310))
                            | (1L << (EXPIRE - 310))
                            | (1L << (EXTENDED - 310))
                            | (1L << (EXTENT_SIZE - 310))
                            | (1L << (FAST - 310))
                            | (1L << (FAULTS - 310))
                            | (1L << (FIELDS - 310))
                            | (1L << (FILE_BLOCK_SIZE - 310))
                            | (1L << (FILTER - 310))
                            | (1L << (FIRST - 310))
                            | (1L << (FIXED - 310))
                            | (1L << (FOLLOWS - 310))
                            | (1L << (FULL - 310))
                            | (1L << (FUNCTION - 310))
                            | (1L << (GLOBAL - 310))
                            | (1L << (GRANTS - 310))
                            | (1L << (GROUP_REPLICATION - 310))
                            | (1L << (HASH - 310))
                            | (1L << (HOST - 310))
                            | (1L << (IDENTIFIED - 310))
                            | (1L << (IGNORE_SERVER_IDS - 310))
                            | (1L << (IMPORT - 310))
                            | (1L << (INDEXES - 310))
                            | (1L << (INITIAL_SIZE - 310))
                            | (1L << (INPLACE - 310))
                            | (1L << (INSERT_METHOD - 310))
                            | (1L << (INSTANCE - 310))
                            | (1L << (INVOKER - 310))
                            | (1L << (IO - 310))
                            | (1L << (IO_THREAD - 310))
                            | (1L << (IPC - 310))
                            | (1L << (ISOLATION - 310))
                            | (1L << (ISSUER - 310))
                            | (1L << (KEY_BLOCK_SIZE - 310))
                            | (1L << (LANGUAGE - 310))
                            | (1L << (LAST - 310))
                            | (1L << (LEAVES - 310))
                            | (1L << (LESS - 310))
                            | (1L << (LEVEL - 310))
                            | (1L << (LIST - 310))
                            | (1L << (LOCAL - 310))
                            | (1L << (LOGFILE - 310))
                            | (1L << (LOGS - 310))
                            | (1L << (MASTER - 310))))
                    != 0)
            || ((((_la - 374)) & ~0x3f) == 0
                && ((1L << (_la - 374))
                        & ((1L << (MASTER_AUTO_POSITION - 374))
                            | (1L << (MASTER_CONNECT_RETRY - 374))
                            | (1L << (MASTER_DELAY - 374))
                            | (1L << (MASTER_HEARTBEAT_PERIOD - 374))
                            | (1L << (MASTER_HOST - 374))
                            | (1L << (MASTER_LOG_FILE - 374))
                            | (1L << (MASTER_LOG_POS - 374))
                            | (1L << (MASTER_PASSWORD - 374))
                            | (1L << (MASTER_PORT - 374))
                            | (1L << (MASTER_RETRY_COUNT - 374))
                            | (1L << (MASTER_SSL - 374))
                            | (1L << (MASTER_SSL_CA - 374))
                            | (1L << (MASTER_SSL_CAPATH - 374))
                            | (1L << (MASTER_SSL_CERT - 374))
                            | (1L << (MASTER_SSL_CIPHER - 374))
                            | (1L << (MASTER_SSL_CRL - 374))
                            | (1L << (MASTER_SSL_CRLPATH - 374))
                            | (1L << (MASTER_SSL_KEY - 374))
                            | (1L << (MASTER_TLS_VERSION - 374))
                            | (1L << (MASTER_USER - 374))
                            | (1L << (MAX_CONNECTIONS_PER_HOUR - 374))
                            | (1L << (MAX_QUERIES_PER_HOUR - 374))
                            | (1L << (MAX_ROWS - 374))
                            | (1L << (MAX_SIZE - 374))
                            | (1L << (MAX_UPDATES_PER_HOUR - 374))
                            | (1L << (MAX_USER_CONNECTIONS - 374))
                            | (1L << (MEDIUM - 374))
                            | (1L << (MERGE - 374))
                            | (1L << (MID - 374))
                            | (1L << (MIGRATE - 374))
                            | (1L << (MIN_ROWS - 374))
                            | (1L << (MODIFY - 374))
                            | (1L << (MUTEX - 374))
                            | (1L << (MYSQL - 374))
                            | (1L << (NAME - 374))
                            | (1L << (NAMES - 374))
                            | (1L << (NCHAR - 374))
                            | (1L << (NEVER - 374))
                            | (1L << (NO - 374))
                            | (1L << (NODEGROUP - 374))
                            | (1L << (NONE - 374))
                            | (1L << (OFFLINE - 374))
                            | (1L << (OFFSET - 374))
                            | (1L << (OJ - 374))
                            | (1L << (OLD_PASSWORD - 374))
                            | (1L << (ONE - 374))
                            | (1L << (ONLINE - 374))
                            | (1L << (ONLY - 374))
                            | (1L << (OPTIMIZER_COSTS - 374))
                            | (1L << (OPTIONS - 374))
                            | (1L << (OWNER - 374))
                            | (1L << (PACK_KEYS - 374))
                            | (1L << (PAGE - 374))
                            | (1L << (PARSER - 374))
                            | (1L << (PARTIAL - 374))
                            | (1L << (PARTITIONING - 374))
                            | (1L << (PARTITIONS - 374))
                            | (1L << (PASSWORD - 374))
                            | (1L << (PHASE - 374))
                            | (1L << (PLUGIN_DIR - 374))))
                    != 0)
            || ((((_la - 438)) & ~0x3f) == 0
                && ((1L << (_la - 438))
                        & ((1L << (PLUGINS - 438))
                            | (1L << (PORT - 438))
                            | (1L << (PRECEDES - 438))
                            | (1L << (PREPARE - 438))
                            | (1L << (PRESERVE - 438))
                            | (1L << (PREV - 438))
                            | (1L << (PROCESSLIST - 438))
                            | (1L << (PROFILE - 438))
                            | (1L << (PROFILES - 438))
                            | (1L << (PROXY - 438))
                            | (1L << (QUERY - 438))
                            | (1L << (QUICK - 438))
                            | (1L << (REBUILD - 438))
                            | (1L << (RECOVER - 438))
                            | (1L << (REDO_BUFFER_SIZE - 438))
                            | (1L << (REDUNDANT - 438))
                            | (1L << (RELAY_LOG_FILE - 438))
                            | (1L << (RELAY_LOG_POS - 438))
                            | (1L << (RELAYLOG - 438))
                            | (1L << (REMOVE - 438))
                            | (1L << (REORGANIZE - 438))
                            | (1L << (REPAIR - 438))
                            | (1L << (REPLICATE_DO_DB - 438))
                            | (1L << (REPLICATE_DO_TABLE - 438))
                            | (1L << (REPLICATE_IGNORE_DB - 438))
                            | (1L << (REPLICATE_IGNORE_TABLE - 438))
                            | (1L << (REPLICATE_REWRITE_DB - 438))
                            | (1L << (REPLICATE_WILD_DO_TABLE - 438))
                            | (1L << (REPLICATE_WILD_IGNORE_TABLE - 438))
                            | (1L << (REPLICATION - 438))
                            | (1L << (RESUME - 438))
                            | (1L << (RETURNS - 438))
                            | (1L << (ROLLBACK - 438))
                            | (1L << (ROLLUP - 438))
                            | (1L << (ROTATE - 438))
                            | (1L << (ROW - 438))
                            | (1L << (ROWS - 438))
                            | (1L << (ROW_FORMAT - 438))
                            | (1L << (SAVEPOINT - 438))
                            | (1L << (SCHEDULE - 438))
                            | (1L << (SECURITY - 438))
                            | (1L << (SERVER - 438))
                            | (1L << (SESSION - 438))
                            | (1L << (SHARE - 438))
                            | (1L << (SHARED - 438))
                            | (1L << (SIGNED - 438))
                            | (1L << (SIMPLE - 438))
                            | (1L << (SLAVE - 438))
                            | (1L << (SNAPSHOT - 438))
                            | (1L << (SOCKET - 438))
                            | (1L << (SOME - 438))
                            | (1L << (SOUNDS - 438))
                            | (1L << (SOURCE - 438))
                            | (1L << (SQL_AFTER_GTIDS - 438))
                            | (1L << (SQL_AFTER_MTS_GAPS - 438))
                            | (1L << (SQL_BEFORE_GTIDS - 438))
                            | (1L << (SQL_BUFFER_RESULT - 438))
                            | (1L << (SQL_CACHE - 438))
                            | (1L << (SQL_NO_CACHE - 438))
                            | (1L << (SQL_THREAD - 438))))
                    != 0)
            || ((((_la - 502)) & ~0x3f) == 0
                && ((1L << (_la - 502))
                        & ((1L << (START - 502))
                            | (1L << (STARTS - 502))
                            | (1L << (STATS_AUTO_RECALC - 502))
                            | (1L << (STATS_PERSISTENT - 502))
                            | (1L << (STATS_SAMPLE_PAGES - 502))
                            | (1L << (STATUS - 502))
                            | (1L << (STOP - 502))
                            | (1L << (STORAGE - 502))
                            | (1L << (STRING - 502))
                            | (1L << (SUBJECT - 502))
                            | (1L << (SUBPARTITION - 502))
                            | (1L << (SUBPARTITIONS - 502))
                            | (1L << (SUSPEND - 502))
                            | (1L << (SWAPS - 502))
                            | (1L << (SWITCHES - 502))
                            | (1L << (TABLESPACE - 502))
                            | (1L << (TEMPORARY - 502))
                            | (1L << (TEMPTABLE - 502))
                            | (1L << (THAN - 502))
                            | (1L << (TRANSACTION - 502))
                            | (1L << (TRUNCATE - 502))
                            | (1L << (UNDEFINED - 502))
                            | (1L << (UNDOFILE - 502))
                            | (1L << (UNDO_BUFFER_SIZE - 502))
                            | (1L << (UNKNOWN - 502))
                            | (1L << (UPGRADE - 502))
                            | (1L << (USER - 502))
                            | (1L << (VALIDATION - 502))
                            | (1L << (VALUE - 502))
                            | (1L << (VARIABLES - 502))
                            | (1L << (VIEW - 502))
                            | (1L << (WAIT - 502))
                            | (1L << (WARNINGS - 502))
                            | (1L << (WITHOUT - 502))
                            | (1L << (WORK - 502))
                            | (1L << (WRAPPER - 502))
                            | (1L << (X509 - 502))
                            | (1L << (XA - 502))
                            | (1L << (XML - 502))))
                    != 0)
            || _la == MEMORY)) {
          _errHandler.recoverInline(this);
        } else {
          if (_input.LA(1) == Token.EOF) matchedEOF = true;
          _errHandler.reportMatch(this);
          consume();
        }
      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  public static class FunctionNameBaseContext extends ParserRuleContext {
    public TerminalNode ABS() {
      return getToken(MySqlParser.ABS, 0);
    }

    public TerminalNode ACOS() {
      return getToken(MySqlParser.ACOS, 0);
    }

    public TerminalNode ADDDATE() {
      return getToken(MySqlParser.ADDDATE, 0);
    }

    public TerminalNode ADDTIME() {
      return getToken(MySqlParser.ADDTIME, 0);
    }

    public TerminalNode AES_DECRYPT() {
      return getToken(MySqlParser.AES_DECRYPT, 0);
    }

    public TerminalNode AES_ENCRYPT() {
      return getToken(MySqlParser.AES_ENCRYPT, 0);
    }

    public TerminalNode AREA() {
      return getToken(MySqlParser.AREA, 0);
    }

    public TerminalNode ASBINARY() {
      return getToken(MySqlParser.ASBINARY, 0);
    }

    public TerminalNode ASIN() {
      return getToken(MySqlParser.ASIN, 0);
    }

    public TerminalNode ASTEXT() {
      return getToken(MySqlParser.ASTEXT, 0);
    }

    public TerminalNode ASWKB() {
      return getToken(MySqlParser.ASWKB, 0);
    }

    public TerminalNode ASWKT() {
      return getToken(MySqlParser.ASWKT, 0);
    }

    public TerminalNode ASYMMETRIC_DECRYPT() {
      return getToken(MySqlParser.ASYMMETRIC_DECRYPT, 0);
    }

    public TerminalNode ASYMMETRIC_DERIVE() {
      return getToken(MySqlParser.ASYMMETRIC_DERIVE, 0);
    }

    public TerminalNode ASYMMETRIC_ENCRYPT() {
      return getToken(MySqlParser.ASYMMETRIC_ENCRYPT, 0);
    }

    public TerminalNode ASYMMETRIC_SIGN() {
      return getToken(MySqlParser.ASYMMETRIC_SIGN, 0);
    }

    public TerminalNode ASYMMETRIC_VERIFY() {
      return getToken(MySqlParser.ASYMMETRIC_VERIFY, 0);
    }

    public TerminalNode ATAN() {
      return getToken(MySqlParser.ATAN, 0);
    }

    public TerminalNode ATAN2() {
      return getToken(MySqlParser.ATAN2, 0);
    }

    public TerminalNode BENCHMARK() {
      return getToken(MySqlParser.BENCHMARK, 0);
    }

    public TerminalNode BIN() {
      return getToken(MySqlParser.BIN, 0);
    }

    public TerminalNode BIT_COUNT() {
      return getToken(MySqlParser.BIT_COUNT, 0);
    }

    public TerminalNode BIT_LENGTH() {
      return getToken(MySqlParser.BIT_LENGTH, 0);
    }

    public TerminalNode BUFFER() {
      return getToken(MySqlParser.BUFFER, 0);
    }

    public TerminalNode CEIL() {
      return getToken(MySqlParser.CEIL, 0);
    }

    public TerminalNode CEILING() {
      return getToken(MySqlParser.CEILING, 0);
    }

    public TerminalNode CENTROID() {
      return getToken(MySqlParser.CENTROID, 0);
    }

    public TerminalNode CHARACTER_LENGTH() {
      return getToken(MySqlParser.CHARACTER_LENGTH, 0);
    }

    public TerminalNode CHARSET() {
      return getToken(MySqlParser.CHARSET, 0);
    }

    public TerminalNode CHAR_LENGTH() {
      return getToken(MySqlParser.CHAR_LENGTH, 0);
    }

    public TerminalNode COERCIBILITY() {
      return getToken(MySqlParser.COERCIBILITY, 0);
    }

    public TerminalNode COLLATION() {
      return getToken(MySqlParser.COLLATION, 0);
    }

    public TerminalNode COMPRESS() {
      return getToken(MySqlParser.COMPRESS, 0);
    }

    public TerminalNode CONCAT() {
      return getToken(MySqlParser.CONCAT, 0);
    }

    public TerminalNode CONCAT_WS() {
      return getToken(MySqlParser.CONCAT_WS, 0);
    }

    public TerminalNode CONNECTION_ID() {
      return getToken(MySqlParser.CONNECTION_ID, 0);
    }

    public TerminalNode CONV() {
      return getToken(MySqlParser.CONV, 0);
    }

    public TerminalNode CONVERT_TZ() {
      return getToken(MySqlParser.CONVERT_TZ, 0);
    }

    public TerminalNode COS() {
      return getToken(MySqlParser.COS, 0);
    }

    public TerminalNode COT() {
      return getToken(MySqlParser.COT, 0);
    }

    public TerminalNode COUNT() {
      return getToken(MySqlParser.COUNT, 0);
    }

    public TerminalNode CRC32() {
      return getToken(MySqlParser.CRC32, 0);
    }

    public TerminalNode CREATE_ASYMMETRIC_PRIV_KEY() {
      return getToken(MySqlParser.CREATE_ASYMMETRIC_PRIV_KEY, 0);
    }

    public TerminalNode CREATE_ASYMMETRIC_PUB_KEY() {
      return getToken(MySqlParser.CREATE_ASYMMETRIC_PUB_KEY, 0);
    }

    public TerminalNode CREATE_DH_PARAMETERS() {
      return getToken(MySqlParser.CREATE_DH_PARAMETERS, 0);
    }

    public TerminalNode CREATE_DIGEST() {
      return getToken(MySqlParser.CREATE_DIGEST, 0);
    }

    public TerminalNode CROSSES() {
      return getToken(MySqlParser.CROSSES, 0);
    }

    public TerminalNode DATABASE() {
      return getToken(MySqlParser.DATABASE, 0);
    }

    public TerminalNode DATE() {
      return getToken(MySqlParser.DATE, 0);
    }

    public TerminalNode DATEDIFF() {
      return getToken(MySqlParser.DATEDIFF, 0);
    }

    public TerminalNode DATE_FORMAT() {
      return getToken(MySqlParser.DATE_FORMAT, 0);
    }

    public TerminalNode DAY() {
      return getToken(MySqlParser.DAY, 0);
    }

    public TerminalNode DAYNAME() {
      return getToken(MySqlParser.DAYNAME, 0);
    }

    public TerminalNode DAYOFMONTH() {
      return getToken(MySqlParser.DAYOFMONTH, 0);
    }

    public TerminalNode DAYOFWEEK() {
      return getToken(MySqlParser.DAYOFWEEK, 0);
    }

    public TerminalNode DAYOFYEAR() {
      return getToken(MySqlParser.DAYOFYEAR, 0);
    }

    public TerminalNode DECODE() {
      return getToken(MySqlParser.DECODE, 0);
    }

    public TerminalNode DEGREES() {
      return getToken(MySqlParser.DEGREES, 0);
    }

    public TerminalNode DES_DECRYPT() {
      return getToken(MySqlParser.DES_DECRYPT, 0);
    }

    public TerminalNode DES_ENCRYPT() {
      return getToken(MySqlParser.DES_ENCRYPT, 0);
    }

    public TerminalNode DIMENSION() {
      return getToken(MySqlParser.DIMENSION, 0);
    }

    public TerminalNode DISJOINT() {
      return getToken(MySqlParser.DISJOINT, 0);
    }

    public TerminalNode ELT() {
      return getToken(MySqlParser.ELT, 0);
    }

    public TerminalNode ENCODE() {
      return getToken(MySqlParser.ENCODE, 0);
    }

    public TerminalNode ENCRYPT() {
      return getToken(MySqlParser.ENCRYPT, 0);
    }

    public TerminalNode ENDPOINT() {
      return getToken(MySqlParser.ENDPOINT, 0);
    }

    public TerminalNode ENVELOPE() {
      return getToken(MySqlParser.ENVELOPE, 0);
    }

    public TerminalNode EQUALS() {
      return getToken(MySqlParser.EQUALS, 0);
    }

    public TerminalNode EXP() {
      return getToken(MySqlParser.EXP, 0);
    }

    public TerminalNode EXPORT_SET() {
      return getToken(MySqlParser.EXPORT_SET, 0);
    }

    public TerminalNode EXTERIORRING() {
      return getToken(MySqlParser.EXTERIORRING, 0);
    }

    public TerminalNode EXTRACTVALUE() {
      return getToken(MySqlParser.EXTRACTVALUE, 0);
    }

    public TerminalNode FIELD() {
      return getToken(MySqlParser.FIELD, 0);
    }

    public TerminalNode FIND_IN_SET() {
      return getToken(MySqlParser.FIND_IN_SET, 0);
    }

    public TerminalNode FLOOR() {
      return getToken(MySqlParser.FLOOR, 0);
    }

    public TerminalNode FORMAT() {
      return getToken(MySqlParser.FORMAT, 0);
    }

    public TerminalNode FOUND_ROWS() {
      return getToken(MySqlParser.FOUND_ROWS, 0);
    }

    public TerminalNode FROM_BASE64() {
      return getToken(MySqlParser.FROM_BASE64, 0);
    }

    public TerminalNode FROM_DAYS() {
      return getToken(MySqlParser.FROM_DAYS, 0);
    }

    public TerminalNode FROM_UNIXTIME() {
      return getToken(MySqlParser.FROM_UNIXTIME, 0);
    }

    public TerminalNode GEOMCOLLFROMTEXT() {
      return getToken(MySqlParser.GEOMCOLLFROMTEXT, 0);
    }

    public TerminalNode GEOMCOLLFROMWKB() {
      return getToken(MySqlParser.GEOMCOLLFROMWKB, 0);
    }

    public TerminalNode GEOMETRYCOLLECTION() {
      return getToken(MySqlParser.GEOMETRYCOLLECTION, 0);
    }

    public TerminalNode GEOMETRYCOLLECTIONFROMTEXT() {
      return getToken(MySqlParser.GEOMETRYCOLLECTIONFROMTEXT, 0);
    }

    public TerminalNode GEOMETRYCOLLECTIONFROMWKB() {
      return getToken(MySqlParser.GEOMETRYCOLLECTIONFROMWKB, 0);
    }

    public TerminalNode GEOMETRYFROMTEXT() {
      return getToken(MySqlParser.GEOMETRYFROMTEXT, 0);
    }

    public TerminalNode GEOMETRYFROMWKB() {
      return getToken(MySqlParser.GEOMETRYFROMWKB, 0);
    }

    public TerminalNode GEOMETRYN() {
      return getToken(MySqlParser.GEOMETRYN, 0);
    }

    public TerminalNode GEOMETRYTYPE() {
      return getToken(MySqlParser.GEOMETRYTYPE, 0);
    }

    public TerminalNode GEOMFROMTEXT() {
      return getToken(MySqlParser.GEOMFROMTEXT, 0);
    }

    public TerminalNode GEOMFROMWKB() {
      return getToken(MySqlParser.GEOMFROMWKB, 0);
    }

    public TerminalNode GET_FORMAT() {
      return getToken(MySqlParser.GET_FORMAT, 0);
    }

    public TerminalNode GET_LOCK() {
      return getToken(MySqlParser.GET_LOCK, 0);
    }

    public TerminalNode GLENGTH() {
      return getToken(MySqlParser.GLENGTH, 0);
    }

    public TerminalNode GREATEST() {
      return getToken(MySqlParser.GREATEST, 0);
    }

    public TerminalNode GTID_SUBSET() {
      return getToken(MySqlParser.GTID_SUBSET, 0);
    }

    public TerminalNode GTID_SUBTRACT() {
      return getToken(MySqlParser.GTID_SUBTRACT, 0);
    }

    public TerminalNode HEX() {
      return getToken(MySqlParser.HEX, 0);
    }

    public TerminalNode HOUR() {
      return getToken(MySqlParser.HOUR, 0);
    }

    public TerminalNode IFNULL() {
      return getToken(MySqlParser.IFNULL, 0);
    }

    public TerminalNode INET6_ATON() {
      return getToken(MySqlParser.INET6_ATON, 0);
    }

    public TerminalNode INET6_NTOA() {
      return getToken(MySqlParser.INET6_NTOA, 0);
    }

    public TerminalNode INET_ATON() {
      return getToken(MySqlParser.INET_ATON, 0);
    }

    public TerminalNode INET_NTOA() {
      return getToken(MySqlParser.INET_NTOA, 0);
    }

    public TerminalNode INSTR() {
      return getToken(MySqlParser.INSTR, 0);
    }

    public TerminalNode INTERIORRINGN() {
      return getToken(MySqlParser.INTERIORRINGN, 0);
    }

    public TerminalNode INTERSECTS() {
      return getToken(MySqlParser.INTERSECTS, 0);
    }

    public TerminalNode ISCLOSED() {
      return getToken(MySqlParser.ISCLOSED, 0);
    }

    public TerminalNode ISEMPTY() {
      return getToken(MySqlParser.ISEMPTY, 0);
    }

    public TerminalNode ISNULL() {
      return getToken(MySqlParser.ISNULL, 0);
    }

    public TerminalNode ISSIMPLE() {
      return getToken(MySqlParser.ISSIMPLE, 0);
    }

    public TerminalNode IS_FREE_LOCK() {
      return getToken(MySqlParser.IS_FREE_LOCK, 0);
    }

    public TerminalNode IS_IPV4() {
      return getToken(MySqlParser.IS_IPV4, 0);
    }

    public TerminalNode IS_IPV4_COMPAT() {
      return getToken(MySqlParser.IS_IPV4_COMPAT, 0);
    }

    public TerminalNode IS_IPV4_MAPPED() {
      return getToken(MySqlParser.IS_IPV4_MAPPED, 0);
    }

    public TerminalNode IS_IPV6() {
      return getToken(MySqlParser.IS_IPV6, 0);
    }

    public TerminalNode IS_USED_LOCK() {
      return getToken(MySqlParser.IS_USED_LOCK, 0);
    }

    public TerminalNode LAST_INSERT_ID() {
      return getToken(MySqlParser.LAST_INSERT_ID, 0);
    }

    public TerminalNode LCASE() {
      return getToken(MySqlParser.LCASE, 0);
    }

    public TerminalNode LEAST() {
      return getToken(MySqlParser.LEAST, 0);
    }

    public TerminalNode LEFT() {
      return getToken(MySqlParser.LEFT, 0);
    }

    public TerminalNode LENGTH() {
      return getToken(MySqlParser.LENGTH, 0);
    }

    public TerminalNode LINEFROMTEXT() {
      return getToken(MySqlParser.LINEFROMTEXT, 0);
    }

    public TerminalNode LINEFROMWKB() {
      return getToken(MySqlParser.LINEFROMWKB, 0);
    }

    public TerminalNode LINESTRING() {
      return getToken(MySqlParser.LINESTRING, 0);
    }

    public TerminalNode LINESTRINGFROMTEXT() {
      return getToken(MySqlParser.LINESTRINGFROMTEXT, 0);
    }

    public TerminalNode LINESTRINGFROMWKB() {
      return getToken(MySqlParser.LINESTRINGFROMWKB, 0);
    }

    public TerminalNode LN() {
      return getToken(MySqlParser.LN, 0);
    }

    public TerminalNode LOAD_FILE() {
      return getToken(MySqlParser.LOAD_FILE, 0);
    }

    public TerminalNode LOCATE() {
      return getToken(MySqlParser.LOCATE, 0);
    }

    public TerminalNode LOG() {
      return getToken(MySqlParser.LOG, 0);
    }

    public TerminalNode LOG10() {
      return getToken(MySqlParser.LOG10, 0);
    }

    public TerminalNode LOG2() {
      return getToken(MySqlParser.LOG2, 0);
    }

    public TerminalNode LOWER() {
      return getToken(MySqlParser.LOWER, 0);
    }

    public TerminalNode LPAD() {
      return getToken(MySqlParser.LPAD, 0);
    }

    public TerminalNode LTRIM() {
      return getToken(MySqlParser.LTRIM, 0);
    }

    public TerminalNode MAKEDATE() {
      return getToken(MySqlParser.MAKEDATE, 0);
    }

    public TerminalNode MAKETIME() {
      return getToken(MySqlParser.MAKETIME, 0);
    }

    public TerminalNode MAKE_SET() {
      return getToken(MySqlParser.MAKE_SET, 0);
    }

    public TerminalNode MASTER_POS_WAIT() {
      return getToken(MySqlParser.MASTER_POS_WAIT, 0);
    }

    public TerminalNode MBRCONTAINS() {
      return getToken(MySqlParser.MBRCONTAINS, 0);
    }

    public TerminalNode MBRDISJOINT() {
      return getToken(MySqlParser.MBRDISJOINT, 0);
    }

    public TerminalNode MBREQUAL() {
      return getToken(MySqlParser.MBREQUAL, 0);
    }

    public TerminalNode MBRINTERSECTS() {
      return getToken(MySqlParser.MBRINTERSECTS, 0);
    }

    public TerminalNode MBROVERLAPS() {
      return getToken(MySqlParser.MBROVERLAPS, 0);
    }

    public TerminalNode MBRTOUCHES() {
      return getToken(MySqlParser.MBRTOUCHES, 0);
    }

    public TerminalNode MBRWITHIN() {
      return getToken(MySqlParser.MBRWITHIN, 0);
    }

    public TerminalNode MD5() {
      return getToken(MySqlParser.MD5, 0);
    }

    public TerminalNode MICROSECOND() {
      return getToken(MySqlParser.MICROSECOND, 0);
    }

    public TerminalNode MINUTE() {
      return getToken(MySqlParser.MINUTE, 0);
    }

    public TerminalNode MLINEFROMTEXT() {
      return getToken(MySqlParser.MLINEFROMTEXT, 0);
    }

    public TerminalNode MLINEFROMWKB() {
      return getToken(MySqlParser.MLINEFROMWKB, 0);
    }

    public TerminalNode MONTH() {
      return getToken(MySqlParser.MONTH, 0);
    }

    public TerminalNode MONTHNAME() {
      return getToken(MySqlParser.MONTHNAME, 0);
    }

    public TerminalNode MPOINTFROMTEXT() {
      return getToken(MySqlParser.MPOINTFROMTEXT, 0);
    }

    public TerminalNode MPOINTFROMWKB() {
      return getToken(MySqlParser.MPOINTFROMWKB, 0);
    }

    public TerminalNode MPOLYFROMTEXT() {
      return getToken(MySqlParser.MPOLYFROMTEXT, 0);
    }

    public TerminalNode MPOLYFROMWKB() {
      return getToken(MySqlParser.MPOLYFROMWKB, 0);
    }

    public TerminalNode MULTILINESTRING() {
      return getToken(MySqlParser.MULTILINESTRING, 0);
    }

    public TerminalNode MULTILINESTRINGFROMTEXT() {
      return getToken(MySqlParser.MULTILINESTRINGFROMTEXT, 0);
    }

    public TerminalNode MULTILINESTRINGFROMWKB() {
      return getToken(MySqlParser.MULTILINESTRINGFROMWKB, 0);
    }

    public TerminalNode MULTIPOINT() {
      return getToken(MySqlParser.MULTIPOINT, 0);
    }

    public TerminalNode MULTIPOINTFROMTEXT() {
      return getToken(MySqlParser.MULTIPOINTFROMTEXT, 0);
    }

    public TerminalNode MULTIPOINTFROMWKB() {
      return getToken(MySqlParser.MULTIPOINTFROMWKB, 0);
    }

    public TerminalNode MULTIPOLYGON() {
      return getToken(MySqlParser.MULTIPOLYGON, 0);
    }

    public TerminalNode MULTIPOLYGONFROMTEXT() {
      return getToken(MySqlParser.MULTIPOLYGONFROMTEXT, 0);
    }

    public TerminalNode MULTIPOLYGONFROMWKB() {
      return getToken(MySqlParser.MULTIPOLYGONFROMWKB, 0);
    }

    public TerminalNode NAME_CONST() {
      return getToken(MySqlParser.NAME_CONST, 0);
    }

    public TerminalNode NULLIF() {
      return getToken(MySqlParser.NULLIF, 0);
    }

    public TerminalNode NUMGEOMETRIES() {
      return getToken(MySqlParser.NUMGEOMETRIES, 0);
    }

    public TerminalNode NUMINTERIORRINGS() {
      return getToken(MySqlParser.NUMINTERIORRINGS, 0);
    }

    public TerminalNode NUMPOINTS() {
      return getToken(MySqlParser.NUMPOINTS, 0);
    }

    public TerminalNode OCT() {
      return getToken(MySqlParser.OCT, 0);
    }

    public TerminalNode OCTET_LENGTH() {
      return getToken(MySqlParser.OCTET_LENGTH, 0);
    }

    public TerminalNode ORD() {
      return getToken(MySqlParser.ORD, 0);
    }

    public TerminalNode OVERLAPS() {
      return getToken(MySqlParser.OVERLAPS, 0);
    }

    public TerminalNode PERIOD_ADD() {
      return getToken(MySqlParser.PERIOD_ADD, 0);
    }

    public TerminalNode PERIOD_DIFF() {
      return getToken(MySqlParser.PERIOD_DIFF, 0);
    }

    public TerminalNode PI() {
      return getToken(MySqlParser.PI, 0);
    }

    public TerminalNode POINT() {
      return getToken(MySqlParser.POINT, 0);
    }

    public TerminalNode POINTFROMTEXT() {
      return getToken(MySqlParser.POINTFROMTEXT, 0);
    }

    public TerminalNode POINTFROMWKB() {
      return getToken(MySqlParser.POINTFROMWKB, 0);
    }

    public TerminalNode POINTN() {
      return getToken(MySqlParser.POINTN, 0);
    }

    public TerminalNode POLYFROMTEXT() {
      return getToken(MySqlParser.POLYFROMTEXT, 0);
    }

    public TerminalNode POLYFROMWKB() {
      return getToken(MySqlParser.POLYFROMWKB, 0);
    }

    public TerminalNode POLYGON() {
      return getToken(MySqlParser.POLYGON, 0);
    }

    public TerminalNode POLYGONFROMTEXT() {
      return getToken(MySqlParser.POLYGONFROMTEXT, 0);
    }

    public TerminalNode POLYGONFROMWKB() {
      return getToken(MySqlParser.POLYGONFROMWKB, 0);
    }

    public TerminalNode POSITION() {
      return getToken(MySqlParser.POSITION, 0);
    }

    public TerminalNode POW() {
      return getToken(MySqlParser.POW, 0);
    }

    public TerminalNode POWER() {
      return getToken(MySqlParser.POWER, 0);
    }

    public TerminalNode QUARTER() {
      return getToken(MySqlParser.QUARTER, 0);
    }

    public TerminalNode QUOTE() {
      return getToken(MySqlParser.QUOTE, 0);
    }

    public TerminalNode RADIANS() {
      return getToken(MySqlParser.RADIANS, 0);
    }

    public TerminalNode RAND() {
      return getToken(MySqlParser.RAND, 0);
    }

    public TerminalNode RANDOM_BYTES() {
      return getToken(MySqlParser.RANDOM_BYTES, 0);
    }

    public TerminalNode RELEASE_LOCK() {
      return getToken(MySqlParser.RELEASE_LOCK, 0);
    }

    public TerminalNode REVERSE() {
      return getToken(MySqlParser.REVERSE, 0);
    }

    public TerminalNode RIGHT() {
      return getToken(MySqlParser.RIGHT, 0);
    }

    public TerminalNode ROUND() {
      return getToken(MySqlParser.ROUND, 0);
    }

    public TerminalNode ROW_COUNT() {
      return getToken(MySqlParser.ROW_COUNT, 0);
    }

    public TerminalNode RPAD() {
      return getToken(MySqlParser.RPAD, 0);
    }

    public TerminalNode RTRIM() {
      return getToken(MySqlParser.RTRIM, 0);
    }

    public TerminalNode SECOND() {
      return getToken(MySqlParser.SECOND, 0);
    }

    public TerminalNode SEC_TO_TIME() {
      return getToken(MySqlParser.SEC_TO_TIME, 0);
    }

    public TerminalNode SESSION_USER() {
      return getToken(MySqlParser.SESSION_USER, 0);
    }

    public TerminalNode SHA() {
      return getToken(MySqlParser.SHA, 0);
    }

    public TerminalNode SHA1() {
      return getToken(MySqlParser.SHA1, 0);
    }

    public TerminalNode SHA2() {
      return getToken(MySqlParser.SHA2, 0);
    }

    public TerminalNode SIGN() {
      return getToken(MySqlParser.SIGN, 0);
    }

    public TerminalNode SIN() {
      return getToken(MySqlParser.SIN, 0);
    }

    public TerminalNode SLEEP() {
      return getToken(MySqlParser.SLEEP, 0);
    }

    public TerminalNode SOUNDEX() {
      return getToken(MySqlParser.SOUNDEX, 0);
    }

    public TerminalNode SQL_THREAD_WAIT_AFTER_GTIDS() {
      return getToken(MySqlParser.SQL_THREAD_WAIT_AFTER_GTIDS, 0);
    }

    public TerminalNode SQRT() {
      return getToken(MySqlParser.SQRT, 0);
    }

    public TerminalNode SRID() {
      return getToken(MySqlParser.SRID, 0);
    }

    public TerminalNode STARTPOINT() {
      return getToken(MySqlParser.STARTPOINT, 0);
    }

    public TerminalNode STRCMP() {
      return getToken(MySqlParser.STRCMP, 0);
    }

    public TerminalNode STR_TO_DATE() {
      return getToken(MySqlParser.STR_TO_DATE, 0);
    }

    public TerminalNode ST_AREA() {
      return getToken(MySqlParser.ST_AREA, 0);
    }

    public TerminalNode ST_ASBINARY() {
      return getToken(MySqlParser.ST_ASBINARY, 0);
    }

    public TerminalNode ST_ASTEXT() {
      return getToken(MySqlParser.ST_ASTEXT, 0);
    }

    public TerminalNode ST_ASWKB() {
      return getToken(MySqlParser.ST_ASWKB, 0);
    }

    public TerminalNode ST_ASWKT() {
      return getToken(MySqlParser.ST_ASWKT, 0);
    }

    public TerminalNode ST_BUFFER() {
      return getToken(MySqlParser.ST_BUFFER, 0);
    }

    public TerminalNode ST_CENTROID() {
      return getToken(MySqlParser.ST_CENTROID, 0);
    }

    public TerminalNode ST_CONTAINS() {
      return getToken(MySqlParser.ST_CONTAINS, 0);
    }

    public TerminalNode ST_CROSSES() {
      return getToken(MySqlParser.ST_CROSSES, 0);
    }

    public TerminalNode ST_DIFFERENCE() {
      return getToken(MySqlParser.ST_DIFFERENCE, 0);
    }

    public TerminalNode ST_DIMENSION() {
      return getToken(MySqlParser.ST_DIMENSION, 0);
    }

    public TerminalNode ST_DISJOINT() {
      return getToken(MySqlParser.ST_DISJOINT, 0);
    }

    public TerminalNode ST_DISTANCE() {
      return getToken(MySqlParser.ST_DISTANCE, 0);
    }

    public TerminalNode ST_ENDPOINT() {
      return getToken(MySqlParser.ST_ENDPOINT, 0);
    }

    public TerminalNode ST_ENVELOPE() {
      return getToken(MySqlParser.ST_ENVELOPE, 0);
    }

    public TerminalNode ST_EQUALS() {
      return getToken(MySqlParser.ST_EQUALS, 0);
    }

    public TerminalNode ST_EXTERIORRING() {
      return getToken(MySqlParser.ST_EXTERIORRING, 0);
    }

    public TerminalNode ST_GEOMCOLLFROMTEXT() {
      return getToken(MySqlParser.ST_GEOMCOLLFROMTEXT, 0);
    }

    public TerminalNode ST_GEOMCOLLFROMTXT() {
      return getToken(MySqlParser.ST_GEOMCOLLFROMTXT, 0);
    }

    public TerminalNode ST_GEOMCOLLFROMWKB() {
      return getToken(MySqlParser.ST_GEOMCOLLFROMWKB, 0);
    }

    public TerminalNode ST_GEOMETRYCOLLECTIONFROMTEXT() {
      return getToken(MySqlParser.ST_GEOMETRYCOLLECTIONFROMTEXT, 0);
    }

    public TerminalNode ST_GEOMETRYCOLLECTIONFROMWKB() {
      return getToken(MySqlParser.ST_GEOMETRYCOLLECTIONFROMWKB, 0);
    }

    public TerminalNode ST_GEOMETRYFROMTEXT() {
      return getToken(MySqlParser.ST_GEOMETRYFROMTEXT, 0);
    }

    public TerminalNode ST_GEOMETRYFROMWKB() {
      return getToken(MySqlParser.ST_GEOMETRYFROMWKB, 0);
    }

    public TerminalNode ST_GEOMETRYN() {
      return getToken(MySqlParser.ST_GEOMETRYN, 0);
    }

    public TerminalNode ST_GEOMETRYTYPE() {
      return getToken(MySqlParser.ST_GEOMETRYTYPE, 0);
    }

    public TerminalNode ST_GEOMFROMTEXT() {
      return getToken(MySqlParser.ST_GEOMFROMTEXT, 0);
    }

    public TerminalNode ST_GEOMFROMWKB() {
      return getToken(MySqlParser.ST_GEOMFROMWKB, 0);
    }

    public TerminalNode ST_INTERIORRINGN() {
      return getToken(MySqlParser.ST_INTERIORRINGN, 0);
    }

    public TerminalNode ST_INTERSECTION() {
      return getToken(MySqlParser.ST_INTERSECTION, 0);
    }

    public TerminalNode ST_INTERSECTS() {
      return getToken(MySqlParser.ST_INTERSECTS, 0);
    }

    public TerminalNode ST_ISCLOSED() {
      return getToken(MySqlParser.ST_ISCLOSED, 0);
    }

    public TerminalNode ST_ISEMPTY() {
      return getToken(MySqlParser.ST_ISEMPTY, 0);
    }

    public TerminalNode ST_ISSIMPLE() {
      return getToken(MySqlParser.ST_ISSIMPLE, 0);
    }

    public TerminalNode ST_LINEFROMTEXT() {
      return getToken(MySqlParser.ST_LINEFROMTEXT, 0);
    }

    public TerminalNode ST_LINEFROMWKB() {
      return getToken(MySqlParser.ST_LINEFROMWKB, 0);
    }

    public TerminalNode ST_LINESTRINGFROMTEXT() {
      return getToken(MySqlParser.ST_LINESTRINGFROMTEXT, 0);
    }

    public TerminalNode ST_LINESTRINGFROMWKB() {
      return getToken(MySqlParser.ST_LINESTRINGFROMWKB, 0);
    }

    public TerminalNode ST_NUMGEOMETRIES() {
      return getToken(MySqlParser.ST_NUMGEOMETRIES, 0);
    }

    public TerminalNode ST_NUMINTERIORRING() {
      return getToken(MySqlParser.ST_NUMINTERIORRING, 0);
    }

    public TerminalNode ST_NUMINTERIORRINGS() {
      return getToken(MySqlParser.ST_NUMINTERIORRINGS, 0);
    }

    public TerminalNode ST_NUMPOINTS() {
      return getToken(MySqlParser.ST_NUMPOINTS, 0);
    }

    public TerminalNode ST_OVERLAPS() {
      return getToken(MySqlParser.ST_OVERLAPS, 0);
    }

    public TerminalNode ST_POINTFROMTEXT() {
      return getToken(MySqlParser.ST_POINTFROMTEXT, 0);
    }

    public TerminalNode ST_POINTFROMWKB() {
      return getToken(MySqlParser.ST_POINTFROMWKB, 0);
    }

    public TerminalNode ST_POINTN() {
      return getToken(MySqlParser.ST_POINTN, 0);
    }

    public TerminalNode ST_POLYFROMTEXT() {
      return getToken(MySqlParser.ST_POLYFROMTEXT, 0);
    }

    public TerminalNode ST_POLYFROMWKB() {
      return getToken(MySqlParser.ST_POLYFROMWKB, 0);
    }

    public TerminalNode ST_POLYGONFROMTEXT() {
      return getToken(MySqlParser.ST_POLYGONFROMTEXT, 0);
    }

    public TerminalNode ST_POLYGONFROMWKB() {
      return getToken(MySqlParser.ST_POLYGONFROMWKB, 0);
    }

    public TerminalNode ST_SRID() {
      return getToken(MySqlParser.ST_SRID, 0);
    }

    public TerminalNode ST_STARTPOINT() {
      return getToken(MySqlParser.ST_STARTPOINT, 0);
    }

    public TerminalNode ST_SYMDIFFERENCE() {
      return getToken(MySqlParser.ST_SYMDIFFERENCE, 0);
    }

    public TerminalNode ST_TOUCHES() {
      return getToken(MySqlParser.ST_TOUCHES, 0);
    }

    public TerminalNode ST_UNION() {
      return getToken(MySqlParser.ST_UNION, 0);
    }

    public TerminalNode ST_WITHIN() {
      return getToken(MySqlParser.ST_WITHIN, 0);
    }

    public TerminalNode ST_X() {
      return getToken(MySqlParser.ST_X, 0);
    }

    public TerminalNode ST_Y() {
      return getToken(MySqlParser.ST_Y, 0);
    }

    public TerminalNode SUBDATE() {
      return getToken(MySqlParser.SUBDATE, 0);
    }

    public TerminalNode SUBSTRING_INDEX() {
      return getToken(MySqlParser.SUBSTRING_INDEX, 0);
    }

    public TerminalNode SUBTIME() {
      return getToken(MySqlParser.SUBTIME, 0);
    }

    public TerminalNode SYSTEM_USER() {
      return getToken(MySqlParser.SYSTEM_USER, 0);
    }

    public TerminalNode TAN() {
      return getToken(MySqlParser.TAN, 0);
    }

    public TerminalNode TIME() {
      return getToken(MySqlParser.TIME, 0);
    }

    public TerminalNode TIMEDIFF() {
      return getToken(MySqlParser.TIMEDIFF, 0);
    }

    public TerminalNode TIMESTAMP() {
      return getToken(MySqlParser.TIMESTAMP, 0);
    }

    public TerminalNode TIMESTAMPADD() {
      return getToken(MySqlParser.TIMESTAMPADD, 0);
    }

    public TerminalNode TIMESTAMPDIFF() {
      return getToken(MySqlParser.TIMESTAMPDIFF, 0);
    }

    public TerminalNode TIME_FORMAT() {
      return getToken(MySqlParser.TIME_FORMAT, 0);
    }

    public TerminalNode TIME_TO_SEC() {
      return getToken(MySqlParser.TIME_TO_SEC, 0);
    }

    public TerminalNode TOUCHES() {
      return getToken(MySqlParser.TOUCHES, 0);
    }

    public TerminalNode TO_BASE64() {
      return getToken(MySqlParser.TO_BASE64, 0);
    }

    public TerminalNode TO_DAYS() {
      return getToken(MySqlParser.TO_DAYS, 0);
    }

    public TerminalNode TO_SECONDS() {
      return getToken(MySqlParser.TO_SECONDS, 0);
    }

    public TerminalNode UCASE() {
      return getToken(MySqlParser.UCASE, 0);
    }

    public TerminalNode UNCOMPRESS() {
      return getToken(MySqlParser.UNCOMPRESS, 0);
    }

    public TerminalNode UNCOMPRESSED_LENGTH() {
      return getToken(MySqlParser.UNCOMPRESSED_LENGTH, 0);
    }

    public TerminalNode UNHEX() {
      return getToken(MySqlParser.UNHEX, 0);
    }

    public TerminalNode UNIX_TIMESTAMP() {
      return getToken(MySqlParser.UNIX_TIMESTAMP, 0);
    }

    public TerminalNode UPDATEXML() {
      return getToken(MySqlParser.UPDATEXML, 0);
    }

    public TerminalNode UPPER() {
      return getToken(MySqlParser.UPPER, 0);
    }

    public TerminalNode UUID() {
      return getToken(MySqlParser.UUID, 0);
    }

    public TerminalNode UUID_SHORT() {
      return getToken(MySqlParser.UUID_SHORT, 0);
    }

    public TerminalNode VALIDATE_PASSWORD_STRENGTH() {
      return getToken(MySqlParser.VALIDATE_PASSWORD_STRENGTH, 0);
    }

    public TerminalNode VERSION() {
      return getToken(MySqlParser.VERSION, 0);
    }

    public TerminalNode WAIT_UNTIL_SQL_THREAD_AFTER_GTIDS() {
      return getToken(MySqlParser.WAIT_UNTIL_SQL_THREAD_AFTER_GTIDS, 0);
    }

    public TerminalNode WEEK() {
      return getToken(MySqlParser.WEEK, 0);
    }

    public TerminalNode WEEKDAY() {
      return getToken(MySqlParser.WEEKDAY, 0);
    }

    public TerminalNode WEEKOFYEAR() {
      return getToken(MySqlParser.WEEKOFYEAR, 0);
    }

    public TerminalNode WEIGHT_STRING() {
      return getToken(MySqlParser.WEIGHT_STRING, 0);
    }

    public TerminalNode WITHIN() {
      return getToken(MySqlParser.WITHIN, 0);
    }

    public TerminalNode YEAR() {
      return getToken(MySqlParser.YEAR, 0);
    }

    public TerminalNode YEARWEEK() {
      return getToken(MySqlParser.YEARWEEK, 0);
    }

    public TerminalNode Y_FUNCTION() {
      return getToken(MySqlParser.Y_FUNCTION, 0);
    }

    public TerminalNode X_FUNCTION() {
      return getToken(MySqlParser.X_FUNCTION, 0);
    }

    public FunctionNameBaseContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_functionNameBase;
    }

    @Override
    public void enterRule(ParseTreeListener listener) {
      if (listener instanceof MySqlParserListener)
        ((MySqlParserListener) listener).enterFunctionNameBase(this);
    }

    @Override
    public void exitRule(ParseTreeListener listener) {
      if (listener instanceof MySqlParserListener)
        ((MySqlParserListener) listener).exitFunctionNameBase(this);
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof MySqlParserVisitor)
        return ((MySqlParserVisitor<? extends T>) visitor).visitFunctionNameBase(this);
      else return visitor.visitChildren(this);
    }
  }

  public final FunctionNameBaseContext functionNameBase() throws RecognitionException {
    FunctionNameBaseContext _localctx = new FunctionNameBaseContext(_ctx, getState());
    enterRule(_localctx, 78, RULE_functionNameBase);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
        setState(655);
        _la = _input.LA(1);
        if (!(_la == DATABASE
            || _la == LEFT
            || _la == RIGHT
            || ((((_la - 182)) & ~0x3f) == 0
                && ((1L << (_la - 182))
                        & ((1L << (DATE - 182))
                            | (1L << (TIME - 182))
                            | (1L << (TIMESTAMP - 182))
                            | (1L << (YEAR - 182))
                            | (1L << (COUNT - 182))
                            | (1L << (POSITION - 182))))
                    != 0)
            || ((((_la - 554)) & ~0x3f) == 0
                && ((1L << (_la - 554))
                        & ((1L << (QUARTER - 554))
                            | (1L << (MONTH - 554))
                            | (1L << (DAY - 554))
                            | (1L << (HOUR - 554))
                            | (1L << (MINUTE - 554))
                            | (1L << (WEEK - 554))
                            | (1L << (SECOND - 554))
                            | (1L << (MICROSECOND - 554))))
                    != 0)
            || ((((_la - 626)) & ~0x3f) == 0
                && ((1L << (_la - 626))
                        & ((1L << (GEOMETRYCOLLECTION - 626))
                            | (1L << (LINESTRING - 626))
                            | (1L << (MULTILINESTRING - 626))
                            | (1L << (MULTIPOINT - 626))
                            | (1L << (MULTIPOLYGON - 626))
                            | (1L << (POINT - 626))
                            | (1L << (POLYGON - 626))
                            | (1L << (ABS - 626))
                            | (1L << (ACOS - 626))
                            | (1L << (ADDDATE - 626))
                            | (1L << (ADDTIME - 626))
                            | (1L << (AES_DECRYPT - 626))
                            | (1L << (AES_ENCRYPT - 626))
                            | (1L << (AREA - 626))
                            | (1L << (ASBINARY - 626))
                            | (1L << (ASIN - 626))
                            | (1L << (ASTEXT - 626))
                            | (1L << (ASWKB - 626))
                            | (1L << (ASWKT - 626))
                            | (1L << (ASYMMETRIC_DECRYPT - 626))
                            | (1L << (ASYMMETRIC_DERIVE - 626))
                            | (1L << (ASYMMETRIC_ENCRYPT - 626))
                            | (1L << (ASYMMETRIC_SIGN - 626))
                            | (1L << (ASYMMETRIC_VERIFY - 626))
                            | (1L << (ATAN - 626))
                            | (1L << (ATAN2 - 626))
                            | (1L << (BENCHMARK - 626))
                            | (1L << (BIN - 626))
                            | (1L << (BIT_COUNT - 626))
                            | (1L << (BIT_LENGTH - 626))
                            | (1L << (BUFFER - 626))
                            | (1L << (CEIL - 626))
                            | (1L << (CEILING - 626))
                            | (1L << (CENTROID - 626))
                            | (1L << (CHARACTER_LENGTH - 626))
                            | (1L << (CHARSET - 626))
                            | (1L << (CHAR_LENGTH - 626))
                            | (1L << (COERCIBILITY - 626))
                            | (1L << (COLLATION - 626))
                            | (1L << (COMPRESS - 626))
                            | (1L << (CONCAT - 626))
                            | (1L << (CONCAT_WS - 626))
                            | (1L << (CONNECTION_ID - 626))
                            | (1L << (CONV - 626))
                            | (1L << (CONVERT_TZ - 626))
                            | (1L << (COS - 626))
                            | (1L << (COT - 626))
                            | (1L << (CRC32 - 626))
                            | (1L << (CREATE_ASYMMETRIC_PRIV_KEY - 626))
                            | (1L << (CREATE_ASYMMETRIC_PUB_KEY - 626))
                            | (1L << (CREATE_DH_PARAMETERS - 626))
                            | (1L << (CREATE_DIGEST - 626))
                            | (1L << (CROSSES - 626))
                            | (1L << (DATEDIFF - 626))
                            | (1L << (DATE_FORMAT - 626))
                            | (1L << (DAYNAME - 626))
                            | (1L << (DAYOFMONTH - 626))
                            | (1L << (DAYOFWEEK - 626))
                            | (1L << (DAYOFYEAR - 626))
                            | (1L << (DECODE - 626))
                            | (1L << (DEGREES - 626))
                            | (1L << (DES_DECRYPT - 626))
                            | (1L << (DES_ENCRYPT - 626))
                            | (1L << (DIMENSION - 626))))
                    != 0)
            || ((((_la - 690)) & ~0x3f) == 0
                && ((1L << (_la - 690))
                        & ((1L << (DISJOINT - 690))
                            | (1L << (ELT - 690))
                            | (1L << (ENCODE - 690))
                            | (1L << (ENCRYPT - 690))
                            | (1L << (ENDPOINT - 690))
                            | (1L << (ENVELOPE - 690))
                            | (1L << (EQUALS - 690))
                            | (1L << (EXP - 690))
                            | (1L << (EXPORT_SET - 690))
                            | (1L << (EXTERIORRING - 690))
                            | (1L << (EXTRACTVALUE - 690))
                            | (1L << (FIELD - 690))
                            | (1L << (FIND_IN_SET - 690))
                            | (1L << (FLOOR - 690))
                            | (1L << (FORMAT - 690))
                            | (1L << (FOUND_ROWS - 690))
                            | (1L << (FROM_BASE64 - 690))
                            | (1L << (FROM_DAYS - 690))
                            | (1L << (FROM_UNIXTIME - 690))
                            | (1L << (GEOMCOLLFROMTEXT - 690))
                            | (1L << (GEOMCOLLFROMWKB - 690))
                            | (1L << (GEOMETRYCOLLECTIONFROMTEXT - 690))
                            | (1L << (GEOMETRYCOLLECTIONFROMWKB - 690))
                            | (1L << (GEOMETRYFROMTEXT - 690))
                            | (1L << (GEOMETRYFROMWKB - 690))
                            | (1L << (GEOMETRYN - 690))
                            | (1L << (GEOMETRYTYPE - 690))
                            | (1L << (GEOMFROMTEXT - 690))
                            | (1L << (GEOMFROMWKB - 690))
                            | (1L << (GET_FORMAT - 690))
                            | (1L << (GET_LOCK - 690))
                            | (1L << (GLENGTH - 690))
                            | (1L << (GREATEST - 690))
                            | (1L << (GTID_SUBSET - 690))
                            | (1L << (GTID_SUBTRACT - 690))
                            | (1L << (HEX - 690))
                            | (1L << (IFNULL - 690))
                            | (1L << (INET6_ATON - 690))
                            | (1L << (INET6_NTOA - 690))
                            | (1L << (INET_ATON - 690))
                            | (1L << (INET_NTOA - 690))
                            | (1L << (INSTR - 690))
                            | (1L << (INTERIORRINGN - 690))
                            | (1L << (INTERSECTS - 690))
                            | (1L << (ISCLOSED - 690))
                            | (1L << (ISEMPTY - 690))
                            | (1L << (ISNULL - 690))
                            | (1L << (ISSIMPLE - 690))
                            | (1L << (IS_FREE_LOCK - 690))
                            | (1L << (IS_IPV4 - 690))
                            | (1L << (IS_IPV4_COMPAT - 690))
                            | (1L << (IS_IPV4_MAPPED - 690))
                            | (1L << (IS_IPV6 - 690))
                            | (1L << (IS_USED_LOCK - 690))
                            | (1L << (LAST_INSERT_ID - 690))
                            | (1L << (LCASE - 690))
                            | (1L << (LEAST - 690))
                            | (1L << (LENGTH - 690))
                            | (1L << (LINEFROMTEXT - 690))
                            | (1L << (LINEFROMWKB - 690))
                            | (1L << (LINESTRINGFROMTEXT - 690))
                            | (1L << (LINESTRINGFROMWKB - 690))
                            | (1L << (LN - 690))
                            | (1L << (LOAD_FILE - 690))))
                    != 0)
            || ((((_la - 754)) & ~0x3f) == 0
                && ((1L << (_la - 754))
                        & ((1L << (LOCATE - 754))
                            | (1L << (LOG - 754))
                            | (1L << (LOG10 - 754))
                            | (1L << (LOG2 - 754))
                            | (1L << (LOWER - 754))
                            | (1L << (LPAD - 754))
                            | (1L << (LTRIM - 754))
                            | (1L << (MAKEDATE - 754))
                            | (1L << (MAKETIME - 754))
                            | (1L << (MAKE_SET - 754))
                            | (1L << (MASTER_POS_WAIT - 754))
                            | (1L << (MBRCONTAINS - 754))
                            | (1L << (MBRDISJOINT - 754))
                            | (1L << (MBREQUAL - 754))
                            | (1L << (MBRINTERSECTS - 754))
                            | (1L << (MBROVERLAPS - 754))
                            | (1L << (MBRTOUCHES - 754))
                            | (1L << (MBRWITHIN - 754))
                            | (1L << (MD5 - 754))
                            | (1L << (MLINEFROMTEXT - 754))
                            | (1L << (MLINEFROMWKB - 754))
                            | (1L << (MONTHNAME - 754))
                            | (1L << (MPOINTFROMTEXT - 754))
                            | (1L << (MPOINTFROMWKB - 754))
                            | (1L << (MPOLYFROMTEXT - 754))
                            | (1L << (MPOLYFROMWKB - 754))
                            | (1L << (MULTILINESTRINGFROMTEXT - 754))
                            | (1L << (MULTILINESTRINGFROMWKB - 754))
                            | (1L << (MULTIPOINTFROMTEXT - 754))
                            | (1L << (MULTIPOINTFROMWKB - 754))
                            | (1L << (MULTIPOLYGONFROMTEXT - 754))
                            | (1L << (MULTIPOLYGONFROMWKB - 754))
                            | (1L << (NAME_CONST - 754))
                            | (1L << (NULLIF - 754))
                            | (1L << (NUMGEOMETRIES - 754))
                            | (1L << (NUMINTERIORRINGS - 754))
                            | (1L << (NUMPOINTS - 754))
                            | (1L << (OCT - 754))
                            | (1L << (OCTET_LENGTH - 754))
                            | (1L << (ORD - 754))
                            | (1L << (OVERLAPS - 754))
                            | (1L << (PERIOD_ADD - 754))
                            | (1L << (PERIOD_DIFF - 754))
                            | (1L << (PI - 754))
                            | (1L << (POINTFROMTEXT - 754))
                            | (1L << (POINTFROMWKB - 754))
                            | (1L << (POINTN - 754))
                            | (1L << (POLYFROMTEXT - 754))
                            | (1L << (POLYFROMWKB - 754))
                            | (1L << (POLYGONFROMTEXT - 754))
                            | (1L << (POLYGONFROMWKB - 754))
                            | (1L << (POW - 754))
                            | (1L << (POWER - 754))
                            | (1L << (QUOTE - 754))
                            | (1L << (RADIANS - 754))
                            | (1L << (RAND - 754))
                            | (1L << (RANDOM_BYTES - 754))
                            | (1L << (RELEASE_LOCK - 754))
                            | (1L << (REVERSE - 754))
                            | (1L << (ROUND - 754))
                            | (1L << (ROW_COUNT - 754))
                            | (1L << (RPAD - 754))
                            | (1L << (RTRIM - 754))
                            | (1L << (SEC_TO_TIME - 754))))
                    != 0)
            || ((((_la - 818)) & ~0x3f) == 0
                && ((1L << (_la - 818))
                        & ((1L << (SESSION_USER - 818))
                            | (1L << (SHA - 818))
                            | (1L << (SHA1 - 818))
                            | (1L << (SHA2 - 818))
                            | (1L << (SIGN - 818))
                            | (1L << (SIN - 818))
                            | (1L << (SLEEP - 818))
                            | (1L << (SOUNDEX - 818))
                            | (1L << (SQL_THREAD_WAIT_AFTER_GTIDS - 818))
                            | (1L << (SQRT - 818))
                            | (1L << (SRID - 818))
                            | (1L << (STARTPOINT - 818))
                            | (1L << (STRCMP - 818))
                            | (1L << (STR_TO_DATE - 818))
                            | (1L << (ST_AREA - 818))
                            | (1L << (ST_ASBINARY - 818))
                            | (1L << (ST_ASTEXT - 818))
                            | (1L << (ST_ASWKB - 818))
                            | (1L << (ST_ASWKT - 818))
                            | (1L << (ST_BUFFER - 818))
                            | (1L << (ST_CENTROID - 818))
                            | (1L << (ST_CONTAINS - 818))
                            | (1L << (ST_CROSSES - 818))
                            | (1L << (ST_DIFFERENCE - 818))
                            | (1L << (ST_DIMENSION - 818))
                            | (1L << (ST_DISJOINT - 818))
                            | (1L << (ST_DISTANCE - 818))
                            | (1L << (ST_ENDPOINT - 818))
                            | (1L << (ST_ENVELOPE - 818))
                            | (1L << (ST_EQUALS - 818))
                            | (1L << (ST_EXTERIORRING - 818))
                            | (1L << (ST_GEOMCOLLFROMTEXT - 818))
                            | (1L << (ST_GEOMCOLLFROMTXT - 818))
                            | (1L << (ST_GEOMCOLLFROMWKB - 818))
                            | (1L << (ST_GEOMETRYCOLLECTIONFROMTEXT - 818))
                            | (1L << (ST_GEOMETRYCOLLECTIONFROMWKB - 818))
                            | (1L << (ST_GEOMETRYFROMTEXT - 818))
                            | (1L << (ST_GEOMETRYFROMWKB - 818))
                            | (1L << (ST_GEOMETRYN - 818))
                            | (1L << (ST_GEOMETRYTYPE - 818))
                            | (1L << (ST_GEOMFROMTEXT - 818))
                            | (1L << (ST_GEOMFROMWKB - 818))
                            | (1L << (ST_INTERIORRINGN - 818))
                            | (1L << (ST_INTERSECTION - 818))
                            | (1L << (ST_INTERSECTS - 818))
                            | (1L << (ST_ISCLOSED - 818))
                            | (1L << (ST_ISEMPTY - 818))
                            | (1L << (ST_ISSIMPLE - 818))
                            | (1L << (ST_LINEFROMTEXT - 818))
                            | (1L << (ST_LINEFROMWKB - 818))
                            | (1L << (ST_LINESTRINGFROMTEXT - 818))
                            | (1L << (ST_LINESTRINGFROMWKB - 818))
                            | (1L << (ST_NUMGEOMETRIES - 818))
                            | (1L << (ST_NUMINTERIORRING - 818))
                            | (1L << (ST_NUMINTERIORRINGS - 818))
                            | (1L << (ST_NUMPOINTS - 818))
                            | (1L << (ST_OVERLAPS - 818))
                            | (1L << (ST_POINTFROMTEXT - 818))
                            | (1L << (ST_POINTFROMWKB - 818))
                            | (1L << (ST_POINTN - 818))
                            | (1L << (ST_POLYFROMTEXT - 818))
                            | (1L << (ST_POLYFROMWKB - 818))
                            | (1L << (ST_POLYGONFROMTEXT - 818))
                            | (1L << (ST_POLYGONFROMWKB - 818))))
                    != 0)
            || ((((_la - 882)) & ~0x3f) == 0
                && ((1L << (_la - 882))
                        & ((1L << (ST_SRID - 882))
                            | (1L << (ST_STARTPOINT - 882))
                            | (1L << (ST_SYMDIFFERENCE - 882))
                            | (1L << (ST_TOUCHES - 882))
                            | (1L << (ST_UNION - 882))
                            | (1L << (ST_WITHIN - 882))
                            | (1L << (ST_X - 882))
                            | (1L << (ST_Y - 882))
                            | (1L << (SUBDATE - 882))
                            | (1L << (SUBSTRING_INDEX - 882))
                            | (1L << (SUBTIME - 882))
                            | (1L << (SYSTEM_USER - 882))
                            | (1L << (TAN - 882))
                            | (1L << (TIMEDIFF - 882))
                            | (1L << (TIMESTAMPADD - 882))
                            | (1L << (TIMESTAMPDIFF - 882))
                            | (1L << (TIME_FORMAT - 882))
                            | (1L << (TIME_TO_SEC - 882))
                            | (1L << (TOUCHES - 882))
                            | (1L << (TO_BASE64 - 882))
                            | (1L << (TO_DAYS - 882))
                            | (1L << (TO_SECONDS - 882))
                            | (1L << (UCASE - 882))
                            | (1L << (UNCOMPRESS - 882))
                            | (1L << (UNCOMPRESSED_LENGTH - 882))
                            | (1L << (UNHEX - 882))
                            | (1L << (UNIX_TIMESTAMP - 882))
                            | (1L << (UPDATEXML - 882))
                            | (1L << (UPPER - 882))
                            | (1L << (UUID - 882))
                            | (1L << (UUID_SHORT - 882))
                            | (1L << (VALIDATE_PASSWORD_STRENGTH - 882))
                            | (1L << (VERSION - 882))
                            | (1L << (WAIT_UNTIL_SQL_THREAD_AFTER_GTIDS - 882))
                            | (1L << (WEEKDAY - 882))
                            | (1L << (WEEKOFYEAR - 882))
                            | (1L << (WEIGHT_STRING - 882))
                            | (1L << (WITHIN - 882))
                            | (1L << (YEARWEEK - 882))
                            | (1L << (Y_FUNCTION - 882))
                            | (1L << (X_FUNCTION - 882))))
                    != 0))) {
          _errHandler.recoverInline(this);
        } else {
          if (_input.LA(1) == Token.EOF) matchedEOF = true;
          _errHandler.reportMatch(this);
          consume();
        }
      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  public boolean sempred(RuleContext _localctx, int ruleIndex, int predIndex) {
    switch (ruleIndex) {
      case 27:
        return expression_sempred((ExpressionContext) _localctx, predIndex);
      case 28:
        return predicate_sempred((PredicateContext) _localctx, predIndex);
      case 29:
        return expressionAtom_sempred((ExpressionAtomContext) _localctx, predIndex);
    }
    return true;
  }

  private boolean expression_sempred(ExpressionContext _localctx, int predIndex) {
    switch (predIndex) {
      case 0:
        return precpred(_ctx, 3);
    }
    return true;
  }

  private boolean predicate_sempred(PredicateContext _localctx, int predIndex) {
    switch (predIndex) {
      case 1:
        return precpred(_ctx, 6);
      case 2:
        return precpred(_ctx, 5);
      case 3:
        return precpred(_ctx, 4);
      case 4:
        return precpred(_ctx, 2);
      case 5:
        return precpred(_ctx, 8);
      case 6:
        return precpred(_ctx, 7);
      case 7:
        return precpred(_ctx, 3);
    }
    return true;
  }

  private boolean expressionAtom_sempred(ExpressionAtomContext _localctx, int predIndex) {
    switch (predIndex) {
      case 8:
        return precpred(_ctx, 2);
      case 9:
        return precpred(_ctx, 1);
      case 10:
        return precpred(_ctx, 8);
    }
    return true;
  }

  public static final String _serializedATN =
      "\3\u608b\ua72a\u8133\ub9ed\u417c\u3be7\u7786\u5964\3\u03d2\u0294\4\2\t"
          + "\2\4\3\t\3\4\4\t\4\4\5\t\5\4\6\t\6\4\7\t\7\4\b\t\b\4\t\t\t\4\n\t\n\4\13"
          + "\t\13\4\f\t\f\4\r\t\r\4\16\t\16\4\17\t\17\4\20\t\20\4\21\t\21\4\22\t\22"
          + "\4\23\t\23\4\24\t\24\4\25\t\25\4\26\t\26\4\27\t\27\4\30\t\30\4\31\t\31"
          + "\4\32\t\32\4\33\t\33\4\34\t\34\4\35\t\35\4\36\t\36\4\37\t\37\4 \t \4!"
          + "\t!\4\"\t\"\4#\t#\4$\t$\4%\t%\4&\t&\4\'\t\'\4(\t(\4)\t)\3\2\3\2\3\2\3"
          + "\2\3\2\3\2\3\2\3\2\3\2\3\2\3\2\3\2\3\2\5\2`\n\2\3\3\3\3\3\3\3\3\5\3f\n"
          + "\3\3\4\3\4\3\4\5\4k\n\4\5\4m\n\4\3\5\3\5\3\5\3\5\5\5s\n\5\3\6\3\6\5\6"
          + "w\n\6\3\7\3\7\3\7\5\7|\n\7\3\b\3\b\3\b\3\b\3\b\3\b\5\b\u0084\n\b\3\t\3"
          + "\t\3\t\5\t\u0089\n\t\3\n\3\n\3\13\6\13\u008e\n\13\r\13\16\13\u008f\3\f"
          + "\3\f\3\r\5\r\u0095\n\r\3\r\3\r\3\16\5\16\u009a\n\16\3\16\3\16\3\17\3\17"
          + "\3\17\3\17\3\17\3\17\3\17\3\17\3\17\5\17\u00a7\n\17\3\17\5\17\u00aa\n"
          + "\17\3\20\3\20\5\20\u00ae\n\20\3\20\3\20\5\20\u00b2\n\20\3\20\3\20\3\20"
          + "\5\20\u00b7\n\20\3\20\3\20\3\20\5\20\u00bc\n\20\3\20\3\20\5\20\u00c0\n"
          + "\20\5\20\u00c2\n\20\3\21\3\21\3\21\3\21\3\22\3\22\3\22\3\22\3\22\3\22"
          + "\3\23\3\23\3\23\7\23\u00d1\n\23\f\23\16\23\u00d4\13\23\3\24\3\24\3\24"
          + "\5\24\u00d9\n\24\3\24\5\24\u00dc\n\24\3\24\3\24\3\24\5\24\u00e1\n\24\3"
          + "\24\5\24\u00e4\n\24\3\25\3\25\3\25\3\25\5\25\u00ea\n\25\3\25\3\25\3\25"
          + "\3\25\3\25\5\25\u00f1\n\25\3\25\3\25\5\25\u00f5\n\25\3\26\3\26\3\26\3"
          + "\26\3\26\3\26\3\26\3\26\3\26\3\26\3\26\3\26\3\26\3\26\3\26\3\26\3\26\3"
          + "\26\3\26\3\26\3\26\3\26\3\26\3\26\3\26\3\26\3\26\3\26\3\26\3\26\6\26\u0115"
          + "\n\26\r\26\16\26\u0116\3\26\3\26\5\26\u011b\n\26\3\26\3\26\3\26\3\26\6"
          + "\26\u0121\n\26\r\26\16\26\u0122\3\26\3\26\5\26\u0127\n\26\3\26\3\26\3"
          + "\26\3\26\3\26\3\26\3\26\5\26\u0130\n\26\3\26\3\26\3\26\3\26\3\26\3\26"
          + "\5\26\u0138\n\26\3\26\3\26\3\26\5\26\u013d\n\26\3\26\3\26\3\26\3\26\3"
          + "\26\3\26\5\26\u0145\n\26\3\26\3\26\3\26\5\26\u014a\n\26\3\26\3\26\3\26"
          + "\5\26\u014f\n\26\5\26\u0151\n\26\3\26\3\26\3\26\3\26\3\26\3\26\3\26\5"
          + "\26\u015a\n\26\3\26\3\26\3\26\5\26\u015f\n\26\3\26\3\26\3\26\3\26\3\26"
          + "\3\26\5\26\u0167\n\26\3\26\3\26\3\26\5\26\u016c\n\26\3\26\3\26\3\26\3"
          + "\26\3\26\3\26\5\26\u0174\n\26\3\26\3\26\3\26\3\26\3\26\3\26\5\26\u017c"
          + "\n\26\3\26\5\26\u017f\n\26\3\26\3\26\3\26\3\26\3\26\3\26\3\26\3\26\5\26"
          + "\u0189\n\26\3\26\3\26\3\26\3\26\3\26\3\26\3\26\3\26\3\26\5\26\u0194\n"
          + "\26\3\27\3\27\3\27\3\27\3\27\3\30\3\30\3\30\3\30\7\30\u019f\n\30\f\30"
          + "\16\30\u01a2\13\30\3\30\3\30\3\30\3\30\3\30\5\30\u01a9\n\30\3\31\3\31"
          + "\5\31\u01ad\n\31\3\32\3\32\3\32\3\32\3\32\3\32\3\32\3\32\3\32\3\32\3\32"
          + "\3\32\3\32\3\32\3\32\3\32\3\32\3\32\3\32\3\32\3\32\3\32\3\32\5\32\u01c6"
          + "\n\32\3\33\3\33\3\33\3\33\5\33\u01cc\n\33\3\33\3\33\3\33\3\33\3\33\5\33"
          + "\u01d3\n\33\7\33\u01d5\n\33\f\33\16\33\u01d8\13\33\3\34\3\34\3\34\3\34"
          + "\5\34\u01de\n\34\3\35\3\35\3\35\3\35\3\35\3\35\5\35\u01e6\n\35\3\35\3"
          + "\35\3\35\5\35\u01eb\n\35\3\35\3\35\3\35\3\35\7\35\u01f1\n\35\f\35\16\35"
          + "\u01f4\13\35\3\36\3\36\3\36\5\36\u01f9\n\36\3\36\3\36\3\36\3\36\3\36\3"
          + "\36\3\36\3\36\5\36\u0203\n\36\3\36\3\36\3\36\3\36\3\36\3\36\3\36\3\36"
          + "\3\36\3\36\3\36\5\36\u0210\n\36\3\36\3\36\3\36\3\36\5\36\u0216\n\36\3"
          + "\36\3\36\3\36\3\36\3\36\3\36\3\36\3\36\3\36\3\36\5\36\u0222\n\36\3\36"
          + "\3\36\3\36\3\36\5\36\u0228\n\36\7\36\u022a\n\36\f\36\16\36\u022d\13\36"
          + "\3\37\3\37\3\37\3\37\3\37\3\37\3\37\3\37\3\37\3\37\3\37\3\37\3\37\7\37"
          + "\u023c\n\37\f\37\16\37\u023f\13\37\3\37\3\37\3\37\3\37\3\37\3\37\3\37"
          + "\6\37\u0248\n\37\r\37\16\37\u0249\3\37\3\37\3\37\3\37\3\37\3\37\5\37\u0252"
          + "\n\37\3\37\3\37\3\37\3\37\3\37\3\37\3\37\3\37\3\37\3\37\3\37\7\37\u025f"
          + "\n\37\f\37\16\37\u0262\13\37\3 \3 \3!\3!\3!\3!\3!\3!\3!\3!\3!\3!\3!\3"
          + "!\3!\3!\5!\u0274\n!\3\"\3\"\3\"\3\"\3\"\3\"\3\"\5\"\u027d\n\"\3#\3#\3"
          + "#\3#\3#\3#\3#\5#\u0286\n#\3$\3$\3%\3%\3&\3&\3\'\3\'\3(\3(\3)\3)\3)\2\5"
          + "8:<*\2\4\6\b\n\f\16\20\22\24\26\30\32\34\36 \"$&(*,.\60\62\64\668:<>@"
          + "BDFHJLNP\2\30\4\2\u03bc\u03be\u03c6\u03c6\4\288\u009b\u009b\4\2gg\u03c9"
          + "\u03c9\4\2\u00bf\u00bf\u019d\u019d\4\2\u00b8\u00b9\u00bb\u00bb\4\2\u00a0"
          + "\u00a0\u01e7\u01e7\4\2\u00e7\u00e8\u00ee\u00ee\4\2\"\"\u00e5\u00e8\3\2"
          + "\u00f1\u00f2\5\2\21\21TT\u0099\u0099\4\2\u00bd\u00bd\u00bf\u00bf\5\2\16"
          + "\16**\u032e\u032e\4\2ee\u03b1\u03b1\5\288\u009b\u009b\u0214\u0214\4\2"
          + "yy\u0083\u0083\6\2ee\u03a9\u03a9\u03ab\u03ab\u03b1\u03b2\3\2\u03a6\u03ad"
          + "\3\2\u023d\u0264\3\2\u022c\u0233\5\2\u00b8\u00bc\u00c6\u00c6\u00c9\u00c9"
          + " \2\u00f8\u010a\u010c\u0112\u0114\u012b\u012d\u0136\u0138\u0144\u0146"
          + "\u014e\u0150\u0150\u0152\u0153\u0155\u0157\u0159\u0159\u015b\u015b\u015d"
          + "\u0163\u0165\u016b\u016d\u0196\u0198\u019e\u01a0\u01a9\u01ab\u01b5\u01b7"
          + "\u01c7\u01c9\u01d6\u01d8\u01e9\u01eb\u01ed\u01ef\u01ff\u0201\u020b\u020d"
          + "\u020d\u020f\u0212\u0214\u0214\u0216\u0217\u021a\u021d\u021f\u0226\u026a"
          + "\u026a\13\2$$VV\u0082\u0082\u00b8\u00ba\u00bc\u00bc\u00d9\u00d9\u00f0"
          + "\u00f0\u022c\u0233\u0274\u039c\2\u0315\2_\3\2\2\2\4a\3\2\2\2\6g\3\2\2"
          + "\2\br\3\2\2\2\nv\3\2\2\2\f{\3\2\2\2\16\u0083\3\2\2\2\20\u0088\3\2\2\2"
          + "\22\u008a\3\2\2\2\24\u008d\3\2\2\2\26\u0091\3\2\2\2\30\u0094\3\2\2\2\32"
          + "\u0099\3\2\2\2\34\u00a9\3\2\2\2\36\u00c1\3\2\2\2 \u00c3\3\2\2\2\"\u00c7"
          + "\3\2\2\2$\u00cd\3\2\2\2&\u00e3\3\2\2\2(\u00f4\3\2\2\2*\u0193\3\2\2\2,"
          + "\u0195\3\2\2\2.\u01a8\3\2\2\2\60\u01aa\3\2\2\2\62\u01c5\3\2\2\2\64\u01cb"
          + "\3\2\2\2\66\u01dd\3\2\2\28\u01ea\3\2\2\2:\u01f5\3\2\2\2<\u0251\3\2\2\2"
          + ">\u0263\3\2\2\2@\u0273\3\2\2\2B\u027c\3\2\2\2D\u0285\3\2\2\2F\u0287\3"
          + "\2\2\2H\u0289\3\2\2\2J\u028b\3\2\2\2L\u028d\3\2\2\2N\u028f\3\2\2\2P\u0291"
          + "\3\2\2\2R`\5J&\2S`\7\u00bc\2\2T`\7\u00ca\2\2U`\7\u00cb\2\2V`\7\u00cc\2"
          + "\2W`\7\u00cd\2\2X`\7\u00ce\2\2Y`\7\u00cf\2\2Z`\7\u00d0\2\2[`\7\u00d1\2"
          + "\2\\`\7\u00d2\2\2]`\7\u00d3\2\2^`\7\u00d4\2\2_R\3\2\2\2_S\3\2\2\2_T\3"
          + "\2\2\2_U\3\2\2\2_V\3\2\2\2_W\3\2\2\2_X\3\2\2\2_Y\3\2\2\2_Z\3\2\2\2_[\3"
          + "\2\2\2_\\\3\2\2\2_]\3\2\2\2_^\3\2\2\2`\3\3\2\2\2ae\5\f\7\2bf\7\u03cc\2"
          + "\2cd\7\u03b6\2\2df\5\f\7\2eb\3\2\2\2ec\3\2\2\2ef\3\2\2\2f\5\3\2\2\2gl"
          + "\5\f\7\2hj\5\20\t\2ik\5\20\t\2ji\3\2\2\2jk\3\2\2\2km\3\2\2\2lh\3\2\2\2"
          + "lm\3\2\2\2m\7\3\2\2\2ns\7\u00bf\2\2os\5H%\2ps\7\u03c5\2\2qs\7\u03c3\2"
          + "\2rn\3\2\2\2ro\3\2\2\2rp\3\2\2\2rq\3\2\2\2s\t\3\2\2\2tw\5\f\7\2uw\7\u03c5"
          + "\2\2vt\3\2\2\2vu\3\2\2\2w\13\3\2\2\2x|\5\16\b\2y|\7\u03ce\2\2z|\7\u03c3"
          + "\2\2{x\3\2\2\2{y\3\2\2\2{z\3\2\2\2|\r\3\2\2\2}\u0084\7\u03cd\2\2~\u0084"
          + "\5H%\2\177\u0084\5J&\2\u0080\u0084\5L\'\2\u0081\u0084\5N(\2\u0082\u0084"
          + "\5P)\2\u0083}\3\2\2\2\u0083~\3\2\2\2\u0083\177\3\2\2\2\u0083\u0080\3\2"
          + "\2\2\u0083\u0081\3\2\2\2\u0083\u0082\3\2\2\2\u0084\17\3\2\2\2\u0085\u0089"
          + "\7\u03cc\2\2\u0086\u0087\7\u03b6\2\2\u0087\u0089\5\f\7\2\u0088\u0085\3"
          + "\2\2\2\u0088\u0086\3\2\2\2\u0089\21\3\2\2\2\u008a\u008b\t\2\2\2\u008b"
          + "\23\3\2\2\2\u008c\u008e\7\u03c5\2\2\u008d\u008c\3\2\2\2\u008e\u008f\3"
          + "\2\2\2\u008f\u008d\3\2\2\2\u008f\u0090\3\2\2\2\u0090\25\3\2\2\2\u0091"
          + "\u0092\t\3\2\2\u0092\27\3\2\2\2\u0093\u0095\7\u03cb\2\2\u0094\u0093\3"
          + "\2\2\2\u0094\u0095\3\2\2\2\u0095\u0096\3\2\2\2\u0096\u0097\7\u03c7\2\2"
          + "\u0097\31\3\2\2\2\u0098\u009a\7e\2\2\u0099\u0098\3\2\2\2\u0099\u009a\3"
          + "\2\2\2\u009a\u009b\3\2\2\2\u009b\u009c\t\4\2\2\u009c\33\3\2\2\2\u009d"
          + "\u00aa\5\24\13\2\u009e\u00aa\5\22\n\2\u009f\u00a0\7\u03ab\2\2\u00a0\u00aa"
          + "\5\22\n\2\u00a1\u00aa\5\30\r\2\u00a2\u00aa\5\26\f\2\u00a3\u00aa\7\u03c8"
          + "\2\2\u00a4\u00aa\7\u03ca\2\2\u00a5\u00a7\7e\2\2\u00a6\u00a5\3\2\2\2\u00a6"
          + "\u00a7\3\2\2\2\u00a7\u00a8\3\2\2\2\u00a8\u00aa\t\4\2\2\u00a9\u009d\3\2"
          + "\2\2\u00a9\u009e\3\2\2\2\u00a9\u009f\3\2\2\2\u00a9\u00a1\3\2\2\2\u00a9"
          + "\u00a2\3\2\2\2\u00a9\u00a3\3\2\2\2\u00a9\u00a4\3\2\2\2\u00a9\u00a6\3\2"
          + "\2\2\u00aa\35\3\2\2\2\u00ab\u00ad\t\5\2\2\u00ac\u00ae\5 \21\2\u00ad\u00ac"
          + "\3\2\2\2\u00ad\u00ae\3\2\2\2\u00ae\u00c2\3\2\2\2\u00af\u00b1\7\u00bd\2"
          + "\2\u00b0\u00b2\5 \21\2\u00b1\u00b0\3\2\2\2\u00b1\u00b2\3\2\2\2\u00b2\u00b6"
          + "\3\2\2\2\u00b3\u00b4\7\30\2\2\u00b4\u00b5\7\u0087\2\2\u00b5\u00b7\5\b"
          + "\5\2\u00b6\u00b3\3\2\2\2\u00b6\u00b7\3\2\2\2\u00b7\u00c2\3\2\2\2\u00b8"
          + "\u00c2\t\6\2\2\u00b9\u00bb\7\u00b6\2\2\u00ba\u00bc\5\"\22\2\u00bb\u00ba"
          + "\3\2\2\2\u00bb\u00bc\3\2\2\2\u00bc\u00c2\3\2\2\2\u00bd\u00bf\t\7\2\2\u00be"
          + "\u00c0\7\u00b1\2\2\u00bf\u00be\3\2\2\2\u00bf\u00c0\3\2\2\2\u00c0\u00c2"
          + "\3\2\2\2\u00c1\u00ab\3\2\2\2\u00c1\u00af\3\2\2\2\u00c1\u00b8\3\2\2\2\u00c1"
          + "\u00b9\3\2\2\2\u00c1\u00bd\3\2\2\2\u00c2\37\3\2\2\2\u00c3\u00c4\7\u03b7"
          + "\2\2\u00c4\u00c5\5\22\n\2\u00c5\u00c6\7\u03b8\2\2\u00c6!\3\2\2\2\u00c7"
          + "\u00c8\7\u03b7\2\2\u00c8\u00c9\5\22\n\2\u00c9\u00ca\7\u03b9\2\2\u00ca"
          + "\u00cb\5\22\n\2\u00cb\u00cc\7\u03b8\2\2\u00cc#\3\2\2\2\u00cd\u00d2\58"
          + "\35\2\u00ce\u00cf\7\u03b9\2\2\u00cf\u00d1\58\35\2\u00d0\u00ce\3\2\2\2"
          + "\u00d1\u00d4\3\2\2\2\u00d2\u00d0\3\2\2\2\u00d2\u00d3\3\2\2\2\u00d3%\3"
          + "\2\2\2\u00d4\u00d2\3\2\2\2\u00d5\u00db\t\b\2\2\u00d6\u00d8\7\u03b7\2\2"
          + "\u00d7\u00d9\5\22\n\2\u00d8\u00d7\3\2\2\2\u00d8\u00d9\3\2\2\2\u00d9\u00da"
          + "\3\2\2\2\u00da\u00dc\7\u03b8\2\2\u00db\u00d6\3\2\2\2\u00db\u00dc\3\2\2"
          + "\2\u00dc\u00e4\3\2\2\2\u00dd\u00de\7\u00ef\2\2\u00de\u00e0\7\u03b7\2\2"
          + "\u00df\u00e1\5\22\n\2\u00e0\u00df\3\2\2\2\u00e0\u00e1\3\2\2\2\u00e1\u00e2"
          + "\3\2\2\2\u00e2\u00e4\7\u03b8\2\2\u00e3\u00d5\3\2\2\2\u00e3\u00dd\3\2\2"
          + "\2\u00e4\'\3\2\2\2\u00e5\u00f5\5*\26\2\u00e6\u00e7\5\62\32\2\u00e7\u00e9"
          + "\7\u03b7\2\2\u00e8\u00ea\5\64\33\2\u00e9\u00e8\3\2\2\2\u00e9\u00ea\3\2"
          + "\2\2\u00ea\u00eb\3\2\2\2\u00eb\u00ec\7\u03b8\2\2\u00ec\u00f5\3\2\2\2\u00ed"
          + "\u00ee\5\4\3\2\u00ee\u00f0\7\u03b7\2\2\u00ef\u00f1\5\64\33\2\u00f0\u00ef"
          + "\3\2\2\2\u00f0\u00f1\3\2\2\2\u00f1\u00f2\3\2\2\2\u00f2\u00f3\7\u03b8\2"
          + "\2\u00f3\u00f5\3\2\2\2\u00f4\u00e5\3\2\2\2\u00f4\u00e6\3\2\2\2\u00f4\u00ed"
          + "\3\2\2\2\u00f5)\3\2\2\2\u00f6\u0194\t\t\2\2\u00f7\u00f8\7\37\2\2\u00f8"
          + "\u00f9\7\u03b7\2\2\u00f9\u00fa\58\35\2\u00fa\u00fb\7\u03b9\2\2\u00fb\u00fc"
          + "\5\36\20\2\u00fc\u00fd\7\u03b8\2\2\u00fd\u0194\3\2\2\2\u00fe\u00ff\7\37"
          + "\2\2\u00ff\u0100\7\u03b7\2\2\u0100\u0101\58\35\2\u0101\u0102\7\u00a4\2"
          + "\2\u0102\u0103\5\b\5\2\u0103\u0104\7\u03b8\2\2\u0104\u0194\3\2\2\2\u0105"
          + "\u0106\7\26\2\2\u0106\u0107\7\u03b7\2\2\u0107\u0108\58\35\2\u0108\u0109"
          + "\7\r\2\2\u0109\u010a\5\36\20\2\u010a\u010b\7\u03b8\2\2\u010b\u0194\3\2"
          + "\2\2\u010c\u010d\7\u00a5\2\2\u010d\u010e\7\u03b7\2\2\u010e\u010f\5\6\4"
          + "\2\u010f\u0110\7\u03b8\2\2\u0110\u0194\3\2\2\2\u0111\u0112\7\25\2\2\u0112"
          + "\u0114\58\35\2\u0113\u0115\5,\27\2\u0114\u0113\3\2\2\2\u0115\u0116\3\2"
          + "\2\2\u0116\u0114\3\2\2\2\u0116\u0117\3\2\2\2\u0117\u011a\3\2\2\2\u0118"
          + "\u0119\7\61\2\2\u0119\u011b\5\66\34\2\u011a\u0118\3\2\2\2\u011a\u011b"
          + "\3\2\2\2\u011b\u011c\3\2\2\2\u011c\u011d\7\u0137\2\2\u011d\u0194\3\2\2"
          + "\2\u011e\u0120\7\25\2\2\u011f\u0121\5,\27\2\u0120\u011f\3\2\2\2\u0121"
          + "\u0122\3\2\2\2\u0122\u0120\3\2\2\2\u0122\u0123\3\2\2\2\u0123\u0126\3\2"
          + "\2\2\u0124\u0125\7\61\2\2\u0125\u0127\5\66\34\2\u0126\u0124\3\2\2\2\u0126"
          + "\u0127\3\2\2\2\u0127\u0128\3\2\2\2\u0128\u0129\7\u0137\2\2\u0129\u0194"
          + "\3\2\2\2\u012a\u012b\7\u00bd\2\2\u012b\u012c\7\u03b7\2\2\u012c\u012f\5"
          + "\64\33\2\u012d\u012e\7\u00a4\2\2\u012e\u0130\5\b\5\2\u012f\u012d\3\2\2"
          + "\2\u012f\u0130\3\2\2\2\u0130\u0131\3\2\2\2\u0131\u0132\7\u03b8\2\2\u0132"
          + "\u0194\3\2\2\2\u0133\u0134\7\u00f0\2\2\u0134\u0137\7\u03b7\2\2\u0135\u0138"
          + "\5\24\13\2\u0136\u0138\58\35\2\u0137\u0135\3\2\2\2\u0137\u0136\3\2\2\2"
          + "\u0138\u0139\3\2\2\2\u0139\u013c\7F\2\2\u013a\u013d\5\24\13\2\u013b\u013d"
          + "\58\35\2\u013c\u013a\3\2\2\2\u013c\u013b\3\2\2\2\u013d\u013e\3\2\2\2\u013e"
          + "\u013f\7\u03b8\2\2\u013f\u0194\3\2\2\2\u0140\u0141\t\n\2\2\u0141\u0144"
          + "\7\u03b7\2\2\u0142\u0145\5\24\13\2\u0143\u0145\58\35\2\u0144\u0142\3\2"
          + "\2\2\u0144\u0143\3\2\2\2\u0145\u0146\3\2\2\2\u0146\u0149\7=\2\2\u0147"
          + "\u014a\5\22\n\2\u0148\u014a\58\35\2\u0149\u0147\3\2\2\2\u0149\u0148\3"
          + "\2\2\2\u014a\u0150\3\2\2\2\u014b\u014e\7:\2\2\u014c\u014f\5\22\n\2\u014d"
          + "\u014f\58\35\2\u014e\u014c\3\2\2\2\u014e\u014d\3\2\2\2\u014f\u0151\3\2"
          + "\2\2\u0150\u014b\3\2\2\2\u0150\u0151\3\2\2\2\u0151\u0152\3\2\2\2\u0152"
          + "\u0153\7\u03b8\2\2\u0153\u0194\3\2\2\2\u0154\u0155\7\u00f4\2\2\u0155\u0156"
          + "\7\u03b7\2\2\u0156\u0159\t\13\2\2\u0157\u015a\5\24\13\2\u0158\u015a\5"
          + "8\35\2\u0159\u0157\3\2\2\2\u0159\u0158\3\2\2\2\u0159\u015a\3\2\2\2\u015a"
          + "\u015b\3\2\2\2\u015b\u015e\7=\2\2\u015c\u015f\5\24\13\2\u015d\u015f\5"
          + "8\35\2\u015e\u015c\3\2\2\2\u015e\u015d\3\2\2\2\u015f\u0160\3\2\2\2\u0160"
          + "\u0161\7\u03b8\2\2\u0161\u0194\3\2\2\2\u0162\u0163\7\u00f4\2\2\u0163\u0166"
          + "\7\u03b7\2\2\u0164\u0167\5\24\13\2\u0165\u0167\58\35\2\u0166\u0164\3\2"
          + "\2\2\u0166\u0165\3\2\2\2\u0167\u0168\3\2\2\2\u0168\u016b\7=\2\2\u0169"
          + "\u016c\5\24\13\2\u016a\u016c\58\35\2\u016b\u0169\3\2\2\2\u016b\u016a\3"
          + "\2\2\2\u016c\u016d\3\2\2\2\u016d\u016e\7\u03b8\2\2\u016e\u0194\3\2\2\2"
          + "\u016f\u0170\7\u0398\2\2\u0170\u0173\7\u03b7\2\2\u0171\u0174\5\24\13\2"
          + "\u0172\u0174\58\35\2\u0173\u0171\3\2\2\2\u0173\u0172\3\2\2\2\u0174\u017b"
          + "\3\2\2\2\u0175\u0176\7\r\2\2\u0176\u0177\t\f\2\2\u0177\u0178\7\u03b7\2"
          + "\2\u0178\u0179\5\22\n\2\u0179\u017a\7\u03b8\2\2\u017a\u017c\3\2\2\2\u017b"
          + "\u0175\3\2\2\2\u017b\u017c\3\2\2\2\u017c\u017e\3\2\2\2\u017d\u017f\5."
          + "\30\2\u017e\u017d\3\2\2\2\u017e\u017f\3\2\2\2\u017f\u0180\3\2\2\2\u0180"
          + "\u0181\7\u03b8\2\2\u0181\u0194\3\2\2\2\u0182\u0183\7\u00ed\2\2\u0183\u0184"
          + "\7\u03b7\2\2\u0184\u0185\5\2\2\2\u0185\u0188\7=\2\2\u0186\u0189\5\24\13"
          + "\2\u0187\u0189\58\35\2\u0188\u0186\3\2\2\2\u0188\u0187\3\2\2\2\u0189\u018a"
          + "\3\2\2\2\u018a\u018b\7\u03b8\2\2\u018b\u0194\3\2\2\2\u018c\u018d\7\u02d1"
          + "\2\2\u018d\u018e\7\u03b7\2\2\u018e\u018f\t\6\2\2\u018f\u0190\7\u03b9\2"
          + "\2\u0190\u0191\5\24\13\2\u0191\u0192\7\u03b8\2\2\u0192\u0194\3\2\2\2\u0193"
          + "\u00f6\3\2\2\2\u0193\u00f7\3\2\2\2\u0193\u00fe\3\2\2\2\u0193\u0105\3\2"
          + "\2\2\u0193\u010c\3\2\2\2\u0193\u0111\3\2\2\2\u0193\u011e\3\2\2\2\u0193"
          + "\u012a\3\2\2\2\u0193\u0133\3\2\2\2\u0193\u0140\3\2\2\2\u0193\u0154\3\2"
          + "\2\2\u0193\u0162\3\2\2\2\u0193\u016f\3\2\2\2\u0193\u0182\3\2\2\2\u0193"
          + "\u018c\3\2\2\2\u0194+\3\2\2\2\u0195\u0196\7\u00a6\2\2\u0196\u0197\5\66"
          + "\34\2\u0197\u0198\7\u0097\2\2\u0198\u0199\5\66\34\2\u0199-\3\2\2\2\u019a"
          + "\u019b\7\u0172\2\2\u019b\u01a0\5\60\31\2\u019c\u019d\7\u03b9\2\2\u019d"
          + "\u019f\5\60\31\2\u019e\u019c\3\2\2\2\u019f\u01a2\3\2\2\2\u01a0\u019e\3"
          + "\2\2\2\u01a0\u01a1\3\2\2\2\u01a1\u01a9\3\2\2\2\u01a2\u01a0\3\2\2\2\u01a3"
          + "\u01a4\7\u0172\2\2\u01a4\u01a5\5\22\n\2\u01a5\u01a6\7\u03ab\2\2\u01a6"
          + "\u01a7\5\22\n\2\u01a7\u01a9\3\2\2\2\u01a8\u019a\3\2\2\2\u01a8\u01a3\3"
          + "\2\2\2\u01a9/\3\2\2\2\u01aa\u01ac\5\22\n\2\u01ab\u01ad\t\r\2\2\u01ac\u01ab"
          + "\3\2\2\2\u01ac\u01ad\3\2\2\2\u01ad\61\3\2\2\2\u01ae\u01c6\5P)\2\u01af"
          + "\u01c6\7\u023e\2\2\u01b0\u01c6\7\u00e9\2\2\u01b1\u01c6\7\u00e5\2\2\u01b2"
          + "\u01c6\7\u00e6\2\2\u01b3\u01c6\7\u00e7\2\2\u01b4\u01c6\7\u00ea\2\2\u01b5"
          + "\u01c6\7\u00eb\2\2\u01b6\u01c6\7\u00ec\2\2\u01b7\u01c6\7D\2\2\u01b8\u01c6"
          + "\7K\2\2\u01b9\u01c6\7\u00e8\2\2\u01ba\u01c6\7\u00ee\2\2\u01bb\u01c6\7"
          + "\u0194\2\2\u01bc\u01c6\7\u00ef\2\2\u01bd\u01c6\7}\2\2\u01be\u01c6\7\u00f1"
          + "\2\2\u01bf\u01c6\7\u00f2\2\2\u01c0\u01c6\7\u00f3\2\2\u01c1\u01c6\7\u00f4"
          + "\2\2\u01c2\u01c6\7\u00f5\2\2\u01c3\u01c6\7\u00f6\2\2\u01c4\u01c6\7\u00f7"
          + "\2\2\u01c5\u01ae\3\2\2\2\u01c5\u01af\3\2\2\2\u01c5\u01b0\3\2\2\2\u01c5"
          + "\u01b1\3\2\2\2\u01c5\u01b2\3\2\2\2\u01c5\u01b3\3\2\2\2\u01c5\u01b4\3\2"
          + "\2\2\u01c5\u01b5\3\2\2\2\u01c5\u01b6\3\2\2\2\u01c5\u01b7\3\2\2\2\u01c5"
          + "\u01b8\3\2\2\2\u01c5\u01b9\3\2\2\2\u01c5\u01ba\3\2\2\2\u01c5\u01bb\3\2"
          + "\2\2\u01c5\u01bc\3\2\2\2\u01c5\u01bd\3\2\2\2\u01c5\u01be\3\2\2\2\u01c5"
          + "\u01bf\3\2\2\2\u01c5\u01c0\3\2\2\2\u01c5\u01c1\3\2\2\2\u01c5\u01c2\3\2"
          + "\2\2\u01c5\u01c3\3\2\2\2\u01c5\u01c4\3\2\2\2\u01c6\63\3\2\2\2\u01c7\u01cc"
          + "\5\34\17\2\u01c8\u01cc\5\6\4\2\u01c9\u01cc\5(\25\2\u01ca\u01cc\58\35\2"
          + "\u01cb\u01c7\3\2\2\2\u01cb\u01c8\3\2\2\2\u01cb\u01c9\3\2\2\2\u01cb\u01ca"
          + "\3\2\2\2\u01cc\u01d6\3\2\2\2\u01cd\u01d2\7\u03b9\2\2\u01ce\u01d3\5\34"
          + "\17\2\u01cf\u01d3\5\6\4\2\u01d0\u01d3\5(\25\2\u01d1\u01d3\58\35\2\u01d2"
          + "\u01ce\3\2\2\2\u01d2\u01cf\3\2\2\2\u01d2\u01d0\3\2\2\2\u01d2\u01d1\3\2"
          + "\2\2\u01d3\u01d5\3\2\2\2\u01d4\u01cd\3\2\2\2\u01d5\u01d8\3\2\2\2\u01d6"
          + "\u01d4\3\2\2\2\u01d6\u01d7\3\2\2\2\u01d7\65\3\2\2\2\u01d8\u01d6\3\2\2"
          + "\2\u01d9\u01de\5\34\17\2\u01da\u01de\5\6\4\2\u01db\u01de\5(\25\2\u01dc"
          + "\u01de\58\35\2\u01dd\u01d9\3\2\2\2\u01dd\u01da\3\2\2\2\u01dd\u01db\3\2"
          + "\2\2\u01dd\u01dc\3\2\2\2\u01de\67\3\2\2\2\u01df\u01e0\b\35\1\2\u01e0\u01e1"
          + "\t\16\2\2\u01e1\u01eb\58\35\6\u01e2\u01e3\5:\36\2\u01e3\u01e5\7N\2\2\u01e4"
          + "\u01e6\7e\2\2\u01e5\u01e4\3\2\2\2\u01e5\u01e6\3\2\2\2\u01e6\u01e7\3\2"
          + "\2\2\u01e7\u01e8\t\17\2\2\u01e8\u01eb\3\2\2\2\u01e9\u01eb\5:\36\2\u01ea"
          + "\u01df\3\2\2\2\u01ea\u01e2\3\2\2\2\u01ea\u01e9\3\2\2\2\u01eb\u01f2\3\2"
          + "\2\2\u01ec\u01ed\f\5\2\2\u01ed\u01ee\5B\"\2\u01ee\u01ef\58\35\6\u01ef"
          + "\u01f1\3\2\2\2\u01f0\u01ec\3\2\2\2\u01f1\u01f4\3\2\2\2\u01f2\u01f0\3\2"
          + "\2\2\u01f2\u01f3\3\2\2\2\u01f39\3\2\2\2\u01f4\u01f2\3\2\2\2\u01f5\u01f8"
          + "\b\36\1\2\u01f6\u01f7\7\u03d0\2\2\u01f7\u01f9\7\u039d\2\2\u01f8\u01f6"
          + "\3\2\2\2\u01f8\u01f9\3\2\2\2\u01f9\u01fa\3\2\2\2\u01fa\u01fb\5<\37\2\u01fb"
          + "\u022b\3\2\2\2\u01fc\u01fd\f\b\2\2\u01fd\u01fe\5@!\2\u01fe\u01ff\5:\36"
          + "\t\u01ff\u022a\3\2\2\2\u0200\u0202\f\7\2\2\u0201\u0203\7e\2\2\u0202\u0201"
          + "\3\2\2\2\u0202\u0203\3\2\2\2\u0203\u0204\3\2\2\2\u0204\u0205\7\20\2\2"
          + "\u0205\u0206\5:\36\2\u0206\u0207\7\f\2\2\u0207\u0208\5:\36\b\u0208\u022a"
          + "\3\2\2\2\u0209\u020a\f\6\2\2\u020a\u020b\7\u01ef\2\2\u020b\u020c\7W\2"
          + "\2\u020c\u022a\5:\36\7\u020d\u020f\f\4\2\2\u020e\u0210\7e\2\2\u020f\u020e"
          + "\3\2\2\2\u020f\u0210\3\2\2\2\u0210\u0211\3\2\2\2\u0211\u0212\t\20\2\2"
          + "\u0212\u022a\5:\36\5\u0213\u0215\f\n\2\2\u0214\u0216\7e\2\2\u0215\u0214"
          + "\3\2\2\2\u0215\u0216\3\2\2\2\u0216\u0217\3\2\2\2\u0217\u0218\7F\2\2\u0218"
          + "\u0219\7\u03b7\2\2\u0219\u021a\5$\23\2\u021a\u021b\7\u03b8\2\2\u021b\u022a"
          + "\3\2\2\2\u021c\u021d\f\t\2\2\u021d\u021e\7N\2\2\u021e\u022a\5\32\16\2"
          + "\u021f\u0221\f\5\2\2\u0220\u0222\7e\2\2\u0221\u0220\3\2\2\2\u0221\u0222"
          + "\3\2\2\2\u0222\u0223\3\2\2\2\u0223\u0224\7W\2\2\u0224\u0227\5:\36\2\u0225"
          + "\u0226\7\u013d\2\2\u0226\u0228\7\u03c5\2\2\u0227\u0225\3\2\2\2\u0227\u0228"
          + "\3\2\2\2\u0228\u022a\3\2\2\2\u0229\u01fc\3\2\2\2\u0229\u0200\3\2\2\2\u0229"
          + "\u0209\3\2\2\2\u0229\u020d\3\2\2\2\u0229\u0213\3\2\2\2\u0229\u021c\3\2"
          + "\2\2\u0229\u021f\3\2\2\2\u022a\u022d\3\2\2\2\u022b\u0229\3\2\2\2\u022b"
          + "\u022c\3\2\2\2\u022c;\3\2\2\2\u022d\u022b\3\2\2\2\u022e\u022f\b\37\1\2"
          + "\u022f\u0252\5\34\17\2\u0230\u0252\5\6\4\2\u0231\u0252\5(\25\2\u0232\u0233"
          + "\5> \2\u0233\u0234\5<\37\t\u0234\u0252\3\2\2\2\u0235\u0236\7\u00bf\2\2"
          + "\u0236\u0252\5<\37\b\u0237\u0238\7\u03b7\2\2\u0238\u023d\58\35\2\u0239"
          + "\u023a\7\u03b9\2\2\u023a\u023c\58\35\2\u023b\u0239\3\2\2\2\u023c\u023f"
          + "\3\2\2\2\u023d\u023b\3\2\2\2\u023d\u023e\3\2\2\2\u023e\u0240\3\2\2\2\u023f"
          + "\u023d\3\2\2\2\u0240\u0241\7\u03b8\2\2\u0241\u0252\3\2\2\2\u0242\u0243"
          + "\7\u01dd\2\2\u0243\u0244\7\u03b7\2\2\u0244\u0247\58\35\2\u0245\u0246\7"
          + "\u03b9\2\2\u0246\u0248\58\35\2\u0247\u0245\3\2\2\2\u0248\u0249\3\2\2\2"
          + "\u0249\u0247\3\2\2\2\u0249\u024a\3\2\2\2\u024a\u024b\3\2\2\2\u024b\u024c"
          + "\7\u03b8\2\2\u024c\u0252\3\2\2\2\u024d\u024e\7L\2\2\u024e\u024f\58\35"
          + "\2\u024f\u0250\5\2\2\2\u0250\u0252\3\2\2\2\u0251\u022e\3\2\2\2\u0251\u0230"
          + "\3\2\2\2\u0251\u0231\3\2\2\2\u0251\u0232\3\2\2\2\u0251\u0235\3\2\2\2\u0251"
          + "\u0237\3\2\2\2\u0251\u0242\3\2\2\2\u0251\u024d\3\2\2\2\u0252\u0260\3\2"
          + "\2\2\u0253\u0254\f\4\2\2\u0254\u0255\5D#\2\u0255\u0256\5<\37\5\u0256\u025f"
          + "\3\2\2\2\u0257\u0258\f\3\2\2\u0258\u0259\5F$\2\u0259\u025a\5<\37\4\u025a"
          + "\u025f\3\2\2\2\u025b\u025c\f\n\2\2\u025c\u025d\7\32\2\2\u025d\u025f\5"
          + "\n\6\2\u025e\u0253\3\2\2\2\u025e\u0257\3\2\2\2\u025e\u025b\3\2\2\2\u025f"
          + "\u0262\3\2\2\2\u0260\u025e\3\2\2\2\u0260\u0261\3\2\2\2\u0261=\3\2\2\2"
          + "\u0262\u0260\3\2\2\2\u0263\u0264\t\21\2\2\u0264?\3\2\2\2\u0265\u0274\7"
          + "\u03ae\2\2\u0266\u0274\7\u03af\2\2\u0267\u0274\7\u03b0\2\2\u0268\u0269"
          + "\7\u03b0\2\2\u0269\u0274\7\u03ae\2\2\u026a\u026b\7\u03af\2\2\u026b\u0274"
          + "\7\u03ae\2\2\u026c\u026d\7\u03b0\2\2\u026d\u0274\7\u03af\2\2\u026e\u026f"
          + "\7\u03b1\2\2\u026f\u0274\7\u03ae\2\2\u0270\u0271\7\u03b0\2\2\u0271\u0272"
          + "\7\u03ae\2\2\u0272\u0274\7\u03af\2\2\u0273\u0265\3\2\2\2\u0273\u0266\3"
          + "\2\2\2\u0273\u0267\3\2\2\2\u0273\u0268\3\2\2\2\u0273\u026a\3\2\2\2\u0273"
          + "\u026c\3\2\2\2\u0273\u026e\3\2\2\2\u0273\u0270\3\2\2\2\u0274A\3\2\2\2"
          + "\u0275\u027d\7\f\2\2\u0276\u0277\7\u03b4\2\2\u0277\u027d\7\u03b4\2\2\u0278"
          + "\u027d\7\u00ab\2\2\u0279\u027d\7l\2\2\u027a\u027b\7\u03b3\2\2\u027b\u027d"
          + "\7\u03b3\2\2\u027c\u0275\3\2\2\2\u027c\u0276\3\2\2\2\u027c\u0278\3\2\2"
          + "\2\u027c\u0279\3\2\2\2\u027c\u027a\3\2\2\2\u027dC\3\2\2\2\u027e\u027f"
          + "\7\u03b0\2\2\u027f\u0286\7\u03b0\2\2\u0280\u0281\7\u03af\2\2\u0281\u0286"
          + "\7\u03af\2\2\u0282\u0286\7\u03b4\2\2\u0283\u0286\7\u03b5\2\2\u0284\u0286"
          + "\7\u03b3\2\2\u0285\u027e\3\2\2\2\u0285\u0280\3\2\2\2\u0285\u0282\3\2\2"
          + "\2\u0285\u0283\3\2\2\2\u0285\u0284\3\2\2\2\u0286E\3\2\2\2\u0287\u0288"
          + "\t\22\2\2\u0288G\3\2\2\2\u0289\u028a\t\23\2\2\u028aI\3\2\2\2\u028b\u028c"
          + "\t\24\2\2\u028cK\3\2\2\2\u028d\u028e\t\25\2\2\u028eM\3\2\2\2\u028f\u0290"
          + "\t\26\2\2\u0290O\3\2\2\2\u0291\u0292\t\27\2\2\u0292Q\3\2\2\2M_ejlrv{\u0083"
          + "\u0088\u008f\u0094\u0099\u00a6\u00a9\u00ad\u00b1\u00b6\u00bb\u00bf\u00c1"
          + "\u00d2\u00d8\u00db\u00e0\u00e3\u00e9\u00f0\u00f4\u0116\u011a\u0122\u0126"
          + "\u012f\u0137\u013c\u0144\u0149\u014e\u0150\u0159\u015e\u0166\u016b\u0173"
          + "\u017b\u017e\u0188\u0193\u01a0\u01a8\u01ac\u01c5\u01cb\u01d2\u01d6\u01dd"
          + "\u01e5\u01ea\u01f2\u01f8\u0202\u020f\u0215\u0221\u0227\u0229\u022b\u023d"
          + "\u0249\u0251\u025e\u0260\u0273\u027c\u0285";
  public static final ATN _ATN = new ATNDeserializer().deserialize(_serializedATN.toCharArray());

  static {
    _decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
    for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
      _decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
    }
  }
}
