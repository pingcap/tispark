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
      START_NATIONAL_STRING_LITERAL = 963,
      STRING_LITERAL = 964,
      DECIMAL_LITERAL = 965,
      HEXADECIMAL_LITERAL = 966,
      REAL_LITERAL = 967,
      NULL_SPEC_LITERAL = 968,
      BIT_STRING = 969,
      STRING_CHARSET_NAME = 970,
      DOT_ID = 971,
      ID = 972,
      REVERSE_QUOTE_ID = 973,
      STRING_USER_NAME = 974,
      LOCAL_ID = 975,
      GLOBAL_ID = 976,
      ERROR_RECONGNIGION = 977;
  public static final int RULE_intervalType = 0,
      RULE_fullId = 1,
      RULE_fullColumnName = 2,
      RULE_charsetName = 3,
      RULE_collationName = 4,
      RULE_engineName = 5,
      RULE_uid = 6,
      RULE_simpleId = 7,
      RULE_dottedId = 8,
      RULE_decimalLiteral = 9,
      RULE_stringLiteral = 10,
      RULE_booleanLiteral = 11,
      RULE_hexadecimalLiteral = 12,
      RULE_nullNotnull = 13,
      RULE_constant = 14,
      RULE_convertedDataType = 15,
      RULE_lengthOneDimension = 16,
      RULE_lengthTwoDimension = 17,
      RULE_expressions = 18,
      RULE_currentTimestamp = 19,
      RULE_functionCall = 20,
      RULE_specificFunction = 21,
      RULE_caseFuncAlternative = 22,
      RULE_levelsInWeightString = 23,
      RULE_levelInWeightListElement = 24,
      RULE_scalarFunctionName = 25,
      RULE_functionArgs = 26,
      RULE_functionArg = 27,
      RULE_expression = 28,
      RULE_predicate = 29,
      RULE_expressionAtom = 30,
      RULE_unaryOperator = 31,
      RULE_comparisonOperator = 32,
      RULE_logicalOperator = 33,
      RULE_bitOperator = 34,
      RULE_mathOperator = 35,
      RULE_charsetNameBase = 36,
      RULE_transactionLevelBase = 37,
      RULE_privilegesBase = 38,
      RULE_intervalTypeBase = 39,
      RULE_dataTypeBase = 40,
      RULE_keywordsCanBeId = 41,
      RULE_functionNameBase = 42;
  public static final String[] ruleNames = {
    "intervalType",
    "fullId",
    "fullColumnName",
    "charsetName",
    "collationName",
    "engineName",
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
    "transactionLevelBase",
    "privilegesBase",
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
    "START_NATIONAL_STRING_LITERAL",
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
      setState(99);
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
            setState(86);
            intervalTypeBase();
          }
          break;
        case YEAR:
          enterOuterAlt(_localctx, 2);
          {
            setState(87);
            match(YEAR);
          }
          break;
        case YEAR_MONTH:
          enterOuterAlt(_localctx, 3);
          {
            setState(88);
            match(YEAR_MONTH);
          }
          break;
        case DAY_HOUR:
          enterOuterAlt(_localctx, 4);
          {
            setState(89);
            match(DAY_HOUR);
          }
          break;
        case DAY_MINUTE:
          enterOuterAlt(_localctx, 5);
          {
            setState(90);
            match(DAY_MINUTE);
          }
          break;
        case DAY_SECOND:
          enterOuterAlt(_localctx, 6);
          {
            setState(91);
            match(DAY_SECOND);
          }
          break;
        case HOUR_MINUTE:
          enterOuterAlt(_localctx, 7);
          {
            setState(92);
            match(HOUR_MINUTE);
          }
          break;
        case HOUR_SECOND:
          enterOuterAlt(_localctx, 8);
          {
            setState(93);
            match(HOUR_SECOND);
          }
          break;
        case MINUTE_SECOND:
          enterOuterAlt(_localctx, 9);
          {
            setState(94);
            match(MINUTE_SECOND);
          }
          break;
        case SECOND_MICROSECOND:
          enterOuterAlt(_localctx, 10);
          {
            setState(95);
            match(SECOND_MICROSECOND);
          }
          break;
        case MINUTE_MICROSECOND:
          enterOuterAlt(_localctx, 11);
          {
            setState(96);
            match(MINUTE_MICROSECOND);
          }
          break;
        case HOUR_MICROSECOND:
          enterOuterAlt(_localctx, 12);
          {
            setState(97);
            match(HOUR_MICROSECOND);
          }
          break;
        case DAY_MICROSECOND:
          enterOuterAlt(_localctx, 13);
          {
            setState(98);
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
        setState(101);
        uid();
        setState(105);
        _errHandler.sync(this);
        switch (_input.LA(1)) {
          case DOT_ID:
            {
              setState(102);
              match(DOT_ID);
            }
            break;
          case DOT:
            {
              setState(103);
              match(DOT);
              setState(104);
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
        setState(107);
        uid();
        setState(112);
        _errHandler.sync(this);
        switch (getInterpreter().adaptivePredict(_input, 3, _ctx)) {
          case 1:
            {
              setState(108);
              dottedId();
              setState(110);
              _errHandler.sync(this);
              switch (getInterpreter().adaptivePredict(_input, 2, _ctx)) {
                case 1:
                  {
                    setState(109);
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
      setState(118);
      _errHandler.sync(this);
      switch (_input.LA(1)) {
        case BINARY:
          enterOuterAlt(_localctx, 1);
          {
            setState(114);
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
            setState(115);
            charsetNameBase();
          }
          break;
        case STRING_LITERAL:
          enterOuterAlt(_localctx, 3);
          {
            setState(116);
            match(STRING_LITERAL);
          }
          break;
        case CHARSET_REVERSE_QOUTE_STRING:
          enterOuterAlt(_localctx, 4);
          {
            setState(117);
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
      setState(122);
      _errHandler.sync(this);
      switch (getInterpreter().adaptivePredict(_input, 5, _ctx)) {
        case 1:
          enterOuterAlt(_localctx, 1);
          {
            setState(120);
            uid();
          }
          break;
        case 2:
          enterOuterAlt(_localctx, 2);
          {
            setState(121);
            match(STRING_LITERAL);
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

  public static class EngineNameContext extends ParserRuleContext {
    public TerminalNode ARCHIVE() {
      return getToken(MySqlParser.ARCHIVE, 0);
    }

    public TerminalNode BLACKHOLE() {
      return getToken(MySqlParser.BLACKHOLE, 0);
    }

    public TerminalNode CSV() {
      return getToken(MySqlParser.CSV, 0);
    }

    public TerminalNode FEDERATED() {
      return getToken(MySqlParser.FEDERATED, 0);
    }

    public TerminalNode INNODB() {
      return getToken(MySqlParser.INNODB, 0);
    }

    public TerminalNode MEMORY() {
      return getToken(MySqlParser.MEMORY, 0);
    }

    public TerminalNode MRG_MYISAM() {
      return getToken(MySqlParser.MRG_MYISAM, 0);
    }

    public TerminalNode MYISAM() {
      return getToken(MySqlParser.MYISAM, 0);
    }

    public TerminalNode NDB() {
      return getToken(MySqlParser.NDB, 0);
    }

    public TerminalNode NDBCLUSTER() {
      return getToken(MySqlParser.NDBCLUSTER, 0);
    }

    public TerminalNode PERFOMANCE_SCHEMA() {
      return getToken(MySqlParser.PERFOMANCE_SCHEMA, 0);
    }

    public TerminalNode STRING_LITERAL() {
      return getToken(MySqlParser.STRING_LITERAL, 0);
    }

    public TerminalNode REVERSE_QUOTE_ID() {
      return getToken(MySqlParser.REVERSE_QUOTE_ID, 0);
    }

    public EngineNameContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_engineName;
    }

    @Override
    public void enterRule(ParseTreeListener listener) {
      if (listener instanceof MySqlParserListener)
        ((MySqlParserListener) listener).enterEngineName(this);
    }

    @Override
    public void exitRule(ParseTreeListener listener) {
      if (listener instanceof MySqlParserListener)
        ((MySqlParserListener) listener).exitEngineName(this);
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof MySqlParserVisitor)
        return ((MySqlParserVisitor<? extends T>) visitor).visitEngineName(this);
      else return visitor.visitChildren(this);
    }
  }

  public final EngineNameContext engineName() throws RecognitionException {
    EngineNameContext _localctx = new EngineNameContext(_ctx, getState());
    enterRule(_localctx, 10, RULE_engineName);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
        setState(124);
        _la = _input.LA(1);
        if (!(((((_la - 611)) & ~0x3f) == 0
                && ((1L << (_la - 611))
                        & ((1L << (ARCHIVE - 611))
                            | (1L << (BLACKHOLE - 611))
                            | (1L << (CSV - 611))
                            | (1L << (FEDERATED - 611))
                            | (1L << (INNODB - 611))
                            | (1L << (MEMORY - 611))
                            | (1L << (MRG_MYISAM - 611))
                            | (1L << (MYISAM - 611))
                            | (1L << (NDB - 611))
                            | (1L << (NDBCLUSTER - 611))
                            | (1L << (PERFOMANCE_SCHEMA - 611))))
                    != 0)
            || _la == STRING_LITERAL
            || _la == REVERSE_QUOTE_ID)) {
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
    enterRule(_localctx, 12, RULE_uid);
    try {
      setState(129);
      _errHandler.sync(this);
      switch (getInterpreter().adaptivePredict(_input, 6, _ctx)) {
        case 1:
          enterOuterAlt(_localctx, 1);
          {
            setState(126);
            simpleId();
          }
          break;
        case 2:
          enterOuterAlt(_localctx, 2);
          {
            setState(127);
            match(REVERSE_QUOTE_ID);
          }
          break;
        case 3:
          enterOuterAlt(_localctx, 3);
          {
            setState(128);
            match(CHARSET_REVERSE_QOUTE_STRING);
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

  public static class SimpleIdContext extends ParserRuleContext {
    public TerminalNode ID() {
      return getToken(MySqlParser.ID, 0);
    }

    public CharsetNameBaseContext charsetNameBase() {
      return getRuleContext(CharsetNameBaseContext.class, 0);
    }

    public TransactionLevelBaseContext transactionLevelBase() {
      return getRuleContext(TransactionLevelBaseContext.class, 0);
    }

    public EngineNameContext engineName() {
      return getRuleContext(EngineNameContext.class, 0);
    }

    public PrivilegesBaseContext privilegesBase() {
      return getRuleContext(PrivilegesBaseContext.class, 0);
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
    enterRule(_localctx, 14, RULE_simpleId);
    try {
      setState(140);
      _errHandler.sync(this);
      switch (getInterpreter().adaptivePredict(_input, 7, _ctx)) {
        case 1:
          enterOuterAlt(_localctx, 1);
          {
            setState(131);
            match(ID);
          }
          break;
        case 2:
          enterOuterAlt(_localctx, 2);
          {
            setState(132);
            charsetNameBase();
          }
          break;
        case 3:
          enterOuterAlt(_localctx, 3);
          {
            setState(133);
            transactionLevelBase();
          }
          break;
        case 4:
          enterOuterAlt(_localctx, 4);
          {
            setState(134);
            engineName();
          }
          break;
        case 5:
          enterOuterAlt(_localctx, 5);
          {
            setState(135);
            privilegesBase();
          }
          break;
        case 6:
          enterOuterAlt(_localctx, 6);
          {
            setState(136);
            intervalTypeBase();
          }
          break;
        case 7:
          enterOuterAlt(_localctx, 7);
          {
            setState(137);
            dataTypeBase();
          }
          break;
        case 8:
          enterOuterAlt(_localctx, 8);
          {
            setState(138);
            keywordsCanBeId();
          }
          break;
        case 9:
          enterOuterAlt(_localctx, 9);
          {
            setState(139);
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
    enterRule(_localctx, 16, RULE_dottedId);
    try {
      setState(145);
      _errHandler.sync(this);
      switch (_input.LA(1)) {
        case DOT_ID:
          enterOuterAlt(_localctx, 1);
          {
            setState(142);
            match(DOT_ID);
          }
          break;
        case DOT:
          enterOuterAlt(_localctx, 2);
          {
            setState(143);
            match(DOT);
            setState(144);
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
    enterRule(_localctx, 18, RULE_decimalLiteral);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
        setState(147);
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

    public TerminalNode START_NATIONAL_STRING_LITERAL() {
      return getToken(MySqlParser.START_NATIONAL_STRING_LITERAL, 0);
    }

    public TerminalNode STRING_CHARSET_NAME() {
      return getToken(MySqlParser.STRING_CHARSET_NAME, 0);
    }

    public TerminalNode COLLATE() {
      return getToken(MySqlParser.COLLATE, 0);
    }

    public CollationNameContext collationName() {
      return getRuleContext(CollationNameContext.class, 0);
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
    enterRule(_localctx, 20, RULE_stringLiteral);
    int _la;
    try {
      int _alt;
      setState(172);
      _errHandler.sync(this);
      switch (getInterpreter().adaptivePredict(_input, 15, _ctx)) {
        case 1:
          enterOuterAlt(_localctx, 1);
          {
            setState(154);
            _errHandler.sync(this);
            switch (_input.LA(1)) {
              case STRING_LITERAL:
              case STRING_CHARSET_NAME:
                {
                  setState(150);
                  _errHandler.sync(this);
                  _la = _input.LA(1);
                  if (_la == STRING_CHARSET_NAME) {
                    {
                      setState(149);
                      match(STRING_CHARSET_NAME);
                    }
                  }

                  setState(152);
                  match(STRING_LITERAL);
                }
                break;
              case START_NATIONAL_STRING_LITERAL:
                {
                  setState(153);
                  match(START_NATIONAL_STRING_LITERAL);
                }
                break;
              default:
                throw new NoViableAltException(this);
            }
            setState(157);
            _errHandler.sync(this);
            _alt = 1;
            do {
              switch (_alt) {
                case 1:
                  {
                    {
                      setState(156);
                      match(STRING_LITERAL);
                    }
                  }
                  break;
                default:
                  throw new NoViableAltException(this);
              }
              setState(159);
              _errHandler.sync(this);
              _alt = getInterpreter().adaptivePredict(_input, 11, _ctx);
            } while (_alt != 2 && _alt != org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER);
          }
          break;
        case 2:
          enterOuterAlt(_localctx, 2);
          {
            setState(166);
            _errHandler.sync(this);
            switch (_input.LA(1)) {
              case STRING_LITERAL:
              case STRING_CHARSET_NAME:
                {
                  setState(162);
                  _errHandler.sync(this);
                  _la = _input.LA(1);
                  if (_la == STRING_CHARSET_NAME) {
                    {
                      setState(161);
                      match(STRING_CHARSET_NAME);
                    }
                  }

                  setState(164);
                  match(STRING_LITERAL);
                }
                break;
              case START_NATIONAL_STRING_LITERAL:
                {
                  setState(165);
                  match(START_NATIONAL_STRING_LITERAL);
                }
                break;
              default:
                throw new NoViableAltException(this);
            }
            setState(170);
            _errHandler.sync(this);
            switch (getInterpreter().adaptivePredict(_input, 14, _ctx)) {
              case 1:
                {
                  setState(168);
                  match(COLLATE);
                  setState(169);
                  collationName();
                }
                break;
            }
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
    enterRule(_localctx, 22, RULE_booleanLiteral);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
        setState(174);
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
    enterRule(_localctx, 24, RULE_hexadecimalLiteral);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
        setState(177);
        _errHandler.sync(this);
        _la = _input.LA(1);
        if (_la == STRING_CHARSET_NAME) {
          {
            setState(176);
            match(STRING_CHARSET_NAME);
          }
        }

        setState(179);
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
    enterRule(_localctx, 26, RULE_nullNotnull);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
        setState(182);
        _errHandler.sync(this);
        _la = _input.LA(1);
        if (_la == NOT) {
          {
            setState(181);
            match(NOT);
          }
        }

        setState(184);
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
    enterRule(_localctx, 28, RULE_constant);
    int _la;
    try {
      setState(198);
      _errHandler.sync(this);
      switch (getInterpreter().adaptivePredict(_input, 19, _ctx)) {
        case 1:
          enterOuterAlt(_localctx, 1);
          {
            setState(186);
            stringLiteral();
          }
          break;
        case 2:
          enterOuterAlt(_localctx, 2);
          {
            setState(187);
            decimalLiteral();
          }
          break;
        case 3:
          enterOuterAlt(_localctx, 3);
          {
            setState(188);
            match(MINUS);
            setState(189);
            decimalLiteral();
          }
          break;
        case 4:
          enterOuterAlt(_localctx, 4);
          {
            setState(190);
            hexadecimalLiteral();
          }
          break;
        case 5:
          enterOuterAlt(_localctx, 5);
          {
            setState(191);
            booleanLiteral();
          }
          break;
        case 6:
          enterOuterAlt(_localctx, 6);
          {
            setState(192);
            match(REAL_LITERAL);
          }
          break;
        case 7:
          enterOuterAlt(_localctx, 7);
          {
            setState(193);
            match(BIT_STRING);
          }
          break;
        case 8:
          enterOuterAlt(_localctx, 8);
          {
            setState(195);
            _errHandler.sync(this);
            _la = _input.LA(1);
            if (_la == NOT) {
              {
                setState(194);
                match(NOT);
              }
            }

            setState(197);
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
    enterRule(_localctx, 30, RULE_convertedDataType);
    int _la;
    try {
      setState(222);
      _errHandler.sync(this);
      switch (_input.LA(1)) {
        case BINARY:
        case NCHAR:
          enterOuterAlt(_localctx, 1);
          {
            setState(200);
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
            setState(202);
            _errHandler.sync(this);
            _la = _input.LA(1);
            if (_la == LR_BRACKET) {
              {
                setState(201);
                lengthOneDimension();
              }
            }
          }
          break;
        case CHAR:
          enterOuterAlt(_localctx, 2);
          {
            setState(204);
            ((ConvertedDataTypeContext) _localctx).typeName = match(CHAR);
            setState(206);
            _errHandler.sync(this);
            _la = _input.LA(1);
            if (_la == LR_BRACKET) {
              {
                setState(205);
                lengthOneDimension();
              }
            }

            setState(211);
            _errHandler.sync(this);
            _la = _input.LA(1);
            if (_la == CHARACTER) {
              {
                setState(208);
                match(CHARACTER);
                setState(209);
                match(SET);
                setState(210);
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
            setState(213);
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
            setState(214);
            ((ConvertedDataTypeContext) _localctx).typeName = match(DECIMAL);
            setState(216);
            _errHandler.sync(this);
            _la = _input.LA(1);
            if (_la == LR_BRACKET) {
              {
                setState(215);
                lengthTwoDimension();
              }
            }
          }
          break;
        case UNSIGNED:
        case SIGNED:
          enterOuterAlt(_localctx, 5);
          {
            setState(218);
            _la = _input.LA(1);
            if (!(_la == UNSIGNED || _la == SIGNED)) {
              _errHandler.recoverInline(this);
            } else {
              if (_input.LA(1) == Token.EOF) matchedEOF = true;
              _errHandler.reportMatch(this);
              consume();
            }
            setState(220);
            _errHandler.sync(this);
            _la = _input.LA(1);
            if (_la == INTEGER) {
              {
                setState(219);
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
    enterRule(_localctx, 32, RULE_lengthOneDimension);
    try {
      enterOuterAlt(_localctx, 1);
      {
        setState(224);
        match(LR_BRACKET);
        setState(225);
        decimalLiteral();
        setState(226);
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
    enterRule(_localctx, 34, RULE_lengthTwoDimension);
    try {
      enterOuterAlt(_localctx, 1);
      {
        setState(228);
        match(LR_BRACKET);
        setState(229);
        decimalLiteral();
        setState(230);
        match(COMMA);
        setState(231);
        decimalLiteral();
        setState(232);
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
    enterRule(_localctx, 36, RULE_expressions);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
        setState(234);
        expression(0);
        setState(239);
        _errHandler.sync(this);
        _la = _input.LA(1);
        while (_la == COMMA) {
          {
            {
              setState(235);
              match(COMMA);
              setState(236);
              expression(0);
            }
          }
          setState(241);
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
    enterRule(_localctx, 38, RULE_currentTimestamp);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
        setState(256);
        _errHandler.sync(this);
        switch (_input.LA(1)) {
          case CURRENT_TIMESTAMP:
          case LOCALTIME:
          case LOCALTIMESTAMP:
            {
              setState(242);
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
              setState(248);
              _errHandler.sync(this);
              _la = _input.LA(1);
              if (_la == LR_BRACKET) {
                {
                  setState(243);
                  match(LR_BRACKET);
                  setState(245);
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
                      setState(244);
                      decimalLiteral();
                    }
                  }

                  setState(247);
                  match(RR_BRACKET);
                }
              }
            }
            break;
          case NOW:
            {
              setState(250);
              match(NOW);
              setState(251);
              match(LR_BRACKET);
              setState(253);
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
                  setState(252);
                  decimalLiteral();
                }
              }

              setState(255);
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
    enterRule(_localctx, 40, RULE_functionCall);
    int _la;
    try {
      setState(273);
      _errHandler.sync(this);
      switch (getInterpreter().adaptivePredict(_input, 33, _ctx)) {
        case 1:
          _localctx = new SpecificFunctionCallContext(_localctx);
          enterOuterAlt(_localctx, 1);
          {
            setState(258);
            specificFunction();
          }
          break;
        case 2:
          _localctx = new ScalarFunctionCallContext(_localctx);
          enterOuterAlt(_localctx, 2);
          {
            setState(259);
            scalarFunctionName();
            setState(260);
            match(LR_BRACKET);
            setState(262);
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
                                | (1L << (TABLES - 547))
                                | (1L << (ROUTINE - 547))
                                | (1L << (EXECUTE - 547))
                                | (1L << (FILE - 547))
                                | (1L << (PROCESS - 547))
                                | (1L << (RELOAD - 547))
                                | (1L << (SHUTDOWN - 547))
                                | (1L << (SUPER - 547))
                                | (1L << (PRIVILEGES - 547))
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
                || ((((_la - 611)) & ~0x3f) == 0
                    && ((1L << (_la - 611))
                            & ((1L << (ARCHIVE - 611))
                                | (1L << (BLACKHOLE - 611))
                                | (1L << (CSV - 611))
                                | (1L << (FEDERATED - 611))
                                | (1L << (INNODB - 611))
                                | (1L << (MEMORY - 611))
                                | (1L << (MRG_MYISAM - 611))
                                | (1L << (MYISAM - 611))
                                | (1L << (NDB - 611))
                                | (1L << (NDBCLUSTER - 611))
                                | (1L << (PERFOMANCE_SCHEMA - 611))
                                | (1L << (REPEATABLE - 611))
                                | (1L << (COMMITTED - 611))
                                | (1L << (UNCOMMITTED - 611))
                                | (1L << (SERIALIZABLE - 611))
                                | (1L << (GEOMETRYCOLLECTION - 611))
                                | (1L << (LINESTRING - 611))
                                | (1L << (MULTILINESTRING - 611))
                                | (1L << (MULTIPOINT - 611))
                                | (1L << (MULTIPOLYGON - 611))
                                | (1L << (POINT - 611))
                                | (1L << (POLYGON - 611))
                                | (1L << (ABS - 611))
                                | (1L << (ACOS - 611))
                                | (1L << (ADDDATE - 611))
                                | (1L << (ADDTIME - 611))
                                | (1L << (AES_DECRYPT - 611))
                                | (1L << (AES_ENCRYPT - 611))
                                | (1L << (AREA - 611))
                                | (1L << (ASBINARY - 611))
                                | (1L << (ASIN - 611))
                                | (1L << (ASTEXT - 611))
                                | (1L << (ASWKB - 611))
                                | (1L << (ASWKT - 611))
                                | (1L << (ASYMMETRIC_DECRYPT - 611))
                                | (1L << (ASYMMETRIC_DERIVE - 611))
                                | (1L << (ASYMMETRIC_ENCRYPT - 611))
                                | (1L << (ASYMMETRIC_SIGN - 611))
                                | (1L << (ASYMMETRIC_VERIFY - 611))
                                | (1L << (ATAN - 611))
                                | (1L << (ATAN2 - 611))
                                | (1L << (BENCHMARK - 611))
                                | (1L << (BIN - 611))
                                | (1L << (BIT_COUNT - 611))
                                | (1L << (BIT_LENGTH - 611))
                                | (1L << (BUFFER - 611))
                                | (1L << (CEIL - 611))
                                | (1L << (CEILING - 611))
                                | (1L << (CENTROID - 611))
                                | (1L << (CHARACTER_LENGTH - 611))
                                | (1L << (CHARSET - 611))
                                | (1L << (CHAR_LENGTH - 611))
                                | (1L << (COERCIBILITY - 611))
                                | (1L << (COLLATION - 611))
                                | (1L << (COMPRESS - 611))
                                | (1L << (CONCAT - 611))
                                | (1L << (CONCAT_WS - 611))
                                | (1L << (CONNECTION_ID - 611))
                                | (1L << (CONV - 611))
                                | (1L << (CONVERT_TZ - 611))
                                | (1L << (COS - 611))
                                | (1L << (COT - 611))
                                | (1L << (CRC32 - 611))
                                | (1L << (CREATE_ASYMMETRIC_PRIV_KEY - 611))))
                        != 0)
                || ((((_la - 675)) & ~0x3f) == 0
                    && ((1L << (_la - 675))
                            & ((1L << (CREATE_ASYMMETRIC_PUB_KEY - 675))
                                | (1L << (CREATE_DH_PARAMETERS - 675))
                                | (1L << (CREATE_DIGEST - 675))
                                | (1L << (CROSSES - 675))
                                | (1L << (DATEDIFF - 675))
                                | (1L << (DATE_FORMAT - 675))
                                | (1L << (DAYNAME - 675))
                                | (1L << (DAYOFMONTH - 675))
                                | (1L << (DAYOFWEEK - 675))
                                | (1L << (DAYOFYEAR - 675))
                                | (1L << (DECODE - 675))
                                | (1L << (DEGREES - 675))
                                | (1L << (DES_DECRYPT - 675))
                                | (1L << (DES_ENCRYPT - 675))
                                | (1L << (DIMENSION - 675))
                                | (1L << (DISJOINT - 675))
                                | (1L << (ELT - 675))
                                | (1L << (ENCODE - 675))
                                | (1L << (ENCRYPT - 675))
                                | (1L << (ENDPOINT - 675))
                                | (1L << (ENVELOPE - 675))
                                | (1L << (EQUALS - 675))
                                | (1L << (EXP - 675))
                                | (1L << (EXPORT_SET - 675))
                                | (1L << (EXTERIORRING - 675))
                                | (1L << (EXTRACTVALUE - 675))
                                | (1L << (FIELD - 675))
                                | (1L << (FIND_IN_SET - 675))
                                | (1L << (FLOOR - 675))
                                | (1L << (FORMAT - 675))
                                | (1L << (FOUND_ROWS - 675))
                                | (1L << (FROM_BASE64 - 675))
                                | (1L << (FROM_DAYS - 675))
                                | (1L << (FROM_UNIXTIME - 675))
                                | (1L << (GEOMCOLLFROMTEXT - 675))
                                | (1L << (GEOMCOLLFROMWKB - 675))
                                | (1L << (GEOMETRYCOLLECTIONFROMTEXT - 675))
                                | (1L << (GEOMETRYCOLLECTIONFROMWKB - 675))
                                | (1L << (GEOMETRYFROMTEXT - 675))
                                | (1L << (GEOMETRYFROMWKB - 675))
                                | (1L << (GEOMETRYN - 675))
                                | (1L << (GEOMETRYTYPE - 675))
                                | (1L << (GEOMFROMTEXT - 675))
                                | (1L << (GEOMFROMWKB - 675))
                                | (1L << (GET_FORMAT - 675))
                                | (1L << (GET_LOCK - 675))
                                | (1L << (GLENGTH - 675))
                                | (1L << (GREATEST - 675))
                                | (1L << (GTID_SUBSET - 675))
                                | (1L << (GTID_SUBTRACT - 675))
                                | (1L << (HEX - 675))
                                | (1L << (IFNULL - 675))
                                | (1L << (INET6_ATON - 675))
                                | (1L << (INET6_NTOA - 675))
                                | (1L << (INET_ATON - 675))
                                | (1L << (INET_NTOA - 675))
                                | (1L << (INSTR - 675))
                                | (1L << (INTERIORRINGN - 675))
                                | (1L << (INTERSECTS - 675))
                                | (1L << (ISCLOSED - 675))
                                | (1L << (ISEMPTY - 675))
                                | (1L << (ISNULL - 675))
                                | (1L << (ISSIMPLE - 675))
                                | (1L << (IS_FREE_LOCK - 675))))
                        != 0)
                || ((((_la - 739)) & ~0x3f) == 0
                    && ((1L << (_la - 739))
                            & ((1L << (IS_IPV4 - 739))
                                | (1L << (IS_IPV4_COMPAT - 739))
                                | (1L << (IS_IPV4_MAPPED - 739))
                                | (1L << (IS_IPV6 - 739))
                                | (1L << (IS_USED_LOCK - 739))
                                | (1L << (LAST_INSERT_ID - 739))
                                | (1L << (LCASE - 739))
                                | (1L << (LEAST - 739))
                                | (1L << (LENGTH - 739))
                                | (1L << (LINEFROMTEXT - 739))
                                | (1L << (LINEFROMWKB - 739))
                                | (1L << (LINESTRINGFROMTEXT - 739))
                                | (1L << (LINESTRINGFROMWKB - 739))
                                | (1L << (LN - 739))
                                | (1L << (LOAD_FILE - 739))
                                | (1L << (LOCATE - 739))
                                | (1L << (LOG - 739))
                                | (1L << (LOG10 - 739))
                                | (1L << (LOG2 - 739))
                                | (1L << (LOWER - 739))
                                | (1L << (LPAD - 739))
                                | (1L << (LTRIM - 739))
                                | (1L << (MAKEDATE - 739))
                                | (1L << (MAKETIME - 739))
                                | (1L << (MAKE_SET - 739))
                                | (1L << (MASTER_POS_WAIT - 739))
                                | (1L << (MBRCONTAINS - 739))
                                | (1L << (MBRDISJOINT - 739))
                                | (1L << (MBREQUAL - 739))
                                | (1L << (MBRINTERSECTS - 739))
                                | (1L << (MBROVERLAPS - 739))
                                | (1L << (MBRTOUCHES - 739))
                                | (1L << (MBRWITHIN - 739))
                                | (1L << (MD5 - 739))
                                | (1L << (MLINEFROMTEXT - 739))
                                | (1L << (MLINEFROMWKB - 739))
                                | (1L << (MONTHNAME - 739))
                                | (1L << (MPOINTFROMTEXT - 739))
                                | (1L << (MPOINTFROMWKB - 739))
                                | (1L << (MPOLYFROMTEXT - 739))
                                | (1L << (MPOLYFROMWKB - 739))
                                | (1L << (MULTILINESTRINGFROMTEXT - 739))
                                | (1L << (MULTILINESTRINGFROMWKB - 739))
                                | (1L << (MULTIPOINTFROMTEXT - 739))
                                | (1L << (MULTIPOINTFROMWKB - 739))
                                | (1L << (MULTIPOLYGONFROMTEXT - 739))
                                | (1L << (MULTIPOLYGONFROMWKB - 739))
                                | (1L << (NAME_CONST - 739))
                                | (1L << (NULLIF - 739))
                                | (1L << (NUMGEOMETRIES - 739))
                                | (1L << (NUMINTERIORRINGS - 739))
                                | (1L << (NUMPOINTS - 739))
                                | (1L << (OCT - 739))
                                | (1L << (OCTET_LENGTH - 739))
                                | (1L << (ORD - 739))
                                | (1L << (OVERLAPS - 739))
                                | (1L << (PERIOD_ADD - 739))
                                | (1L << (PERIOD_DIFF - 739))
                                | (1L << (PI - 739))
                                | (1L << (POINTFROMTEXT - 739))
                                | (1L << (POINTFROMWKB - 739))
                                | (1L << (POINTN - 739))
                                | (1L << (POLYFROMTEXT - 739))
                                | (1L << (POLYFROMWKB - 739))))
                        != 0)
                || ((((_la - 803)) & ~0x3f) == 0
                    && ((1L << (_la - 803))
                            & ((1L << (POLYGONFROMTEXT - 803))
                                | (1L << (POLYGONFROMWKB - 803))
                                | (1L << (POW - 803))
                                | (1L << (POWER - 803))
                                | (1L << (QUOTE - 803))
                                | (1L << (RADIANS - 803))
                                | (1L << (RAND - 803))
                                | (1L << (RANDOM_BYTES - 803))
                                | (1L << (RELEASE_LOCK - 803))
                                | (1L << (REVERSE - 803))
                                | (1L << (ROUND - 803))
                                | (1L << (ROW_COUNT - 803))
                                | (1L << (RPAD - 803))
                                | (1L << (RTRIM - 803))
                                | (1L << (SEC_TO_TIME - 803))
                                | (1L << (SESSION_USER - 803))
                                | (1L << (SHA - 803))
                                | (1L << (SHA1 - 803))
                                | (1L << (SHA2 - 803))
                                | (1L << (SIGN - 803))
                                | (1L << (SIN - 803))
                                | (1L << (SLEEP - 803))
                                | (1L << (SOUNDEX - 803))
                                | (1L << (SQL_THREAD_WAIT_AFTER_GTIDS - 803))
                                | (1L << (SQRT - 803))
                                | (1L << (SRID - 803))
                                | (1L << (STARTPOINT - 803))
                                | (1L << (STRCMP - 803))
                                | (1L << (STR_TO_DATE - 803))
                                | (1L << (ST_AREA - 803))
                                | (1L << (ST_ASBINARY - 803))
                                | (1L << (ST_ASTEXT - 803))
                                | (1L << (ST_ASWKB - 803))
                                | (1L << (ST_ASWKT - 803))
                                | (1L << (ST_BUFFER - 803))
                                | (1L << (ST_CENTROID - 803))
                                | (1L << (ST_CONTAINS - 803))
                                | (1L << (ST_CROSSES - 803))
                                | (1L << (ST_DIFFERENCE - 803))
                                | (1L << (ST_DIMENSION - 803))
                                | (1L << (ST_DISJOINT - 803))
                                | (1L << (ST_DISTANCE - 803))
                                | (1L << (ST_ENDPOINT - 803))
                                | (1L << (ST_ENVELOPE - 803))
                                | (1L << (ST_EQUALS - 803))
                                | (1L << (ST_EXTERIORRING - 803))
                                | (1L << (ST_GEOMCOLLFROMTEXT - 803))
                                | (1L << (ST_GEOMCOLLFROMTXT - 803))
                                | (1L << (ST_GEOMCOLLFROMWKB - 803))
                                | (1L << (ST_GEOMETRYCOLLECTIONFROMTEXT - 803))
                                | (1L << (ST_GEOMETRYCOLLECTIONFROMWKB - 803))
                                | (1L << (ST_GEOMETRYFROMTEXT - 803))
                                | (1L << (ST_GEOMETRYFROMWKB - 803))
                                | (1L << (ST_GEOMETRYN - 803))
                                | (1L << (ST_GEOMETRYTYPE - 803))
                                | (1L << (ST_GEOMFROMTEXT - 803))
                                | (1L << (ST_GEOMFROMWKB - 803))
                                | (1L << (ST_INTERIORRINGN - 803))
                                | (1L << (ST_INTERSECTION - 803))
                                | (1L << (ST_INTERSECTS - 803))
                                | (1L << (ST_ISCLOSED - 803))
                                | (1L << (ST_ISEMPTY - 803))
                                | (1L << (ST_ISSIMPLE - 803))
                                | (1L << (ST_LINEFROMTEXT - 803))))
                        != 0)
                || ((((_la - 867)) & ~0x3f) == 0
                    && ((1L << (_la - 867))
                            & ((1L << (ST_LINEFROMWKB - 867))
                                | (1L << (ST_LINESTRINGFROMTEXT - 867))
                                | (1L << (ST_LINESTRINGFROMWKB - 867))
                                | (1L << (ST_NUMGEOMETRIES - 867))
                                | (1L << (ST_NUMINTERIORRING - 867))
                                | (1L << (ST_NUMINTERIORRINGS - 867))
                                | (1L << (ST_NUMPOINTS - 867))
                                | (1L << (ST_OVERLAPS - 867))
                                | (1L << (ST_POINTFROMTEXT - 867))
                                | (1L << (ST_POINTFROMWKB - 867))
                                | (1L << (ST_POINTN - 867))
                                | (1L << (ST_POLYFROMTEXT - 867))
                                | (1L << (ST_POLYFROMWKB - 867))
                                | (1L << (ST_POLYGONFROMTEXT - 867))
                                | (1L << (ST_POLYGONFROMWKB - 867))
                                | (1L << (ST_SRID - 867))
                                | (1L << (ST_STARTPOINT - 867))
                                | (1L << (ST_SYMDIFFERENCE - 867))
                                | (1L << (ST_TOUCHES - 867))
                                | (1L << (ST_UNION - 867))
                                | (1L << (ST_WITHIN - 867))
                                | (1L << (ST_X - 867))
                                | (1L << (ST_Y - 867))
                                | (1L << (SUBDATE - 867))
                                | (1L << (SUBSTRING_INDEX - 867))
                                | (1L << (SUBTIME - 867))
                                | (1L << (SYSTEM_USER - 867))
                                | (1L << (TAN - 867))
                                | (1L << (TIMEDIFF - 867))
                                | (1L << (TIMESTAMPADD - 867))
                                | (1L << (TIMESTAMPDIFF - 867))
                                | (1L << (TIME_FORMAT - 867))
                                | (1L << (TIME_TO_SEC - 867))
                                | (1L << (TOUCHES - 867))
                                | (1L << (TO_BASE64 - 867))
                                | (1L << (TO_DAYS - 867))
                                | (1L << (TO_SECONDS - 867))
                                | (1L << (UCASE - 867))
                                | (1L << (UNCOMPRESS - 867))
                                | (1L << (UNCOMPRESSED_LENGTH - 867))
                                | (1L << (UNHEX - 867))
                                | (1L << (UNIX_TIMESTAMP - 867))
                                | (1L << (UPDATEXML - 867))
                                | (1L << (UPPER - 867))
                                | (1L << (UUID - 867))
                                | (1L << (UUID_SHORT - 867))
                                | (1L << (VALIDATE_PASSWORD_STRENGTH - 867))
                                | (1L << (VERSION - 867))
                                | (1L << (WAIT_UNTIL_SQL_THREAD_AFTER_GTIDS - 867))
                                | (1L << (WEEKDAY - 867))
                                | (1L << (WEEKOFYEAR - 867))
                                | (1L << (WEIGHT_STRING - 867))
                                | (1L << (WITHIN - 867))
                                | (1L << (YEARWEEK - 867))
                                | (1L << (Y_FUNCTION - 867))
                                | (1L << (X_FUNCTION - 867))))
                        != 0)
                || ((((_la - 935)) & ~0x3f) == 0
                    && ((1L << (_la - 935))
                            & ((1L << (PLUS - 935))
                                | (1L << (MINUS - 935))
                                | (1L << (EXCLAMATION_SYMBOL - 935))
                                | (1L << (BIT_NOT_OP - 935))
                                | (1L << (LR_BRACKET - 935))
                                | (1L << (ZERO_DECIMAL - 935))
                                | (1L << (ONE_DECIMAL - 935))
                                | (1L << (TWO_DECIMAL - 935))
                                | (1L << (CHARSET_REVERSE_QOUTE_STRING - 935))
                                | (1L << (START_NATIONAL_STRING_LITERAL - 935))
                                | (1L << (STRING_LITERAL - 935))
                                | (1L << (DECIMAL_LITERAL - 935))
                                | (1L << (HEXADECIMAL_LITERAL - 935))
                                | (1L << (REAL_LITERAL - 935))
                                | (1L << (NULL_SPEC_LITERAL - 935))
                                | (1L << (BIT_STRING - 935))
                                | (1L << (STRING_CHARSET_NAME - 935))
                                | (1L << (ID - 935))
                                | (1L << (REVERSE_QUOTE_ID - 935))
                                | (1L << (LOCAL_ID - 935))))
                        != 0)) {
              {
                setState(261);
                functionArgs();
              }
            }

            setState(264);
            match(RR_BRACKET);
          }
          break;
        case 3:
          _localctx = new UdfFunctionCallContext(_localctx);
          enterOuterAlt(_localctx, 3);
          {
            setState(266);
            fullId();
            setState(267);
            match(LR_BRACKET);
            setState(269);
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
                                | (1L << (TABLES - 547))
                                | (1L << (ROUTINE - 547))
                                | (1L << (EXECUTE - 547))
                                | (1L << (FILE - 547))
                                | (1L << (PROCESS - 547))
                                | (1L << (RELOAD - 547))
                                | (1L << (SHUTDOWN - 547))
                                | (1L << (SUPER - 547))
                                | (1L << (PRIVILEGES - 547))
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
                || ((((_la - 611)) & ~0x3f) == 0
                    && ((1L << (_la - 611))
                            & ((1L << (ARCHIVE - 611))
                                | (1L << (BLACKHOLE - 611))
                                | (1L << (CSV - 611))
                                | (1L << (FEDERATED - 611))
                                | (1L << (INNODB - 611))
                                | (1L << (MEMORY - 611))
                                | (1L << (MRG_MYISAM - 611))
                                | (1L << (MYISAM - 611))
                                | (1L << (NDB - 611))
                                | (1L << (NDBCLUSTER - 611))
                                | (1L << (PERFOMANCE_SCHEMA - 611))
                                | (1L << (REPEATABLE - 611))
                                | (1L << (COMMITTED - 611))
                                | (1L << (UNCOMMITTED - 611))
                                | (1L << (SERIALIZABLE - 611))
                                | (1L << (GEOMETRYCOLLECTION - 611))
                                | (1L << (LINESTRING - 611))
                                | (1L << (MULTILINESTRING - 611))
                                | (1L << (MULTIPOINT - 611))
                                | (1L << (MULTIPOLYGON - 611))
                                | (1L << (POINT - 611))
                                | (1L << (POLYGON - 611))
                                | (1L << (ABS - 611))
                                | (1L << (ACOS - 611))
                                | (1L << (ADDDATE - 611))
                                | (1L << (ADDTIME - 611))
                                | (1L << (AES_DECRYPT - 611))
                                | (1L << (AES_ENCRYPT - 611))
                                | (1L << (AREA - 611))
                                | (1L << (ASBINARY - 611))
                                | (1L << (ASIN - 611))
                                | (1L << (ASTEXT - 611))
                                | (1L << (ASWKB - 611))
                                | (1L << (ASWKT - 611))
                                | (1L << (ASYMMETRIC_DECRYPT - 611))
                                | (1L << (ASYMMETRIC_DERIVE - 611))
                                | (1L << (ASYMMETRIC_ENCRYPT - 611))
                                | (1L << (ASYMMETRIC_SIGN - 611))
                                | (1L << (ASYMMETRIC_VERIFY - 611))
                                | (1L << (ATAN - 611))
                                | (1L << (ATAN2 - 611))
                                | (1L << (BENCHMARK - 611))
                                | (1L << (BIN - 611))
                                | (1L << (BIT_COUNT - 611))
                                | (1L << (BIT_LENGTH - 611))
                                | (1L << (BUFFER - 611))
                                | (1L << (CEIL - 611))
                                | (1L << (CEILING - 611))
                                | (1L << (CENTROID - 611))
                                | (1L << (CHARACTER_LENGTH - 611))
                                | (1L << (CHARSET - 611))
                                | (1L << (CHAR_LENGTH - 611))
                                | (1L << (COERCIBILITY - 611))
                                | (1L << (COLLATION - 611))
                                | (1L << (COMPRESS - 611))
                                | (1L << (CONCAT - 611))
                                | (1L << (CONCAT_WS - 611))
                                | (1L << (CONNECTION_ID - 611))
                                | (1L << (CONV - 611))
                                | (1L << (CONVERT_TZ - 611))
                                | (1L << (COS - 611))
                                | (1L << (COT - 611))
                                | (1L << (CRC32 - 611))
                                | (1L << (CREATE_ASYMMETRIC_PRIV_KEY - 611))))
                        != 0)
                || ((((_la - 675)) & ~0x3f) == 0
                    && ((1L << (_la - 675))
                            & ((1L << (CREATE_ASYMMETRIC_PUB_KEY - 675))
                                | (1L << (CREATE_DH_PARAMETERS - 675))
                                | (1L << (CREATE_DIGEST - 675))
                                | (1L << (CROSSES - 675))
                                | (1L << (DATEDIFF - 675))
                                | (1L << (DATE_FORMAT - 675))
                                | (1L << (DAYNAME - 675))
                                | (1L << (DAYOFMONTH - 675))
                                | (1L << (DAYOFWEEK - 675))
                                | (1L << (DAYOFYEAR - 675))
                                | (1L << (DECODE - 675))
                                | (1L << (DEGREES - 675))
                                | (1L << (DES_DECRYPT - 675))
                                | (1L << (DES_ENCRYPT - 675))
                                | (1L << (DIMENSION - 675))
                                | (1L << (DISJOINT - 675))
                                | (1L << (ELT - 675))
                                | (1L << (ENCODE - 675))
                                | (1L << (ENCRYPT - 675))
                                | (1L << (ENDPOINT - 675))
                                | (1L << (ENVELOPE - 675))
                                | (1L << (EQUALS - 675))
                                | (1L << (EXP - 675))
                                | (1L << (EXPORT_SET - 675))
                                | (1L << (EXTERIORRING - 675))
                                | (1L << (EXTRACTVALUE - 675))
                                | (1L << (FIELD - 675))
                                | (1L << (FIND_IN_SET - 675))
                                | (1L << (FLOOR - 675))
                                | (1L << (FORMAT - 675))
                                | (1L << (FOUND_ROWS - 675))
                                | (1L << (FROM_BASE64 - 675))
                                | (1L << (FROM_DAYS - 675))
                                | (1L << (FROM_UNIXTIME - 675))
                                | (1L << (GEOMCOLLFROMTEXT - 675))
                                | (1L << (GEOMCOLLFROMWKB - 675))
                                | (1L << (GEOMETRYCOLLECTIONFROMTEXT - 675))
                                | (1L << (GEOMETRYCOLLECTIONFROMWKB - 675))
                                | (1L << (GEOMETRYFROMTEXT - 675))
                                | (1L << (GEOMETRYFROMWKB - 675))
                                | (1L << (GEOMETRYN - 675))
                                | (1L << (GEOMETRYTYPE - 675))
                                | (1L << (GEOMFROMTEXT - 675))
                                | (1L << (GEOMFROMWKB - 675))
                                | (1L << (GET_FORMAT - 675))
                                | (1L << (GET_LOCK - 675))
                                | (1L << (GLENGTH - 675))
                                | (1L << (GREATEST - 675))
                                | (1L << (GTID_SUBSET - 675))
                                | (1L << (GTID_SUBTRACT - 675))
                                | (1L << (HEX - 675))
                                | (1L << (IFNULL - 675))
                                | (1L << (INET6_ATON - 675))
                                | (1L << (INET6_NTOA - 675))
                                | (1L << (INET_ATON - 675))
                                | (1L << (INET_NTOA - 675))
                                | (1L << (INSTR - 675))
                                | (1L << (INTERIORRINGN - 675))
                                | (1L << (INTERSECTS - 675))
                                | (1L << (ISCLOSED - 675))
                                | (1L << (ISEMPTY - 675))
                                | (1L << (ISNULL - 675))
                                | (1L << (ISSIMPLE - 675))
                                | (1L << (IS_FREE_LOCK - 675))))
                        != 0)
                || ((((_la - 739)) & ~0x3f) == 0
                    && ((1L << (_la - 739))
                            & ((1L << (IS_IPV4 - 739))
                                | (1L << (IS_IPV4_COMPAT - 739))
                                | (1L << (IS_IPV4_MAPPED - 739))
                                | (1L << (IS_IPV6 - 739))
                                | (1L << (IS_USED_LOCK - 739))
                                | (1L << (LAST_INSERT_ID - 739))
                                | (1L << (LCASE - 739))
                                | (1L << (LEAST - 739))
                                | (1L << (LENGTH - 739))
                                | (1L << (LINEFROMTEXT - 739))
                                | (1L << (LINEFROMWKB - 739))
                                | (1L << (LINESTRINGFROMTEXT - 739))
                                | (1L << (LINESTRINGFROMWKB - 739))
                                | (1L << (LN - 739))
                                | (1L << (LOAD_FILE - 739))
                                | (1L << (LOCATE - 739))
                                | (1L << (LOG - 739))
                                | (1L << (LOG10 - 739))
                                | (1L << (LOG2 - 739))
                                | (1L << (LOWER - 739))
                                | (1L << (LPAD - 739))
                                | (1L << (LTRIM - 739))
                                | (1L << (MAKEDATE - 739))
                                | (1L << (MAKETIME - 739))
                                | (1L << (MAKE_SET - 739))
                                | (1L << (MASTER_POS_WAIT - 739))
                                | (1L << (MBRCONTAINS - 739))
                                | (1L << (MBRDISJOINT - 739))
                                | (1L << (MBREQUAL - 739))
                                | (1L << (MBRINTERSECTS - 739))
                                | (1L << (MBROVERLAPS - 739))
                                | (1L << (MBRTOUCHES - 739))
                                | (1L << (MBRWITHIN - 739))
                                | (1L << (MD5 - 739))
                                | (1L << (MLINEFROMTEXT - 739))
                                | (1L << (MLINEFROMWKB - 739))
                                | (1L << (MONTHNAME - 739))
                                | (1L << (MPOINTFROMTEXT - 739))
                                | (1L << (MPOINTFROMWKB - 739))
                                | (1L << (MPOLYFROMTEXT - 739))
                                | (1L << (MPOLYFROMWKB - 739))
                                | (1L << (MULTILINESTRINGFROMTEXT - 739))
                                | (1L << (MULTILINESTRINGFROMWKB - 739))
                                | (1L << (MULTIPOINTFROMTEXT - 739))
                                | (1L << (MULTIPOINTFROMWKB - 739))
                                | (1L << (MULTIPOLYGONFROMTEXT - 739))
                                | (1L << (MULTIPOLYGONFROMWKB - 739))
                                | (1L << (NAME_CONST - 739))
                                | (1L << (NULLIF - 739))
                                | (1L << (NUMGEOMETRIES - 739))
                                | (1L << (NUMINTERIORRINGS - 739))
                                | (1L << (NUMPOINTS - 739))
                                | (1L << (OCT - 739))
                                | (1L << (OCTET_LENGTH - 739))
                                | (1L << (ORD - 739))
                                | (1L << (OVERLAPS - 739))
                                | (1L << (PERIOD_ADD - 739))
                                | (1L << (PERIOD_DIFF - 739))
                                | (1L << (PI - 739))
                                | (1L << (POINTFROMTEXT - 739))
                                | (1L << (POINTFROMWKB - 739))
                                | (1L << (POINTN - 739))
                                | (1L << (POLYFROMTEXT - 739))
                                | (1L << (POLYFROMWKB - 739))))
                        != 0)
                || ((((_la - 803)) & ~0x3f) == 0
                    && ((1L << (_la - 803))
                            & ((1L << (POLYGONFROMTEXT - 803))
                                | (1L << (POLYGONFROMWKB - 803))
                                | (1L << (POW - 803))
                                | (1L << (POWER - 803))
                                | (1L << (QUOTE - 803))
                                | (1L << (RADIANS - 803))
                                | (1L << (RAND - 803))
                                | (1L << (RANDOM_BYTES - 803))
                                | (1L << (RELEASE_LOCK - 803))
                                | (1L << (REVERSE - 803))
                                | (1L << (ROUND - 803))
                                | (1L << (ROW_COUNT - 803))
                                | (1L << (RPAD - 803))
                                | (1L << (RTRIM - 803))
                                | (1L << (SEC_TO_TIME - 803))
                                | (1L << (SESSION_USER - 803))
                                | (1L << (SHA - 803))
                                | (1L << (SHA1 - 803))
                                | (1L << (SHA2 - 803))
                                | (1L << (SIGN - 803))
                                | (1L << (SIN - 803))
                                | (1L << (SLEEP - 803))
                                | (1L << (SOUNDEX - 803))
                                | (1L << (SQL_THREAD_WAIT_AFTER_GTIDS - 803))
                                | (1L << (SQRT - 803))
                                | (1L << (SRID - 803))
                                | (1L << (STARTPOINT - 803))
                                | (1L << (STRCMP - 803))
                                | (1L << (STR_TO_DATE - 803))
                                | (1L << (ST_AREA - 803))
                                | (1L << (ST_ASBINARY - 803))
                                | (1L << (ST_ASTEXT - 803))
                                | (1L << (ST_ASWKB - 803))
                                | (1L << (ST_ASWKT - 803))
                                | (1L << (ST_BUFFER - 803))
                                | (1L << (ST_CENTROID - 803))
                                | (1L << (ST_CONTAINS - 803))
                                | (1L << (ST_CROSSES - 803))
                                | (1L << (ST_DIFFERENCE - 803))
                                | (1L << (ST_DIMENSION - 803))
                                | (1L << (ST_DISJOINT - 803))
                                | (1L << (ST_DISTANCE - 803))
                                | (1L << (ST_ENDPOINT - 803))
                                | (1L << (ST_ENVELOPE - 803))
                                | (1L << (ST_EQUALS - 803))
                                | (1L << (ST_EXTERIORRING - 803))
                                | (1L << (ST_GEOMCOLLFROMTEXT - 803))
                                | (1L << (ST_GEOMCOLLFROMTXT - 803))
                                | (1L << (ST_GEOMCOLLFROMWKB - 803))
                                | (1L << (ST_GEOMETRYCOLLECTIONFROMTEXT - 803))
                                | (1L << (ST_GEOMETRYCOLLECTIONFROMWKB - 803))
                                | (1L << (ST_GEOMETRYFROMTEXT - 803))
                                | (1L << (ST_GEOMETRYFROMWKB - 803))
                                | (1L << (ST_GEOMETRYN - 803))
                                | (1L << (ST_GEOMETRYTYPE - 803))
                                | (1L << (ST_GEOMFROMTEXT - 803))
                                | (1L << (ST_GEOMFROMWKB - 803))
                                | (1L << (ST_INTERIORRINGN - 803))
                                | (1L << (ST_INTERSECTION - 803))
                                | (1L << (ST_INTERSECTS - 803))
                                | (1L << (ST_ISCLOSED - 803))
                                | (1L << (ST_ISEMPTY - 803))
                                | (1L << (ST_ISSIMPLE - 803))
                                | (1L << (ST_LINEFROMTEXT - 803))))
                        != 0)
                || ((((_la - 867)) & ~0x3f) == 0
                    && ((1L << (_la - 867))
                            & ((1L << (ST_LINEFROMWKB - 867))
                                | (1L << (ST_LINESTRINGFROMTEXT - 867))
                                | (1L << (ST_LINESTRINGFROMWKB - 867))
                                | (1L << (ST_NUMGEOMETRIES - 867))
                                | (1L << (ST_NUMINTERIORRING - 867))
                                | (1L << (ST_NUMINTERIORRINGS - 867))
                                | (1L << (ST_NUMPOINTS - 867))
                                | (1L << (ST_OVERLAPS - 867))
                                | (1L << (ST_POINTFROMTEXT - 867))
                                | (1L << (ST_POINTFROMWKB - 867))
                                | (1L << (ST_POINTN - 867))
                                | (1L << (ST_POLYFROMTEXT - 867))
                                | (1L << (ST_POLYFROMWKB - 867))
                                | (1L << (ST_POLYGONFROMTEXT - 867))
                                | (1L << (ST_POLYGONFROMWKB - 867))
                                | (1L << (ST_SRID - 867))
                                | (1L << (ST_STARTPOINT - 867))
                                | (1L << (ST_SYMDIFFERENCE - 867))
                                | (1L << (ST_TOUCHES - 867))
                                | (1L << (ST_UNION - 867))
                                | (1L << (ST_WITHIN - 867))
                                | (1L << (ST_X - 867))
                                | (1L << (ST_Y - 867))
                                | (1L << (SUBDATE - 867))
                                | (1L << (SUBSTRING_INDEX - 867))
                                | (1L << (SUBTIME - 867))
                                | (1L << (SYSTEM_USER - 867))
                                | (1L << (TAN - 867))
                                | (1L << (TIMEDIFF - 867))
                                | (1L << (TIMESTAMPADD - 867))
                                | (1L << (TIMESTAMPDIFF - 867))
                                | (1L << (TIME_FORMAT - 867))
                                | (1L << (TIME_TO_SEC - 867))
                                | (1L << (TOUCHES - 867))
                                | (1L << (TO_BASE64 - 867))
                                | (1L << (TO_DAYS - 867))
                                | (1L << (TO_SECONDS - 867))
                                | (1L << (UCASE - 867))
                                | (1L << (UNCOMPRESS - 867))
                                | (1L << (UNCOMPRESSED_LENGTH - 867))
                                | (1L << (UNHEX - 867))
                                | (1L << (UNIX_TIMESTAMP - 867))
                                | (1L << (UPDATEXML - 867))
                                | (1L << (UPPER - 867))
                                | (1L << (UUID - 867))
                                | (1L << (UUID_SHORT - 867))
                                | (1L << (VALIDATE_PASSWORD_STRENGTH - 867))
                                | (1L << (VERSION - 867))
                                | (1L << (WAIT_UNTIL_SQL_THREAD_AFTER_GTIDS - 867))
                                | (1L << (WEEKDAY - 867))
                                | (1L << (WEEKOFYEAR - 867))
                                | (1L << (WEIGHT_STRING - 867))
                                | (1L << (WITHIN - 867))
                                | (1L << (YEARWEEK - 867))
                                | (1L << (Y_FUNCTION - 867))
                                | (1L << (X_FUNCTION - 867))))
                        != 0)
                || ((((_la - 935)) & ~0x3f) == 0
                    && ((1L << (_la - 935))
                            & ((1L << (PLUS - 935))
                                | (1L << (MINUS - 935))
                                | (1L << (EXCLAMATION_SYMBOL - 935))
                                | (1L << (BIT_NOT_OP - 935))
                                | (1L << (LR_BRACKET - 935))
                                | (1L << (ZERO_DECIMAL - 935))
                                | (1L << (ONE_DECIMAL - 935))
                                | (1L << (TWO_DECIMAL - 935))
                                | (1L << (CHARSET_REVERSE_QOUTE_STRING - 935))
                                | (1L << (START_NATIONAL_STRING_LITERAL - 935))
                                | (1L << (STRING_LITERAL - 935))
                                | (1L << (DECIMAL_LITERAL - 935))
                                | (1L << (HEXADECIMAL_LITERAL - 935))
                                | (1L << (REAL_LITERAL - 935))
                                | (1L << (NULL_SPEC_LITERAL - 935))
                                | (1L << (BIT_STRING - 935))
                                | (1L << (STRING_CHARSET_NAME - 935))
                                | (1L << (ID - 935))
                                | (1L << (REVERSE_QUOTE_ID - 935))
                                | (1L << (LOCAL_ID - 935))))
                        != 0)) {
              {
                setState(268);
                functionArgs();
              }
            }

            setState(271);
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
    enterRule(_localctx, 42, RULE_specificFunction);
    int _la;
    try {
      setState(432);
      _errHandler.sync(this);
      switch (getInterpreter().adaptivePredict(_input, 53, _ctx)) {
        case 1:
          _localctx = new SimpleFunctionCallContext(_localctx);
          enterOuterAlt(_localctx, 1);
          {
            setState(275);
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
            setState(276);
            match(CONVERT);
            setState(277);
            match(LR_BRACKET);
            setState(278);
            expression(0);
            setState(279);
            ((DataTypeFunctionCallContext) _localctx).separator = match(COMMA);
            setState(280);
            convertedDataType();
            setState(281);
            match(RR_BRACKET);
          }
          break;
        case 3:
          _localctx = new DataTypeFunctionCallContext(_localctx);
          enterOuterAlt(_localctx, 3);
          {
            setState(283);
            match(CONVERT);
            setState(284);
            match(LR_BRACKET);
            setState(285);
            expression(0);
            setState(286);
            match(USING);
            setState(287);
            charsetName();
            setState(288);
            match(RR_BRACKET);
          }
          break;
        case 4:
          _localctx = new DataTypeFunctionCallContext(_localctx);
          enterOuterAlt(_localctx, 4);
          {
            setState(290);
            match(CAST);
            setState(291);
            match(LR_BRACKET);
            setState(292);
            expression(0);
            setState(293);
            match(AS);
            setState(294);
            convertedDataType();
            setState(295);
            match(RR_BRACKET);
          }
          break;
        case 5:
          _localctx = new ValuesFunctionCallContext(_localctx);
          enterOuterAlt(_localctx, 5);
          {
            setState(297);
            match(VALUES);
            setState(298);
            match(LR_BRACKET);
            setState(299);
            fullColumnName();
            setState(300);
            match(RR_BRACKET);
          }
          break;
        case 6:
          _localctx = new CaseFunctionCallContext(_localctx);
          enterOuterAlt(_localctx, 6);
          {
            setState(302);
            match(CASE);
            setState(303);
            expression(0);
            setState(305);
            _errHandler.sync(this);
            _la = _input.LA(1);
            do {
              {
                {
                  setState(304);
                  caseFuncAlternative();
                }
              }
              setState(307);
              _errHandler.sync(this);
              _la = _input.LA(1);
            } while (_la == WHEN);
            setState(311);
            _errHandler.sync(this);
            _la = _input.LA(1);
            if (_la == ELSE) {
              {
                setState(309);
                match(ELSE);
                setState(310);
                ((CaseFunctionCallContext) _localctx).elseArg = functionArg();
              }
            }

            setState(313);
            match(END);
          }
          break;
        case 7:
          _localctx = new CaseFunctionCallContext(_localctx);
          enterOuterAlt(_localctx, 7);
          {
            setState(315);
            match(CASE);
            setState(317);
            _errHandler.sync(this);
            _la = _input.LA(1);
            do {
              {
                {
                  setState(316);
                  caseFuncAlternative();
                }
              }
              setState(319);
              _errHandler.sync(this);
              _la = _input.LA(1);
            } while (_la == WHEN);
            setState(323);
            _errHandler.sync(this);
            _la = _input.LA(1);
            if (_la == ELSE) {
              {
                setState(321);
                match(ELSE);
                setState(322);
                ((CaseFunctionCallContext) _localctx).elseArg = functionArg();
              }
            }

            setState(325);
            match(END);
          }
          break;
        case 8:
          _localctx = new CharFunctionCallContext(_localctx);
          enterOuterAlt(_localctx, 8);
          {
            setState(327);
            match(CHAR);
            setState(328);
            match(LR_BRACKET);
            setState(329);
            functionArgs();
            setState(332);
            _errHandler.sync(this);
            _la = _input.LA(1);
            if (_la == USING) {
              {
                setState(330);
                match(USING);
                setState(331);
                charsetName();
              }
            }

            setState(334);
            match(RR_BRACKET);
          }
          break;
        case 9:
          _localctx = new PositionFunctionCallContext(_localctx);
          enterOuterAlt(_localctx, 9);
          {
            setState(336);
            match(POSITION);
            setState(337);
            match(LR_BRACKET);
            setState(340);
            _errHandler.sync(this);
            switch (getInterpreter().adaptivePredict(_input, 39, _ctx)) {
              case 1:
                {
                  setState(338);
                  ((PositionFunctionCallContext) _localctx).positionString = stringLiteral();
                }
                break;
              case 2:
                {
                  setState(339);
                  ((PositionFunctionCallContext) _localctx).positionExpression = expression(0);
                }
                break;
            }
            setState(342);
            match(IN);
            setState(345);
            _errHandler.sync(this);
            switch (getInterpreter().adaptivePredict(_input, 40, _ctx)) {
              case 1:
                {
                  setState(343);
                  ((PositionFunctionCallContext) _localctx).inString = stringLiteral();
                }
                break;
              case 2:
                {
                  setState(344);
                  ((PositionFunctionCallContext) _localctx).inExpression = expression(0);
                }
                break;
            }
            setState(347);
            match(RR_BRACKET);
          }
          break;
        case 10:
          _localctx = new SubstrFunctionCallContext(_localctx);
          enterOuterAlt(_localctx, 10);
          {
            setState(349);
            _la = _input.LA(1);
            if (!(_la == SUBSTR || _la == SUBSTRING)) {
              _errHandler.recoverInline(this);
            } else {
              if (_input.LA(1) == Token.EOF) matchedEOF = true;
              _errHandler.reportMatch(this);
              consume();
            }
            setState(350);
            match(LR_BRACKET);
            setState(353);
            _errHandler.sync(this);
            switch (getInterpreter().adaptivePredict(_input, 41, _ctx)) {
              case 1:
                {
                  setState(351);
                  ((SubstrFunctionCallContext) _localctx).sourceString = stringLiteral();
                }
                break;
              case 2:
                {
                  setState(352);
                  ((SubstrFunctionCallContext) _localctx).sourceExpression = expression(0);
                }
                break;
            }
            setState(355);
            match(FROM);
            setState(358);
            _errHandler.sync(this);
            switch (getInterpreter().adaptivePredict(_input, 42, _ctx)) {
              case 1:
                {
                  setState(356);
                  ((SubstrFunctionCallContext) _localctx).fromDecimal = decimalLiteral();
                }
                break;
              case 2:
                {
                  setState(357);
                  ((SubstrFunctionCallContext) _localctx).fromExpression = expression(0);
                }
                break;
            }
            setState(365);
            _errHandler.sync(this);
            _la = _input.LA(1);
            if (_la == FOR) {
              {
                setState(360);
                match(FOR);
                setState(363);
                _errHandler.sync(this);
                switch (getInterpreter().adaptivePredict(_input, 43, _ctx)) {
                  case 1:
                    {
                      setState(361);
                      ((SubstrFunctionCallContext) _localctx).forDecimal = decimalLiteral();
                    }
                    break;
                  case 2:
                    {
                      setState(362);
                      ((SubstrFunctionCallContext) _localctx).forExpression = expression(0);
                    }
                    break;
                }
              }
            }

            setState(367);
            match(RR_BRACKET);
          }
          break;
        case 11:
          _localctx = new TrimFunctionCallContext(_localctx);
          enterOuterAlt(_localctx, 11);
          {
            setState(369);
            match(TRIM);
            setState(370);
            match(LR_BRACKET);
            setState(371);
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
            setState(374);
            _errHandler.sync(this);
            switch (getInterpreter().adaptivePredict(_input, 45, _ctx)) {
              case 1:
                {
                  setState(372);
                  ((TrimFunctionCallContext) _localctx).sourceString = stringLiteral();
                }
                break;
              case 2:
                {
                  setState(373);
                  ((TrimFunctionCallContext) _localctx).sourceExpression = expression(0);
                }
                break;
            }
            setState(376);
            match(FROM);
            setState(379);
            _errHandler.sync(this);
            switch (getInterpreter().adaptivePredict(_input, 46, _ctx)) {
              case 1:
                {
                  setState(377);
                  ((TrimFunctionCallContext) _localctx).fromString = stringLiteral();
                }
                break;
              case 2:
                {
                  setState(378);
                  ((TrimFunctionCallContext) _localctx).fromExpression = expression(0);
                }
                break;
            }
            setState(381);
            match(RR_BRACKET);
          }
          break;
        case 12:
          _localctx = new TrimFunctionCallContext(_localctx);
          enterOuterAlt(_localctx, 12);
          {
            setState(383);
            match(TRIM);
            setState(384);
            match(LR_BRACKET);
            setState(387);
            _errHandler.sync(this);
            switch (getInterpreter().adaptivePredict(_input, 47, _ctx)) {
              case 1:
                {
                  setState(385);
                  ((TrimFunctionCallContext) _localctx).sourceString = stringLiteral();
                }
                break;
              case 2:
                {
                  setState(386);
                  ((TrimFunctionCallContext) _localctx).sourceExpression = expression(0);
                }
                break;
            }
            setState(389);
            match(FROM);
            setState(392);
            _errHandler.sync(this);
            switch (getInterpreter().adaptivePredict(_input, 48, _ctx)) {
              case 1:
                {
                  setState(390);
                  ((TrimFunctionCallContext) _localctx).fromString = stringLiteral();
                }
                break;
              case 2:
                {
                  setState(391);
                  ((TrimFunctionCallContext) _localctx).fromExpression = expression(0);
                }
                break;
            }
            setState(394);
            match(RR_BRACKET);
          }
          break;
        case 13:
          _localctx = new WeightFunctionCallContext(_localctx);
          enterOuterAlt(_localctx, 13);
          {
            setState(396);
            match(WEIGHT_STRING);
            setState(397);
            match(LR_BRACKET);
            setState(400);
            _errHandler.sync(this);
            switch (getInterpreter().adaptivePredict(_input, 49, _ctx)) {
              case 1:
                {
                  setState(398);
                  stringLiteral();
                }
                break;
              case 2:
                {
                  setState(399);
                  expression(0);
                }
                break;
            }
            setState(408);
            _errHandler.sync(this);
            _la = _input.LA(1);
            if (_la == AS) {
              {
                setState(402);
                match(AS);
                setState(403);
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
                setState(404);
                match(LR_BRACKET);
                setState(405);
                decimalLiteral();
                setState(406);
                match(RR_BRACKET);
              }
            }

            setState(411);
            _errHandler.sync(this);
            _la = _input.LA(1);
            if (_la == LEVEL) {
              {
                setState(410);
                levelsInWeightString();
              }
            }

            setState(413);
            match(RR_BRACKET);
          }
          break;
        case 14:
          _localctx = new ExtractFunctionCallContext(_localctx);
          enterOuterAlt(_localctx, 14);
          {
            setState(415);
            match(EXTRACT);
            setState(416);
            match(LR_BRACKET);
            setState(417);
            intervalType();
            setState(418);
            match(FROM);
            setState(421);
            _errHandler.sync(this);
            switch (getInterpreter().adaptivePredict(_input, 52, _ctx)) {
              case 1:
                {
                  setState(419);
                  ((ExtractFunctionCallContext) _localctx).sourceString = stringLiteral();
                }
                break;
              case 2:
                {
                  setState(420);
                  ((ExtractFunctionCallContext) _localctx).sourceExpression = expression(0);
                }
                break;
            }
            setState(423);
            match(RR_BRACKET);
          }
          break;
        case 15:
          _localctx = new GetFormatFunctionCallContext(_localctx);
          enterOuterAlt(_localctx, 15);
          {
            setState(425);
            match(GET_FORMAT);
            setState(426);
            match(LR_BRACKET);
            setState(427);
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
            setState(428);
            match(COMMA);
            setState(429);
            stringLiteral();
            setState(430);
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
    enterRule(_localctx, 44, RULE_caseFuncAlternative);
    try {
      enterOuterAlt(_localctx, 1);
      {
        setState(434);
        match(WHEN);
        setState(435);
        ((CaseFuncAlternativeContext) _localctx).condition = functionArg();
        setState(436);
        match(THEN);
        setState(437);
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
    enterRule(_localctx, 46, RULE_levelsInWeightString);
    int _la;
    try {
      setState(453);
      _errHandler.sync(this);
      switch (getInterpreter().adaptivePredict(_input, 55, _ctx)) {
        case 1:
          _localctx = new LevelWeightListContext(_localctx);
          enterOuterAlt(_localctx, 1);
          {
            setState(439);
            match(LEVEL);
            setState(440);
            levelInWeightListElement();
            setState(445);
            _errHandler.sync(this);
            _la = _input.LA(1);
            while (_la == COMMA) {
              {
                {
                  setState(441);
                  match(COMMA);
                  setState(442);
                  levelInWeightListElement();
                }
              }
              setState(447);
              _errHandler.sync(this);
              _la = _input.LA(1);
            }
          }
          break;
        case 2:
          _localctx = new LevelWeightRangeContext(_localctx);
          enterOuterAlt(_localctx, 2);
          {
            setState(448);
            match(LEVEL);
            setState(449);
            ((LevelWeightRangeContext) _localctx).firstLevel = decimalLiteral();
            setState(450);
            match(MINUS);
            setState(451);
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
    enterRule(_localctx, 48, RULE_levelInWeightListElement);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
        setState(455);
        decimalLiteral();
        setState(457);
        _errHandler.sync(this);
        _la = _input.LA(1);
        if (_la == ASC || _la == DESC || _la == REVERSE) {
          {
            setState(456);
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
    enterRule(_localctx, 50, RULE_scalarFunctionName);
    try {
      setState(482);
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
            setState(459);
            functionNameBase();
          }
          break;
        case ASCII:
          enterOuterAlt(_localctx, 2);
          {
            setState(460);
            match(ASCII);
          }
          break;
        case CURDATE:
          enterOuterAlt(_localctx, 3);
          {
            setState(461);
            match(CURDATE);
          }
          break;
        case CURRENT_DATE:
          enterOuterAlt(_localctx, 4);
          {
            setState(462);
            match(CURRENT_DATE);
          }
          break;
        case CURRENT_TIME:
          enterOuterAlt(_localctx, 5);
          {
            setState(463);
            match(CURRENT_TIME);
          }
          break;
        case CURRENT_TIMESTAMP:
          enterOuterAlt(_localctx, 6);
          {
            setState(464);
            match(CURRENT_TIMESTAMP);
          }
          break;
        case CURTIME:
          enterOuterAlt(_localctx, 7);
          {
            setState(465);
            match(CURTIME);
          }
          break;
        case DATE_ADD:
          enterOuterAlt(_localctx, 8);
          {
            setState(466);
            match(DATE_ADD);
          }
          break;
        case DATE_SUB:
          enterOuterAlt(_localctx, 9);
          {
            setState(467);
            match(DATE_SUB);
          }
          break;
        case IF:
          enterOuterAlt(_localctx, 10);
          {
            setState(468);
            match(IF);
          }
          break;
        case INSERT:
          enterOuterAlt(_localctx, 11);
          {
            setState(469);
            match(INSERT);
          }
          break;
        case LOCALTIME:
          enterOuterAlt(_localctx, 12);
          {
            setState(470);
            match(LOCALTIME);
          }
          break;
        case LOCALTIMESTAMP:
          enterOuterAlt(_localctx, 13);
          {
            setState(471);
            match(LOCALTIMESTAMP);
          }
          break;
        case MID:
          enterOuterAlt(_localctx, 14);
          {
            setState(472);
            match(MID);
          }
          break;
        case NOW:
          enterOuterAlt(_localctx, 15);
          {
            setState(473);
            match(NOW);
          }
          break;
        case REPLACE:
          enterOuterAlt(_localctx, 16);
          {
            setState(474);
            match(REPLACE);
          }
          break;
        case SUBSTR:
          enterOuterAlt(_localctx, 17);
          {
            setState(475);
            match(SUBSTR);
          }
          break;
        case SUBSTRING:
          enterOuterAlt(_localctx, 18);
          {
            setState(476);
            match(SUBSTRING);
          }
          break;
        case SYSDATE:
          enterOuterAlt(_localctx, 19);
          {
            setState(477);
            match(SYSDATE);
          }
          break;
        case TRIM:
          enterOuterAlt(_localctx, 20);
          {
            setState(478);
            match(TRIM);
          }
          break;
        case UTC_DATE:
          enterOuterAlt(_localctx, 21);
          {
            setState(479);
            match(UTC_DATE);
          }
          break;
        case UTC_TIME:
          enterOuterAlt(_localctx, 22);
          {
            setState(480);
            match(UTC_TIME);
          }
          break;
        case UTC_TIMESTAMP:
          enterOuterAlt(_localctx, 23);
          {
            setState(481);
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
    enterRule(_localctx, 52, RULE_functionArgs);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
        setState(488);
        _errHandler.sync(this);
        switch (getInterpreter().adaptivePredict(_input, 58, _ctx)) {
          case 1:
            {
              setState(484);
              constant();
            }
            break;
          case 2:
            {
              setState(485);
              fullColumnName();
            }
            break;
          case 3:
            {
              setState(486);
              functionCall();
            }
            break;
          case 4:
            {
              setState(487);
              expression(0);
            }
            break;
        }
        setState(499);
        _errHandler.sync(this);
        _la = _input.LA(1);
        while (_la == COMMA) {
          {
            {
              setState(490);
              match(COMMA);
              setState(495);
              _errHandler.sync(this);
              switch (getInterpreter().adaptivePredict(_input, 59, _ctx)) {
                case 1:
                  {
                    setState(491);
                    constant();
                  }
                  break;
                case 2:
                  {
                    setState(492);
                    fullColumnName();
                  }
                  break;
                case 3:
                  {
                    setState(493);
                    functionCall();
                  }
                  break;
                case 4:
                  {
                    setState(494);
                    expression(0);
                  }
                  break;
              }
            }
          }
          setState(501);
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
    enterRule(_localctx, 54, RULE_functionArg);
    try {
      setState(506);
      _errHandler.sync(this);
      switch (getInterpreter().adaptivePredict(_input, 61, _ctx)) {
        case 1:
          enterOuterAlt(_localctx, 1);
          {
            setState(502);
            constant();
          }
          break;
        case 2:
          enterOuterAlt(_localctx, 2);
          {
            setState(503);
            fullColumnName();
          }
          break;
        case 3:
          enterOuterAlt(_localctx, 3);
          {
            setState(504);
            functionCall();
          }
          break;
        case 4:
          enterOuterAlt(_localctx, 4);
          {
            setState(505);
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
    int _startState = 56;
    enterRecursionRule(_localctx, 56, RULE_expression, _p);
    int _la;
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      {
        setState(519);
        _errHandler.sync(this);
        switch (getInterpreter().adaptivePredict(_input, 63, _ctx)) {
          case 1:
            {
              _localctx = new NotExpressionContext(_localctx);
              _ctx = _localctx;
              _prevctx = _localctx;

              setState(509);
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
              setState(510);
              expression(4);
            }
            break;
          case 2:
            {
              _localctx = new IsExpressionContext(_localctx);
              _ctx = _localctx;
              _prevctx = _localctx;
              setState(511);
              predicate(0);
              setState(512);
              match(IS);
              setState(514);
              _errHandler.sync(this);
              _la = _input.LA(1);
              if (_la == NOT) {
                {
                  setState(513);
                  match(NOT);
                }
              }

              setState(516);
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
              setState(518);
              predicate(0);
            }
            break;
        }
        _ctx.stop = _input.LT(-1);
        setState(527);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input, 64, _ctx);
        while (_alt != 2 && _alt != org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER) {
          if (_alt == 1) {
            if (_parseListeners != null) triggerExitRuleEvent();
            _prevctx = _localctx;
            {
              {
                _localctx =
                    new LogicalExpressionContext(new ExpressionContext(_parentctx, _parentState));
                pushNewRecursionContext(_localctx, _startState, RULE_expression);
                setState(521);
                if (!(precpred(_ctx, 3)))
                  throw new FailedPredicateException(this, "precpred(_ctx, 3)");
                setState(522);
                logicalOperator();
                setState(523);
                expression(4);
              }
            }
          }
          setState(529);
          _errHandler.sync(this);
          _alt = getInterpreter().adaptivePredict(_input, 64, _ctx);
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
    int _startState = 58;
    enterRecursionRule(_localctx, 58, RULE_predicate, _p);
    int _la;
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      {
        {
          _localctx = new ExpressionAtomPredicateContext(_localctx);
          _ctx = _localctx;
          _prevctx = _localctx;

          setState(533);
          _errHandler.sync(this);
          _la = _input.LA(1);
          if (_la == LOCAL_ID) {
            {
              setState(531);
              match(LOCAL_ID);
              setState(532);
              match(VAR_ASSIGN);
            }
          }

          setState(535);
          expressionAtom(0);
        }
        _ctx.stop = _input.LT(-1);
        setState(584);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input, 72, _ctx);
        while (_alt != 2 && _alt != org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER) {
          if (_alt == 1) {
            if (_parseListeners != null) triggerExitRuleEvent();
            _prevctx = _localctx;
            {
              setState(582);
              _errHandler.sync(this);
              switch (getInterpreter().adaptivePredict(_input, 71, _ctx)) {
                case 1:
                  {
                    _localctx =
                        new BinaryComparisonPredicateContext(
                            new PredicateContext(_parentctx, _parentState));
                    ((BinaryComparisonPredicateContext) _localctx).left = _prevctx;
                    pushNewRecursionContext(_localctx, _startState, RULE_predicate);
                    setState(537);
                    if (!(precpred(_ctx, 6)))
                      throw new FailedPredicateException(this, "precpred(_ctx, 6)");
                    setState(538);
                    comparisonOperator();
                    setState(539);
                    ((BinaryComparisonPredicateContext) _localctx).right = predicate(7);
                  }
                  break;
                case 2:
                  {
                    _localctx =
                        new BetweenPredicateContext(new PredicateContext(_parentctx, _parentState));
                    pushNewRecursionContext(_localctx, _startState, RULE_predicate);
                    setState(541);
                    if (!(precpred(_ctx, 5)))
                      throw new FailedPredicateException(this, "precpred(_ctx, 5)");
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
                    match(BETWEEN);
                    setState(546);
                    predicate(0);
                    setState(547);
                    match(AND);
                    setState(548);
                    predicate(6);
                  }
                  break;
                case 3:
                  {
                    _localctx =
                        new SoundsLikePredicateContext(
                            new PredicateContext(_parentctx, _parentState));
                    pushNewRecursionContext(_localctx, _startState, RULE_predicate);
                    setState(550);
                    if (!(precpred(_ctx, 4)))
                      throw new FailedPredicateException(this, "precpred(_ctx, 4)");
                    setState(551);
                    match(SOUNDS);
                    setState(552);
                    match(LIKE);
                    setState(553);
                    predicate(5);
                  }
                  break;
                case 4:
                  {
                    _localctx =
                        new RegexpPredicateContext(new PredicateContext(_parentctx, _parentState));
                    pushNewRecursionContext(_localctx, _startState, RULE_predicate);
                    setState(554);
                    if (!(precpred(_ctx, 2)))
                      throw new FailedPredicateException(this, "precpred(_ctx, 2)");
                    setState(556);
                    _errHandler.sync(this);
                    _la = _input.LA(1);
                    if (_la == NOT) {
                      {
                        setState(555);
                        match(NOT);
                      }
                    }

                    setState(558);
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
                    setState(559);
                    predicate(3);
                  }
                  break;
                case 5:
                  {
                    _localctx =
                        new InPredicateContext(new PredicateContext(_parentctx, _parentState));
                    pushNewRecursionContext(_localctx, _startState, RULE_predicate);
                    setState(560);
                    if (!(precpred(_ctx, 8)))
                      throw new FailedPredicateException(this, "precpred(_ctx, 8)");
                    setState(562);
                    _errHandler.sync(this);
                    _la = _input.LA(1);
                    if (_la == NOT) {
                      {
                        setState(561);
                        match(NOT);
                      }
                    }

                    setState(564);
                    match(IN);
                    setState(565);
                    match(LR_BRACKET);
                    setState(566);
                    expressions();
                    setState(567);
                    match(RR_BRACKET);
                  }
                  break;
                case 6:
                  {
                    _localctx =
                        new IsNullPredicateContext(new PredicateContext(_parentctx, _parentState));
                    pushNewRecursionContext(_localctx, _startState, RULE_predicate);
                    setState(569);
                    if (!(precpred(_ctx, 7)))
                      throw new FailedPredicateException(this, "precpred(_ctx, 7)");
                    setState(570);
                    match(IS);
                    setState(571);
                    nullNotnull();
                  }
                  break;
                case 7:
                  {
                    _localctx =
                        new LikePredicateContext(new PredicateContext(_parentctx, _parentState));
                    pushNewRecursionContext(_localctx, _startState, RULE_predicate);
                    setState(572);
                    if (!(precpred(_ctx, 3)))
                      throw new FailedPredicateException(this, "precpred(_ctx, 3)");
                    setState(574);
                    _errHandler.sync(this);
                    _la = _input.LA(1);
                    if (_la == NOT) {
                      {
                        setState(573);
                        match(NOT);
                      }
                    }

                    setState(576);
                    match(LIKE);
                    setState(577);
                    predicate(0);
                    setState(580);
                    _errHandler.sync(this);
                    switch (getInterpreter().adaptivePredict(_input, 70, _ctx)) {
                      case 1:
                        {
                          setState(578);
                          match(ESCAPE);
                          setState(579);
                          match(STRING_LITERAL);
                        }
                        break;
                    }
                  }
                  break;
              }
            }
          }
          setState(586);
          _errHandler.sync(this);
          _alt = getInterpreter().adaptivePredict(_input, 72, _ctx);
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
    int _startState = 60;
    enterRecursionRule(_localctx, 60, RULE_expressionAtom, _p);
    int _la;
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      {
        setState(622);
        _errHandler.sync(this);
        switch (getInterpreter().adaptivePredict(_input, 75, _ctx)) {
          case 1:
            {
              _localctx = new ConstantExpressionAtomContext(_localctx);
              _ctx = _localctx;
              _prevctx = _localctx;

              setState(588);
              constant();
            }
            break;
          case 2:
            {
              _localctx = new FullColumnNameExpressionAtomContext(_localctx);
              _ctx = _localctx;
              _prevctx = _localctx;
              setState(589);
              fullColumnName();
            }
            break;
          case 3:
            {
              _localctx = new FunctionCallExpressionAtomContext(_localctx);
              _ctx = _localctx;
              _prevctx = _localctx;
              setState(590);
              functionCall();
            }
            break;
          case 4:
            {
              _localctx = new UnaryExpressionAtomContext(_localctx);
              _ctx = _localctx;
              _prevctx = _localctx;
              setState(591);
              unaryOperator();
              setState(592);
              expressionAtom(7);
            }
            break;
          case 5:
            {
              _localctx = new BinaryExpressionAtomContext(_localctx);
              _ctx = _localctx;
              _prevctx = _localctx;
              setState(594);
              match(BINARY);
              setState(595);
              expressionAtom(6);
            }
            break;
          case 6:
            {
              _localctx = new NestedExpressionAtomContext(_localctx);
              _ctx = _localctx;
              _prevctx = _localctx;
              setState(596);
              match(LR_BRACKET);
              setState(597);
              expression(0);
              setState(602);
              _errHandler.sync(this);
              _la = _input.LA(1);
              while (_la == COMMA) {
                {
                  {
                    setState(598);
                    match(COMMA);
                    setState(599);
                    expression(0);
                  }
                }
                setState(604);
                _errHandler.sync(this);
                _la = _input.LA(1);
              }
              setState(605);
              match(RR_BRACKET);
            }
            break;
          case 7:
            {
              _localctx = new NestedRowExpressionAtomContext(_localctx);
              _ctx = _localctx;
              _prevctx = _localctx;
              setState(607);
              match(ROW);
              setState(608);
              match(LR_BRACKET);
              setState(609);
              expression(0);
              setState(612);
              _errHandler.sync(this);
              _la = _input.LA(1);
              do {
                {
                  {
                    setState(610);
                    match(COMMA);
                    setState(611);
                    expression(0);
                  }
                }
                setState(614);
                _errHandler.sync(this);
                _la = _input.LA(1);
              } while (_la == COMMA);
              setState(616);
              match(RR_BRACKET);
            }
            break;
          case 8:
            {
              _localctx = new IntervalExpressionAtomContext(_localctx);
              _ctx = _localctx;
              _prevctx = _localctx;
              setState(618);
              match(INTERVAL);
              setState(619);
              expression(0);
              setState(620);
              intervalType();
            }
            break;
        }
        _ctx.stop = _input.LT(-1);
        setState(637);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input, 77, _ctx);
        while (_alt != 2 && _alt != org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER) {
          if (_alt == 1) {
            if (_parseListeners != null) triggerExitRuleEvent();
            _prevctx = _localctx;
            {
              setState(635);
              _errHandler.sync(this);
              switch (getInterpreter().adaptivePredict(_input, 76, _ctx)) {
                case 1:
                  {
                    _localctx =
                        new BitExpressionAtomContext(
                            new ExpressionAtomContext(_parentctx, _parentState));
                    ((BitExpressionAtomContext) _localctx).left = _prevctx;
                    pushNewRecursionContext(_localctx, _startState, RULE_expressionAtom);
                    setState(624);
                    if (!(precpred(_ctx, 2)))
                      throw new FailedPredicateException(this, "precpred(_ctx, 2)");
                    setState(625);
                    bitOperator();
                    setState(626);
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
                    setState(628);
                    if (!(precpred(_ctx, 1)))
                      throw new FailedPredicateException(this, "precpred(_ctx, 1)");
                    setState(629);
                    mathOperator();
                    setState(630);
                    ((MathExpressionAtomContext) _localctx).right = expressionAtom(2);
                  }
                  break;
                case 3:
                  {
                    _localctx =
                        new CollateExpressionAtomContext(
                            new ExpressionAtomContext(_parentctx, _parentState));
                    pushNewRecursionContext(_localctx, _startState, RULE_expressionAtom);
                    setState(632);
                    if (!(precpred(_ctx, 8)))
                      throw new FailedPredicateException(this, "precpred(_ctx, 8)");
                    setState(633);
                    match(COLLATE);
                    setState(634);
                    collationName();
                  }
                  break;
              }
            }
          }
          setState(639);
          _errHandler.sync(this);
          _alt = getInterpreter().adaptivePredict(_input, 77, _ctx);
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
    enterRule(_localctx, 62, RULE_unaryOperator);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
        setState(640);
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
    enterRule(_localctx, 64, RULE_comparisonOperator);
    try {
      setState(656);
      _errHandler.sync(this);
      switch (getInterpreter().adaptivePredict(_input, 78, _ctx)) {
        case 1:
          enterOuterAlt(_localctx, 1);
          {
            setState(642);
            match(EQUAL_SYMBOL);
          }
          break;
        case 2:
          enterOuterAlt(_localctx, 2);
          {
            setState(643);
            match(GREATER_SYMBOL);
          }
          break;
        case 3:
          enterOuterAlt(_localctx, 3);
          {
            setState(644);
            match(LESS_SYMBOL);
          }
          break;
        case 4:
          enterOuterAlt(_localctx, 4);
          {
            setState(645);
            match(LESS_SYMBOL);
            setState(646);
            match(EQUAL_SYMBOL);
          }
          break;
        case 5:
          enterOuterAlt(_localctx, 5);
          {
            setState(647);
            match(GREATER_SYMBOL);
            setState(648);
            match(EQUAL_SYMBOL);
          }
          break;
        case 6:
          enterOuterAlt(_localctx, 6);
          {
            setState(649);
            match(LESS_SYMBOL);
            setState(650);
            match(GREATER_SYMBOL);
          }
          break;
        case 7:
          enterOuterAlt(_localctx, 7);
          {
            setState(651);
            match(EXCLAMATION_SYMBOL);
            setState(652);
            match(EQUAL_SYMBOL);
          }
          break;
        case 8:
          enterOuterAlt(_localctx, 8);
          {
            setState(653);
            match(LESS_SYMBOL);
            setState(654);
            match(EQUAL_SYMBOL);
            setState(655);
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
    enterRule(_localctx, 66, RULE_logicalOperator);
    try {
      setState(665);
      _errHandler.sync(this);
      switch (_input.LA(1)) {
        case AND:
          enterOuterAlt(_localctx, 1);
          {
            setState(658);
            match(AND);
          }
          break;
        case BIT_AND_OP:
          enterOuterAlt(_localctx, 2);
          {
            setState(659);
            match(BIT_AND_OP);
            setState(660);
            match(BIT_AND_OP);
          }
          break;
        case XOR:
          enterOuterAlt(_localctx, 3);
          {
            setState(661);
            match(XOR);
          }
          break;
        case OR:
          enterOuterAlt(_localctx, 4);
          {
            setState(662);
            match(OR);
          }
          break;
        case BIT_OR_OP:
          enterOuterAlt(_localctx, 5);
          {
            setState(663);
            match(BIT_OR_OP);
            setState(664);
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
    enterRule(_localctx, 68, RULE_bitOperator);
    try {
      setState(674);
      _errHandler.sync(this);
      switch (_input.LA(1)) {
        case LESS_SYMBOL:
          enterOuterAlt(_localctx, 1);
          {
            setState(667);
            match(LESS_SYMBOL);
            setState(668);
            match(LESS_SYMBOL);
          }
          break;
        case GREATER_SYMBOL:
          enterOuterAlt(_localctx, 2);
          {
            setState(669);
            match(GREATER_SYMBOL);
            setState(670);
            match(GREATER_SYMBOL);
          }
          break;
        case BIT_AND_OP:
          enterOuterAlt(_localctx, 3);
          {
            setState(671);
            match(BIT_AND_OP);
          }
          break;
        case BIT_XOR_OP:
          enterOuterAlt(_localctx, 4);
          {
            setState(672);
            match(BIT_XOR_OP);
          }
          break;
        case BIT_OR_OP:
          enterOuterAlt(_localctx, 5);
          {
            setState(673);
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
    enterRule(_localctx, 70, RULE_mathOperator);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
        setState(676);
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
    enterRule(_localctx, 72, RULE_charsetNameBase);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
        setState(678);
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

  public static class TransactionLevelBaseContext extends ParserRuleContext {
    public TerminalNode REPEATABLE() {
      return getToken(MySqlParser.REPEATABLE, 0);
    }

    public TerminalNode COMMITTED() {
      return getToken(MySqlParser.COMMITTED, 0);
    }

    public TerminalNode UNCOMMITTED() {
      return getToken(MySqlParser.UNCOMMITTED, 0);
    }

    public TerminalNode SERIALIZABLE() {
      return getToken(MySqlParser.SERIALIZABLE, 0);
    }

    public TransactionLevelBaseContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_transactionLevelBase;
    }

    @Override
    public void enterRule(ParseTreeListener listener) {
      if (listener instanceof MySqlParserListener)
        ((MySqlParserListener) listener).enterTransactionLevelBase(this);
    }

    @Override
    public void exitRule(ParseTreeListener listener) {
      if (listener instanceof MySqlParserListener)
        ((MySqlParserListener) listener).exitTransactionLevelBase(this);
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof MySqlParserVisitor)
        return ((MySqlParserVisitor<? extends T>) visitor).visitTransactionLevelBase(this);
      else return visitor.visitChildren(this);
    }
  }

  public final TransactionLevelBaseContext transactionLevelBase() throws RecognitionException {
    TransactionLevelBaseContext _localctx = new TransactionLevelBaseContext(_ctx, getState());
    enterRule(_localctx, 74, RULE_transactionLevelBase);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
        setState(680);
        _la = _input.LA(1);
        if (!(((((_la - 622)) & ~0x3f) == 0
            && ((1L << (_la - 622))
                    & ((1L << (REPEATABLE - 622))
                        | (1L << (COMMITTED - 622))
                        | (1L << (UNCOMMITTED - 622))
                        | (1L << (SERIALIZABLE - 622))))
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

  public static class PrivilegesBaseContext extends ParserRuleContext {
    public TerminalNode TABLES() {
      return getToken(MySqlParser.TABLES, 0);
    }

    public TerminalNode ROUTINE() {
      return getToken(MySqlParser.ROUTINE, 0);
    }

    public TerminalNode EXECUTE() {
      return getToken(MySqlParser.EXECUTE, 0);
    }

    public TerminalNode FILE() {
      return getToken(MySqlParser.FILE, 0);
    }

    public TerminalNode PROCESS() {
      return getToken(MySqlParser.PROCESS, 0);
    }

    public TerminalNode RELOAD() {
      return getToken(MySqlParser.RELOAD, 0);
    }

    public TerminalNode SHUTDOWN() {
      return getToken(MySqlParser.SHUTDOWN, 0);
    }

    public TerminalNode SUPER() {
      return getToken(MySqlParser.SUPER, 0);
    }

    public TerminalNode PRIVILEGES() {
      return getToken(MySqlParser.PRIVILEGES, 0);
    }

    public PrivilegesBaseContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_privilegesBase;
    }

    @Override
    public void enterRule(ParseTreeListener listener) {
      if (listener instanceof MySqlParserListener)
        ((MySqlParserListener) listener).enterPrivilegesBase(this);
    }

    @Override
    public void exitRule(ParseTreeListener listener) {
      if (listener instanceof MySqlParserListener)
        ((MySqlParserListener) listener).exitPrivilegesBase(this);
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof MySqlParserVisitor)
        return ((MySqlParserVisitor<? extends T>) visitor).visitPrivilegesBase(this);
      else return visitor.visitChildren(this);
    }
  }

  public final PrivilegesBaseContext privilegesBase() throws RecognitionException {
    PrivilegesBaseContext _localctx = new PrivilegesBaseContext(_ctx, getState());
    enterRule(_localctx, 76, RULE_privilegesBase);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
        setState(682);
        _la = _input.LA(1);
        if (!(((((_la - 562)) & ~0x3f) == 0
            && ((1L << (_la - 562))
                    & ((1L << (TABLES - 562))
                        | (1L << (ROUTINE - 562))
                        | (1L << (EXECUTE - 562))
                        | (1L << (FILE - 562))
                        | (1L << (PROCESS - 562))
                        | (1L << (RELOAD - 562))
                        | (1L << (SHUTDOWN - 562))
                        | (1L << (SUPER - 562))
                        | (1L << (PRIVILEGES - 562))))
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
    enterRule(_localctx, 78, RULE_intervalTypeBase);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
        setState(684);
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
    enterRule(_localctx, 80, RULE_dataTypeBase);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
        setState(686);
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
    enterRule(_localctx, 82, RULE_keywordsCanBeId);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
        setState(688);
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
    enterRule(_localctx, 84, RULE_functionNameBase);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
        setState(690);
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
      case 28:
        return expression_sempred((ExpressionContext) _localctx, predIndex);
      case 29:
        return predicate_sempred((PredicateContext) _localctx, predIndex);
      case 30:
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
      "\3\u608b\ua72a\u8133\ub9ed\u417c\u3be7\u7786\u5964\3\u03d3\u02b7\4\2\t"
          + "\2\4\3\t\3\4\4\t\4\4\5\t\5\4\6\t\6\4\7\t\7\4\b\t\b\4\t\t\t\4\n\t\n\4\13"
          + "\t\13\4\f\t\f\4\r\t\r\4\16\t\16\4\17\t\17\4\20\t\20\4\21\t\21\4\22\t\22"
          + "\4\23\t\23\4\24\t\24\4\25\t\25\4\26\t\26\4\27\t\27\4\30\t\30\4\31\t\31"
          + "\4\32\t\32\4\33\t\33\4\34\t\34\4\35\t\35\4\36\t\36\4\37\t\37\4 \t \4!"
          + "\t!\4\"\t\"\4#\t#\4$\t$\4%\t%\4&\t&\4\'\t\'\4(\t(\4)\t)\4*\t*\4+\t+\4"
          + ",\t,\3\2\3\2\3\2\3\2\3\2\3\2\3\2\3\2\3\2\3\2\3\2\3\2\3\2\5\2f\n\2\3\3"
          + "\3\3\3\3\3\3\5\3l\n\3\3\4\3\4\3\4\5\4q\n\4\5\4s\n\4\3\5\3\5\3\5\3\5\5"
          + "\5y\n\5\3\6\3\6\5\6}\n\6\3\7\3\7\3\b\3\b\3\b\5\b\u0084\n\b\3\t\3\t\3\t"
          + "\3\t\3\t\3\t\3\t\3\t\3\t\5\t\u008f\n\t\3\n\3\n\3\n\5\n\u0094\n\n\3\13"
          + "\3\13\3\f\5\f\u0099\n\f\3\f\3\f\5\f\u009d\n\f\3\f\6\f\u00a0\n\f\r\f\16"
          + "\f\u00a1\3\f\5\f\u00a5\n\f\3\f\3\f\5\f\u00a9\n\f\3\f\3\f\5\f\u00ad\n\f"
          + "\5\f\u00af\n\f\3\r\3\r\3\16\5\16\u00b4\n\16\3\16\3\16\3\17\5\17\u00b9"
          + "\n\17\3\17\3\17\3\20\3\20\3\20\3\20\3\20\3\20\3\20\3\20\3\20\5\20\u00c6"
          + "\n\20\3\20\5\20\u00c9\n\20\3\21\3\21\5\21\u00cd\n\21\3\21\3\21\5\21\u00d1"
          + "\n\21\3\21\3\21\3\21\5\21\u00d6\n\21\3\21\3\21\3\21\5\21\u00db\n\21\3"
          + "\21\3\21\5\21\u00df\n\21\5\21\u00e1\n\21\3\22\3\22\3\22\3\22\3\23\3\23"
          + "\3\23\3\23\3\23\3\23\3\24\3\24\3\24\7\24\u00f0\n\24\f\24\16\24\u00f3\13"
          + "\24\3\25\3\25\3\25\5\25\u00f8\n\25\3\25\5\25\u00fb\n\25\3\25\3\25\3\25"
          + "\5\25\u0100\n\25\3\25\5\25\u0103\n\25\3\26\3\26\3\26\3\26\5\26\u0109\n"
          + "\26\3\26\3\26\3\26\3\26\3\26\5\26\u0110\n\26\3\26\3\26\5\26\u0114\n\26"
          + "\3\27\3\27\3\27\3\27\3\27\3\27\3\27\3\27\3\27\3\27\3\27\3\27\3\27\3\27"
          + "\3\27\3\27\3\27\3\27\3\27\3\27\3\27\3\27\3\27\3\27\3\27\3\27\3\27\3\27"
          + "\3\27\3\27\6\27\u0134\n\27\r\27\16\27\u0135\3\27\3\27\5\27\u013a\n\27"
          + "\3\27\3\27\3\27\3\27\6\27\u0140\n\27\r\27\16\27\u0141\3\27\3\27\5\27\u0146"
          + "\n\27\3\27\3\27\3\27\3\27\3\27\3\27\3\27\5\27\u014f\n\27\3\27\3\27\3\27"
          + "\3\27\3\27\3\27\5\27\u0157\n\27\3\27\3\27\3\27\5\27\u015c\n\27\3\27\3"
          + "\27\3\27\3\27\3\27\3\27\5\27\u0164\n\27\3\27\3\27\3\27\5\27\u0169\n\27"
          + "\3\27\3\27\3\27\5\27\u016e\n\27\5\27\u0170\n\27\3\27\3\27\3\27\3\27\3"
          + "\27\3\27\3\27\5\27\u0179\n\27\3\27\3\27\3\27\5\27\u017e\n\27\3\27\3\27"
          + "\3\27\3\27\3\27\3\27\5\27\u0186\n\27\3\27\3\27\3\27\5\27\u018b\n\27\3"
          + "\27\3\27\3\27\3\27\3\27\3\27\5\27\u0193\n\27\3\27\3\27\3\27\3\27\3\27"
          + "\3\27\5\27\u019b\n\27\3\27\5\27\u019e\n\27\3\27\3\27\3\27\3\27\3\27\3"
          + "\27\3\27\3\27\5\27\u01a8\n\27\3\27\3\27\3\27\3\27\3\27\3\27\3\27\3\27"
          + "\3\27\5\27\u01b3\n\27\3\30\3\30\3\30\3\30\3\30\3\31\3\31\3\31\3\31\7\31"
          + "\u01be\n\31\f\31\16\31\u01c1\13\31\3\31\3\31\3\31\3\31\3\31\5\31\u01c8"
          + "\n\31\3\32\3\32\5\32\u01cc\n\32\3\33\3\33\3\33\3\33\3\33\3\33\3\33\3\33"
          + "\3\33\3\33\3\33\3\33\3\33\3\33\3\33\3\33\3\33\3\33\3\33\3\33\3\33\3\33"
          + "\3\33\5\33\u01e5\n\33\3\34\3\34\3\34\3\34\5\34\u01eb\n\34\3\34\3\34\3"
          + "\34\3\34\3\34\5\34\u01f2\n\34\7\34\u01f4\n\34\f\34\16\34\u01f7\13\34\3"
          + "\35\3\35\3\35\3\35\5\35\u01fd\n\35\3\36\3\36\3\36\3\36\3\36\3\36\5\36"
          + "\u0205\n\36\3\36\3\36\3\36\5\36\u020a\n\36\3\36\3\36\3\36\3\36\7\36\u0210"
          + "\n\36\f\36\16\36\u0213\13\36\3\37\3\37\3\37\5\37\u0218\n\37\3\37\3\37"
          + "\3\37\3\37\3\37\3\37\3\37\3\37\5\37\u0222\n\37\3\37\3\37\3\37\3\37\3\37"
          + "\3\37\3\37\3\37\3\37\3\37\3\37\5\37\u022f\n\37\3\37\3\37\3\37\3\37\5\37"
          + "\u0235\n\37\3\37\3\37\3\37\3\37\3\37\3\37\3\37\3\37\3\37\3\37\5\37\u0241"
          + "\n\37\3\37\3\37\3\37\3\37\5\37\u0247\n\37\7\37\u0249\n\37\f\37\16\37\u024c"
          + "\13\37\3 \3 \3 \3 \3 \3 \3 \3 \3 \3 \3 \3 \3 \7 \u025b\n \f \16 \u025e"
          + "\13 \3 \3 \3 \3 \3 \3 \3 \6 \u0267\n \r \16 \u0268\3 \3 \3 \3 \3 \3 \5"
          + " \u0271\n \3 \3 \3 \3 \3 \3 \3 \3 \3 \3 \3 \7 \u027e\n \f \16 \u0281\13"
          + " \3!\3!\3\"\3\"\3\"\3\"\3\"\3\"\3\"\3\"\3\"\3\"\3\"\3\"\3\"\3\"\5\"\u0293"
          + "\n\"\3#\3#\3#\3#\3#\3#\3#\5#\u029c\n#\3$\3$\3$\3$\3$\3$\3$\5$\u02a5\n"
          + "$\3%\3%\3&\3&\3\'\3\'\3(\3(\3)\3)\3*\3*\3+\3+\3,\3,\3,\2\5:<>-\2\4\6\b"
          + "\n\f\16\20\22\24\26\30\32\34\36 \"$&(*,.\60\62\64\668:<>@BDFHJLNPRTV\2"
          + "\33\5\2\u0265\u026f\u03c6\u03c6\u03cf\u03cf\4\2\u03bc\u03be\u03c7\u03c7"
          + "\4\288\u009b\u009b\4\2gg\u03ca\u03ca\4\2\u00bf\u00bf\u019d\u019d\4\2\u00b8"
          + "\u00b9\u00bb\u00bb\4\2\u00a0\u00a0\u01e7\u01e7\4\2\u00e7\u00e8\u00ee\u00ee"
          + "\4\2\"\"\u00e5\u00e8\3\2\u00f1\u00f2\5\2\21\21TT\u0099\u0099\4\2\u00bd"
          + "\u00bd\u00bf\u00bf\5\2\16\16**\u032e\u032e\4\2ee\u03b1\u03b1\5\288\u009b"
          + "\u009b\u0214\u0214\4\2yy\u0083\u0083\6\2ee\u03a9\u03a9\u03ab\u03ab\u03b1"
          + "\u03b2\3\2\u03a6\u03ad\3\2\u023d\u0264\3\2\u0270\u0273\3\2\u0234\u023c"
          + "\3\2\u022c\u0233\5\2\u00b8\u00bc\u00c6\u00c6\u00c9\u00c9 \2\u00f8\u010a"
          + "\u010c\u0112\u0114\u012b\u012d\u0136\u0138\u0144\u0146\u014e\u0150\u0150"
          + "\u0152\u0153\u0155\u0157\u0159\u0159\u015b\u015b\u015d\u0163\u0165\u016b"
          + "\u016d\u0196\u0198\u019e\u01a0\u01a9\u01ab\u01b5\u01b7\u01c7\u01c9\u01d6"
          + "\u01d8\u01e9\u01eb\u01ed\u01ef\u01ff\u0201\u020b\u020d\u020d\u020f\u0212"
          + "\u0214\u0214\u0216\u0217\u021a\u021d\u021f\u0226\u026a\u026a\13\2$$VV"
          + "\u0082\u0082\u00b8\u00ba\u00bc\u00bc\u00d9\u00d9\u00f0\u00f0\u022c\u0233"
          + "\u0274\u039c\2\u033e\2e\3\2\2\2\4g\3\2\2\2\6m\3\2\2\2\bx\3\2\2\2\n|\3"
          + "\2\2\2\f~\3\2\2\2\16\u0083\3\2\2\2\20\u008e\3\2\2\2\22\u0093\3\2\2\2\24"
          + "\u0095\3\2\2\2\26\u00ae\3\2\2\2\30\u00b0\3\2\2\2\32\u00b3\3\2\2\2\34\u00b8"
          + "\3\2\2\2\36\u00c8\3\2\2\2 \u00e0\3\2\2\2\"\u00e2\3\2\2\2$\u00e6\3\2\2"
          + "\2&\u00ec\3\2\2\2(\u0102\3\2\2\2*\u0113\3\2\2\2,\u01b2\3\2\2\2.\u01b4"
          + "\3\2\2\2\60\u01c7\3\2\2\2\62\u01c9\3\2\2\2\64\u01e4\3\2\2\2\66\u01ea\3"
          + "\2\2\28\u01fc\3\2\2\2:\u0209\3\2\2\2<\u0214\3\2\2\2>\u0270\3\2\2\2@\u0282"
          + "\3\2\2\2B\u0292\3\2\2\2D\u029b\3\2\2\2F\u02a4\3\2\2\2H\u02a6\3\2\2\2J"
          + "\u02a8\3\2\2\2L\u02aa\3\2\2\2N\u02ac\3\2\2\2P\u02ae\3\2\2\2R\u02b0\3\2"
          + "\2\2T\u02b2\3\2\2\2V\u02b4\3\2\2\2Xf\5P)\2Yf\7\u00bc\2\2Zf\7\u00ca\2\2"
          + "[f\7\u00cb\2\2\\f\7\u00cc\2\2]f\7\u00cd\2\2^f\7\u00ce\2\2_f\7\u00cf\2"
          + "\2`f\7\u00d0\2\2af\7\u00d1\2\2bf\7\u00d2\2\2cf\7\u00d3\2\2df\7\u00d4\2"
          + "\2eX\3\2\2\2eY\3\2\2\2eZ\3\2\2\2e[\3\2\2\2e\\\3\2\2\2e]\3\2\2\2e^\3\2"
          + "\2\2e_\3\2\2\2e`\3\2\2\2ea\3\2\2\2eb\3\2\2\2ec\3\2\2\2ed\3\2\2\2f\3\3"
          + "\2\2\2gk\5\16\b\2hl\7\u03cd\2\2ij\7\u03b6\2\2jl\5\16\b\2kh\3\2\2\2ki\3"
          + "\2\2\2kl\3\2\2\2l\5\3\2\2\2mr\5\16\b\2np\5\22\n\2oq\5\22\n\2po\3\2\2\2"
          + "pq\3\2\2\2qs\3\2\2\2rn\3\2\2\2rs\3\2\2\2s\7\3\2\2\2ty\7\u00bf\2\2uy\5"
          + "J&\2vy\7\u03c6\2\2wy\7\u03c3\2\2xt\3\2\2\2xu\3\2\2\2xv\3\2\2\2xw\3\2\2"
          + "\2y\t\3\2\2\2z}\5\16\b\2{}\7\u03c6\2\2|z\3\2\2\2|{\3\2\2\2}\13\3\2\2\2"
          + "~\177\t\2\2\2\177\r\3\2\2\2\u0080\u0084\5\20\t\2\u0081\u0084\7\u03cf\2"
          + "\2\u0082\u0084\7\u03c3\2\2\u0083\u0080\3\2\2\2\u0083\u0081\3\2\2\2\u0083"
          + "\u0082\3\2\2\2\u0084\17\3\2\2\2\u0085\u008f\7\u03ce\2\2\u0086\u008f\5"
          + "J&\2\u0087\u008f\5L\'\2\u0088\u008f\5\f\7\2\u0089\u008f\5N(\2\u008a\u008f"
          + "\5P)\2\u008b\u008f\5R*\2\u008c\u008f\5T+\2\u008d\u008f\5V,\2\u008e\u0085"
          + "\3\2\2\2\u008e\u0086\3\2\2\2\u008e\u0087\3\2\2\2\u008e\u0088\3\2\2\2\u008e"
          + "\u0089\3\2\2\2\u008e\u008a\3\2\2\2\u008e\u008b\3\2\2\2\u008e\u008c\3\2"
          + "\2\2\u008e\u008d\3\2\2\2\u008f\21\3\2\2\2\u0090\u0094\7\u03cd\2\2\u0091"
          + "\u0092\7\u03b6\2\2\u0092\u0094\5\16\b\2\u0093\u0090\3\2\2\2\u0093\u0091"
          + "\3\2\2\2\u0094\23\3\2\2\2\u0095\u0096\t\3\2\2\u0096\25\3\2\2\2\u0097\u0099"
          + "\7\u03cc\2\2\u0098\u0097\3\2\2\2\u0098\u0099\3\2\2\2\u0099\u009a\3\2\2"
          + "\2\u009a\u009d\7\u03c6\2\2\u009b\u009d\7\u03c5\2\2\u009c\u0098\3\2\2\2"
          + "\u009c\u009b\3\2\2\2\u009d\u009f\3\2\2\2\u009e\u00a0\7\u03c6\2\2\u009f"
          + "\u009e\3\2\2\2\u00a0\u00a1\3\2\2\2\u00a1\u009f\3\2\2\2\u00a1\u00a2\3\2"
          + "\2\2\u00a2\u00af\3\2\2\2\u00a3\u00a5\7\u03cc\2\2\u00a4\u00a3\3\2\2\2\u00a4"
          + "\u00a5\3\2\2\2\u00a5\u00a6\3\2\2\2\u00a6\u00a9\7\u03c6\2\2\u00a7\u00a9"
          + "\7\u03c5\2\2\u00a8\u00a4\3\2\2\2\u00a8\u00a7\3\2\2\2\u00a9\u00ac\3\2\2"
          + "\2\u00aa\u00ab\7\32\2\2\u00ab\u00ad\5\n\6\2\u00ac\u00aa\3\2\2\2\u00ac"
          + "\u00ad\3\2\2\2\u00ad\u00af\3\2\2\2\u00ae\u009c\3\2\2\2\u00ae\u00a8\3\2"
          + "\2\2\u00af\27\3\2\2\2\u00b0\u00b1\t\4\2\2\u00b1\31\3\2\2\2\u00b2\u00b4"
          + "\7\u03cc\2\2\u00b3\u00b2\3\2\2\2\u00b3\u00b4\3\2\2\2\u00b4\u00b5\3\2\2"
          + "\2\u00b5\u00b6\7\u03c8\2\2\u00b6\33\3\2\2\2\u00b7\u00b9\7e\2\2\u00b8\u00b7"
          + "\3\2\2\2\u00b8\u00b9\3\2\2\2\u00b9\u00ba\3\2\2\2\u00ba\u00bb\t\5\2\2\u00bb"
          + "\35\3\2\2\2\u00bc\u00c9\5\26\f\2\u00bd\u00c9\5\24\13\2\u00be\u00bf\7\u03ab"
          + "\2\2\u00bf\u00c9\5\24\13\2\u00c0\u00c9\5\32\16\2\u00c1\u00c9\5\30\r\2"
          + "\u00c2\u00c9\7\u03c9\2\2\u00c3\u00c9\7\u03cb\2\2\u00c4\u00c6\7e\2\2\u00c5"
          + "\u00c4\3\2\2\2\u00c5\u00c6\3\2\2\2\u00c6\u00c7\3\2\2\2\u00c7\u00c9\t\5"
          + "\2\2\u00c8\u00bc\3\2\2\2\u00c8\u00bd\3\2\2\2\u00c8\u00be\3\2\2\2\u00c8"
          + "\u00c0\3\2\2\2\u00c8\u00c1\3\2\2\2\u00c8\u00c2\3\2\2\2\u00c8\u00c3\3\2"
          + "\2\2\u00c8\u00c5\3\2\2\2\u00c9\37\3\2\2\2\u00ca\u00cc\t\6\2\2\u00cb\u00cd"
          + "\5\"\22\2\u00cc\u00cb\3\2\2\2\u00cc\u00cd\3\2\2\2\u00cd\u00e1\3\2\2\2"
          + "\u00ce\u00d0\7\u00bd\2\2\u00cf\u00d1\5\"\22\2\u00d0\u00cf\3\2\2\2\u00d0"
          + "\u00d1\3\2\2\2\u00d1\u00d5\3\2\2\2\u00d2\u00d3\7\30\2\2\u00d3\u00d4\7"
          + "\u0087\2\2\u00d4\u00d6\5\b\5\2\u00d5\u00d2\3\2\2\2\u00d5\u00d6\3\2\2\2"
          + "\u00d6\u00e1\3\2\2\2\u00d7\u00e1\t\7\2\2\u00d8\u00da\7\u00b6\2\2\u00d9"
          + "\u00db\5$\23\2\u00da\u00d9\3\2\2\2\u00da\u00db\3\2\2\2\u00db\u00e1\3\2"
          + "\2\2\u00dc\u00de\t\b\2\2\u00dd\u00df\7\u00b1\2\2\u00de\u00dd\3\2\2\2\u00de"
          + "\u00df\3\2\2\2\u00df\u00e1\3\2\2\2\u00e0\u00ca\3\2\2\2\u00e0\u00ce\3\2"
          + "\2\2\u00e0\u00d7\3\2\2\2\u00e0\u00d8\3\2\2\2\u00e0\u00dc\3\2\2\2\u00e1"
          + "!\3\2\2\2\u00e2\u00e3\7\u03b7\2\2\u00e3\u00e4\5\24\13\2\u00e4\u00e5\7"
          + "\u03b8\2\2\u00e5#\3\2\2\2\u00e6\u00e7\7\u03b7\2\2\u00e7\u00e8\5\24\13"
          + "\2\u00e8\u00e9\7\u03b9\2\2\u00e9\u00ea\5\24\13\2\u00ea\u00eb\7\u03b8\2"
          + "\2\u00eb%\3\2\2\2\u00ec\u00f1\5:\36\2\u00ed\u00ee\7\u03b9\2\2\u00ee\u00f0"
          + "\5:\36\2\u00ef\u00ed\3\2\2\2\u00f0\u00f3\3\2\2\2\u00f1\u00ef\3\2\2\2\u00f1"
          + "\u00f2\3\2\2\2\u00f2\'\3\2\2\2\u00f3\u00f1\3\2\2\2\u00f4\u00fa\t\t\2\2"
          + "\u00f5\u00f7\7\u03b7\2\2\u00f6\u00f8\5\24\13\2\u00f7\u00f6\3\2\2\2\u00f7"
          + "\u00f8\3\2\2\2\u00f8\u00f9\3\2\2\2\u00f9\u00fb\7\u03b8\2\2\u00fa\u00f5"
          + "\3\2\2\2\u00fa\u00fb\3\2\2\2\u00fb\u0103\3\2\2\2\u00fc\u00fd\7\u00ef\2"
          + "\2\u00fd\u00ff\7\u03b7\2\2\u00fe\u0100\5\24\13\2\u00ff\u00fe\3\2\2\2\u00ff"
          + "\u0100\3\2\2\2\u0100\u0101\3\2\2\2\u0101\u0103\7\u03b8\2\2\u0102\u00f4"
          + "\3\2\2\2\u0102\u00fc\3\2\2\2\u0103)\3\2\2\2\u0104\u0114\5,\27\2\u0105"
          + "\u0106\5\64\33\2\u0106\u0108\7\u03b7\2\2\u0107\u0109\5\66\34\2\u0108\u0107"
          + "\3\2\2\2\u0108\u0109\3\2\2\2\u0109\u010a\3\2\2\2\u010a\u010b\7\u03b8\2"
          + "\2\u010b\u0114\3\2\2\2\u010c\u010d\5\4\3\2\u010d\u010f\7\u03b7\2\2\u010e"
          + "\u0110\5\66\34\2\u010f\u010e\3\2\2\2\u010f\u0110\3\2\2\2\u0110\u0111\3"
          + "\2\2\2\u0111\u0112\7\u03b8\2\2\u0112\u0114\3\2\2\2\u0113\u0104\3\2\2\2"
          + "\u0113\u0105\3\2\2\2\u0113\u010c\3\2\2\2\u0114+\3\2\2\2\u0115\u01b3\t"
          + "\n\2\2\u0116\u0117\7\37\2\2\u0117\u0118\7\u03b7\2\2\u0118\u0119\5:\36"
          + "\2\u0119\u011a\7\u03b9\2\2\u011a\u011b\5 \21\2\u011b\u011c\7\u03b8\2\2"
          + "\u011c\u01b3\3\2\2\2\u011d\u011e\7\37\2\2\u011e\u011f\7\u03b7\2\2\u011f"
          + "\u0120\5:\36\2\u0120\u0121\7\u00a4\2\2\u0121\u0122\5\b\5\2\u0122\u0123"
          + "\7\u03b8\2\2\u0123\u01b3\3\2\2\2\u0124\u0125\7\26\2\2\u0125\u0126\7\u03b7"
          + "\2\2\u0126\u0127\5:\36\2\u0127\u0128\7\r\2\2\u0128\u0129\5 \21\2\u0129"
          + "\u012a\7\u03b8\2\2\u012a\u01b3\3\2\2\2\u012b\u012c\7\u00a5\2\2\u012c\u012d"
          + "\7\u03b7\2\2\u012d\u012e\5\6\4\2\u012e\u012f\7\u03b8\2\2\u012f\u01b3\3"
          + "\2\2\2\u0130\u0131\7\25\2\2\u0131\u0133\5:\36\2\u0132\u0134\5.\30\2\u0133"
          + "\u0132\3\2\2\2\u0134\u0135\3\2\2\2\u0135\u0133\3\2\2\2\u0135\u0136\3\2"
          + "\2\2\u0136\u0139\3\2\2\2\u0137\u0138\7\61\2\2\u0138\u013a\58\35\2\u0139"
          + "\u0137\3\2\2\2\u0139\u013a\3\2\2\2\u013a\u013b\3\2\2\2\u013b\u013c\7\u0137"
          + "\2\2\u013c\u01b3\3\2\2\2\u013d\u013f\7\25\2\2\u013e\u0140\5.\30\2\u013f"
          + "\u013e\3\2\2\2\u0140\u0141\3\2\2\2\u0141\u013f\3\2\2\2\u0141\u0142\3\2"
          + "\2\2\u0142\u0145\3\2\2\2\u0143\u0144\7\61\2\2\u0144\u0146\58\35\2\u0145"
          + "\u0143\3\2\2\2\u0145\u0146\3\2\2\2\u0146\u0147\3\2\2\2\u0147\u0148\7\u0137"
          + "\2\2\u0148\u01b3\3\2\2\2\u0149\u014a\7\u00bd\2\2\u014a\u014b\7\u03b7\2"
          + "\2\u014b\u014e\5\66\34\2\u014c\u014d\7\u00a4\2\2\u014d\u014f\5\b\5\2\u014e"
          + "\u014c\3\2\2\2\u014e\u014f\3\2\2\2\u014f\u0150\3\2\2\2\u0150\u0151\7\u03b8"
          + "\2\2\u0151\u01b3\3\2\2\2\u0152\u0153\7\u00f0\2\2\u0153\u0156\7\u03b7\2"
          + "\2\u0154\u0157\5\26\f\2\u0155\u0157\5:\36\2\u0156\u0154\3\2\2\2\u0156"
          + "\u0155\3\2\2\2\u0157\u0158\3\2\2\2\u0158\u015b\7F\2\2\u0159\u015c\5\26"
          + "\f\2\u015a\u015c\5:\36\2\u015b\u0159\3\2\2\2\u015b\u015a\3\2\2\2\u015c"
          + "\u015d\3\2\2\2\u015d\u015e\7\u03b8\2\2\u015e\u01b3\3\2\2\2\u015f\u0160"
          + "\t\13\2\2\u0160\u0163\7\u03b7\2\2\u0161\u0164\5\26\f\2\u0162\u0164\5:"
          + "\36\2\u0163\u0161\3\2\2\2\u0163\u0162\3\2\2\2\u0164\u0165\3\2\2\2\u0165"
          + "\u0168\7=\2\2\u0166\u0169\5\24\13\2\u0167\u0169\5:\36\2\u0168\u0166\3"
          + "\2\2\2\u0168\u0167\3\2\2\2\u0169\u016f\3\2\2\2\u016a\u016d\7:\2\2\u016b"
          + "\u016e\5\24\13\2\u016c\u016e\5:\36\2\u016d\u016b\3\2\2\2\u016d\u016c\3"
          + "\2\2\2\u016e\u0170\3\2\2\2\u016f\u016a\3\2\2\2\u016f\u0170\3\2\2\2\u0170"
          + "\u0171\3\2\2\2\u0171\u0172\7\u03b8\2\2\u0172\u01b3\3\2\2\2\u0173\u0174"
          + "\7\u00f4\2\2\u0174\u0175\7\u03b7\2\2\u0175\u0178\t\f\2\2\u0176\u0179\5"
          + "\26\f\2\u0177\u0179\5:\36\2\u0178\u0176\3\2\2\2\u0178\u0177\3\2\2\2\u0178"
          + "\u0179\3\2\2\2\u0179\u017a\3\2\2\2\u017a\u017d\7=\2\2\u017b\u017e\5\26"
          + "\f\2\u017c\u017e\5:\36\2\u017d\u017b\3\2\2\2\u017d\u017c\3\2\2\2\u017e"
          + "\u017f\3\2\2\2\u017f\u0180\7\u03b8\2\2\u0180\u01b3\3\2\2\2\u0181\u0182"
          + "\7\u00f4\2\2\u0182\u0185\7\u03b7\2\2\u0183\u0186\5\26\f\2\u0184\u0186"
          + "\5:\36\2\u0185\u0183\3\2\2\2\u0185\u0184\3\2\2\2\u0186\u0187\3\2\2\2\u0187"
          + "\u018a\7=\2\2\u0188\u018b\5\26\f\2\u0189\u018b\5:\36\2\u018a\u0188\3\2"
          + "\2\2\u018a\u0189\3\2\2\2\u018b\u018c\3\2\2\2\u018c\u018d\7\u03b8\2\2\u018d"
          + "\u01b3\3\2\2\2\u018e\u018f\7\u0398\2\2\u018f\u0192\7\u03b7\2\2\u0190\u0193"
          + "\5\26\f\2\u0191\u0193\5:\36\2\u0192\u0190\3\2\2\2\u0192\u0191\3\2\2\2"
          + "\u0193\u019a\3\2\2\2\u0194\u0195\7\r\2\2\u0195\u0196\t\r\2\2\u0196\u0197"
          + "\7\u03b7\2\2\u0197\u0198\5\24\13\2\u0198\u0199\7\u03b8\2\2\u0199\u019b"
          + "\3\2\2\2\u019a\u0194\3\2\2\2\u019a\u019b\3\2\2\2\u019b\u019d\3\2\2\2\u019c"
          + "\u019e\5\60\31\2\u019d\u019c\3\2\2\2\u019d\u019e\3\2\2\2\u019e\u019f\3"
          + "\2\2\2\u019f\u01a0\7\u03b8\2\2\u01a0\u01b3\3\2\2\2\u01a1\u01a2\7\u00ed"
          + "\2\2\u01a2\u01a3\7\u03b7\2\2\u01a3\u01a4\5\2\2\2\u01a4\u01a7\7=\2\2\u01a5"
          + "\u01a8\5\26\f\2\u01a6\u01a8\5:\36\2\u01a7\u01a5\3\2\2\2\u01a7\u01a6\3"
          + "\2\2\2\u01a8\u01a9\3\2\2\2\u01a9\u01aa\7\u03b8\2\2\u01aa\u01b3\3\2\2\2"
          + "\u01ab\u01ac\7\u02d1\2\2\u01ac\u01ad\7\u03b7\2\2\u01ad\u01ae\t\7\2\2\u01ae"
          + "\u01af\7\u03b9\2\2\u01af\u01b0\5\26\f\2\u01b0\u01b1\7\u03b8\2\2\u01b1"
          + "\u01b3\3\2\2\2\u01b2\u0115\3\2\2\2\u01b2\u0116\3\2\2\2\u01b2\u011d\3\2"
          + "\2\2\u01b2\u0124\3\2\2\2\u01b2\u012b\3\2\2\2\u01b2\u0130\3\2\2\2\u01b2"
          + "\u013d\3\2\2\2\u01b2\u0149\3\2\2\2\u01b2\u0152\3\2\2\2\u01b2\u015f\3\2"
          + "\2\2\u01b2\u0173\3\2\2\2\u01b2\u0181\3\2\2\2\u01b2\u018e\3\2\2\2\u01b2"
          + "\u01a1\3\2\2\2\u01b2\u01ab\3\2\2\2\u01b3-\3\2\2\2\u01b4\u01b5\7\u00a6"
          + "\2\2\u01b5\u01b6\58\35\2\u01b6\u01b7\7\u0097\2\2\u01b7\u01b8\58\35\2\u01b8"
          + "/\3\2\2\2\u01b9\u01ba\7\u0172\2\2\u01ba\u01bf\5\62\32\2\u01bb\u01bc\7"
          + "\u03b9\2\2\u01bc\u01be\5\62\32\2\u01bd\u01bb\3\2\2\2\u01be\u01c1\3\2\2"
          + "\2\u01bf\u01bd\3\2\2\2\u01bf\u01c0\3\2\2\2\u01c0\u01c8\3\2\2\2\u01c1\u01bf"
          + "\3\2\2\2\u01c2\u01c3\7\u0172\2\2\u01c3\u01c4\5\24\13\2\u01c4\u01c5\7\u03ab"
          + "\2\2\u01c5\u01c6\5\24\13\2\u01c6\u01c8\3\2\2\2\u01c7\u01b9\3\2\2\2\u01c7"
          + "\u01c2\3\2\2\2\u01c8\61\3\2\2\2\u01c9\u01cb\5\24\13\2\u01ca\u01cc\t\16"
          + "\2\2\u01cb\u01ca\3\2\2\2\u01cb\u01cc\3\2\2\2\u01cc\63\3\2\2\2\u01cd\u01e5"
          + "\5V,\2\u01ce\u01e5\7\u023e\2\2\u01cf\u01e5\7\u00e9\2\2\u01d0\u01e5\7\u00e5"
          + "\2\2\u01d1\u01e5\7\u00e6\2\2\u01d2\u01e5\7\u00e7\2\2\u01d3\u01e5\7\u00ea"
          + "\2\2\u01d4\u01e5\7\u00eb\2\2\u01d5\u01e5\7\u00ec\2\2\u01d6\u01e5\7D\2"
          + "\2\u01d7\u01e5\7K\2\2\u01d8\u01e5\7\u00e8\2\2\u01d9\u01e5\7\u00ee\2\2"
          + "\u01da\u01e5\7\u0194\2\2\u01db\u01e5\7\u00ef\2\2\u01dc\u01e5\7}\2\2\u01dd"
          + "\u01e5\7\u00f1\2\2\u01de\u01e5\7\u00f2\2\2\u01df\u01e5\7\u00f3\2\2\u01e0"
          + "\u01e5\7\u00f4\2\2\u01e1\u01e5\7\u00f5\2\2\u01e2\u01e5\7\u00f6\2\2\u01e3"
          + "\u01e5\7\u00f7\2\2\u01e4\u01cd\3\2\2\2\u01e4\u01ce\3\2\2\2\u01e4\u01cf"
          + "\3\2\2\2\u01e4\u01d0\3\2\2\2\u01e4\u01d1\3\2\2\2\u01e4\u01d2\3\2\2\2\u01e4"
          + "\u01d3\3\2\2\2\u01e4\u01d4\3\2\2\2\u01e4\u01d5\3\2\2\2\u01e4\u01d6\3\2"
          + "\2\2\u01e4\u01d7\3\2\2\2\u01e4\u01d8\3\2\2\2\u01e4\u01d9\3\2\2\2\u01e4"
          + "\u01da\3\2\2\2\u01e4\u01db\3\2\2\2\u01e4\u01dc\3\2\2\2\u01e4\u01dd\3\2"
          + "\2\2\u01e4\u01de\3\2\2\2\u01e4\u01df\3\2\2\2\u01e4\u01e0\3\2\2\2\u01e4"
          + "\u01e1\3\2\2\2\u01e4\u01e2\3\2\2\2\u01e4\u01e3\3\2\2\2\u01e5\65\3\2\2"
          + "\2\u01e6\u01eb\5\36\20\2\u01e7\u01eb\5\6\4\2\u01e8\u01eb\5*\26\2\u01e9"
          + "\u01eb\5:\36\2\u01ea\u01e6\3\2\2\2\u01ea\u01e7\3\2\2\2\u01ea\u01e8\3\2"
          + "\2\2\u01ea\u01e9\3\2\2\2\u01eb\u01f5\3\2\2\2\u01ec\u01f1\7\u03b9\2\2\u01ed"
          + "\u01f2\5\36\20\2\u01ee\u01f2\5\6\4\2\u01ef\u01f2\5*\26\2\u01f0\u01f2\5"
          + ":\36\2\u01f1\u01ed\3\2\2\2\u01f1\u01ee\3\2\2\2\u01f1\u01ef\3\2\2\2\u01f1"
          + "\u01f0\3\2\2\2\u01f2\u01f4\3\2\2\2\u01f3\u01ec\3\2\2\2\u01f4\u01f7\3\2"
          + "\2\2\u01f5\u01f3\3\2\2\2\u01f5\u01f6\3\2\2\2\u01f6\67\3\2\2\2\u01f7\u01f5"
          + "\3\2\2\2\u01f8\u01fd\5\36\20\2\u01f9\u01fd\5\6\4\2\u01fa\u01fd\5*\26\2"
          + "\u01fb\u01fd\5:\36\2\u01fc\u01f8\3\2\2\2\u01fc\u01f9\3\2\2\2\u01fc\u01fa"
          + "\3\2\2\2\u01fc\u01fb\3\2\2\2\u01fd9\3\2\2\2\u01fe\u01ff\b\36\1\2\u01ff"
          + "\u0200\t\17\2\2\u0200\u020a\5:\36\6\u0201\u0202\5<\37\2\u0202\u0204\7"
          + "N\2\2\u0203\u0205\7e\2\2\u0204\u0203\3\2\2\2\u0204\u0205\3\2\2\2\u0205"
          + "\u0206\3\2\2\2\u0206\u0207\t\20\2\2\u0207\u020a\3\2\2\2\u0208\u020a\5"
          + "<\37\2\u0209\u01fe\3\2\2\2\u0209\u0201\3\2\2\2\u0209\u0208\3\2\2\2\u020a"
          + "\u0211\3\2\2\2\u020b\u020c\f\5\2\2\u020c\u020d\5D#\2\u020d\u020e\5:\36"
          + "\6\u020e\u0210\3\2\2\2\u020f\u020b\3\2\2\2\u0210\u0213\3\2\2\2\u0211\u020f"
          + "\3\2\2\2\u0211\u0212\3\2\2\2\u0212;\3\2\2\2\u0213\u0211\3\2\2\2\u0214"
          + "\u0217\b\37\1\2\u0215\u0216\7\u03d1\2\2\u0216\u0218\7\u039d\2\2\u0217"
          + "\u0215\3\2\2\2\u0217\u0218\3\2\2\2\u0218\u0219\3\2\2\2\u0219\u021a\5>"
          + " \2\u021a\u024a\3\2\2\2\u021b\u021c\f\b\2\2\u021c\u021d\5B\"\2\u021d\u021e"
          + "\5<\37\t\u021e\u0249\3\2\2\2\u021f\u0221\f\7\2\2\u0220\u0222\7e\2\2\u0221"
          + "\u0220\3\2\2\2\u0221\u0222\3\2\2\2\u0222\u0223\3\2\2\2\u0223\u0224\7\20"
          + "\2\2\u0224\u0225\5<\37\2\u0225\u0226\7\f\2\2\u0226\u0227\5<\37\b\u0227"
          + "\u0249\3\2\2\2\u0228\u0229\f\6\2\2\u0229\u022a\7\u01ef\2\2\u022a\u022b"
          + "\7W\2\2\u022b\u0249\5<\37\7\u022c\u022e\f\4\2\2\u022d\u022f\7e\2\2\u022e"
          + "\u022d\3\2\2\2\u022e\u022f\3\2\2\2\u022f\u0230\3\2\2\2\u0230\u0231\t\21"
          + "\2\2\u0231\u0249\5<\37\5\u0232\u0234\f\n\2\2\u0233\u0235\7e\2\2\u0234"
          + "\u0233\3\2\2\2\u0234\u0235\3\2\2\2\u0235\u0236\3\2\2\2\u0236\u0237\7F"
          + "\2\2\u0237\u0238\7\u03b7\2\2\u0238\u0239\5&\24\2\u0239\u023a\7\u03b8\2"
          + "\2\u023a\u0249\3\2\2\2\u023b\u023c\f\t\2\2\u023c\u023d\7N\2\2\u023d\u0249"
          + "\5\34\17\2\u023e\u0240\f\5\2\2\u023f\u0241\7e\2\2\u0240\u023f\3\2\2\2"
          + "\u0240\u0241\3\2\2\2\u0241\u0242\3\2\2\2\u0242\u0243\7W\2\2\u0243\u0246"
          + "\5<\37\2\u0244\u0245\7\u013d\2\2\u0245\u0247\7\u03c6\2\2\u0246\u0244\3"
          + "\2\2\2\u0246\u0247\3\2\2\2\u0247\u0249\3\2\2\2\u0248\u021b\3\2\2\2\u0248"
          + "\u021f\3\2\2\2\u0248\u0228\3\2\2\2\u0248\u022c\3\2\2\2\u0248\u0232\3\2"
          + "\2\2\u0248\u023b\3\2\2\2\u0248\u023e\3\2\2\2\u0249\u024c\3\2\2\2\u024a"
          + "\u0248\3\2\2\2\u024a\u024b\3\2\2\2\u024b=\3\2\2\2\u024c\u024a\3\2\2\2"
          + "\u024d\u024e\b \1\2\u024e\u0271\5\36\20\2\u024f\u0271\5\6\4\2\u0250\u0271"
          + "\5*\26\2\u0251\u0252\5@!\2\u0252\u0253\5> \t\u0253\u0271\3\2\2\2\u0254"
          + "\u0255\7\u00bf\2\2\u0255\u0271\5> \b\u0256\u0257\7\u03b7\2\2\u0257\u025c"
          + "\5:\36\2\u0258\u0259\7\u03b9\2\2\u0259\u025b\5:\36\2\u025a\u0258\3\2\2"
          + "\2\u025b\u025e\3\2\2\2\u025c\u025a\3\2\2\2\u025c\u025d\3\2\2\2\u025d\u025f"
          + "\3\2\2\2\u025e\u025c\3\2\2\2\u025f\u0260\7\u03b8\2\2\u0260\u0271\3\2\2"
          + "\2\u0261\u0262\7\u01dd\2\2\u0262\u0263\7\u03b7\2\2\u0263\u0266\5:\36\2"
          + "\u0264\u0265\7\u03b9\2\2\u0265\u0267\5:\36\2\u0266\u0264\3\2\2\2\u0267"
          + "\u0268\3\2\2\2\u0268\u0266\3\2\2\2\u0268\u0269\3\2\2\2\u0269\u026a\3\2"
          + "\2\2\u026a\u026b\7\u03b8\2\2\u026b\u0271\3\2\2\2\u026c\u026d\7L\2\2\u026d"
          + "\u026e\5:\36\2\u026e\u026f\5\2\2\2\u026f\u0271\3\2\2\2\u0270\u024d\3\2"
          + "\2\2\u0270\u024f\3\2\2\2\u0270\u0250\3\2\2\2\u0270\u0251\3\2\2\2\u0270"
          + "\u0254\3\2\2\2\u0270\u0256\3\2\2\2\u0270\u0261\3\2\2\2\u0270\u026c\3\2"
          + "\2\2\u0271\u027f\3\2\2\2\u0272\u0273\f\4\2\2\u0273\u0274\5F$\2\u0274\u0275"
          + "\5> \5\u0275\u027e\3\2\2\2\u0276\u0277\f\3\2\2\u0277\u0278\5H%\2\u0278"
          + "\u0279\5> \4\u0279\u027e\3\2\2\2\u027a\u027b\f\n\2\2\u027b\u027c\7\32"
          + "\2\2\u027c\u027e\5\n\6\2\u027d\u0272\3\2\2\2\u027d\u0276\3\2\2\2\u027d"
          + "\u027a\3\2\2\2\u027e\u0281\3\2\2\2\u027f\u027d\3\2\2\2\u027f\u0280\3\2"
          + "\2\2\u0280?\3\2\2\2\u0281\u027f\3\2\2\2\u0282\u0283\t\22\2\2\u0283A\3"
          + "\2\2\2\u0284\u0293\7\u03ae\2\2\u0285\u0293\7\u03af\2\2\u0286\u0293\7\u03b0"
          + "\2\2\u0287\u0288\7\u03b0\2\2\u0288\u0293\7\u03ae\2\2\u0289\u028a\7\u03af"
          + "\2\2\u028a\u0293\7\u03ae\2\2\u028b\u028c\7\u03b0\2\2\u028c\u0293\7\u03af"
          + "\2\2\u028d\u028e\7\u03b1\2\2\u028e\u0293\7\u03ae\2\2\u028f\u0290\7\u03b0"
          + "\2\2\u0290\u0291\7\u03ae\2\2\u0291\u0293\7\u03af\2\2\u0292\u0284\3\2\2"
          + "\2\u0292\u0285\3\2\2\2\u0292\u0286\3\2\2\2\u0292\u0287\3\2\2\2\u0292\u0289"
          + "\3\2\2\2\u0292\u028b\3\2\2\2\u0292\u028d\3\2\2\2\u0292\u028f\3\2\2\2\u0293"
          + "C\3\2\2\2\u0294\u029c\7\f\2\2\u0295\u0296\7\u03b4\2\2\u0296\u029c\7\u03b4"
          + "\2\2\u0297\u029c\7\u00ab\2\2\u0298\u029c\7l\2\2\u0299\u029a\7\u03b3\2"
          + "\2\u029a\u029c\7\u03b3\2\2\u029b\u0294\3\2\2\2\u029b\u0295\3\2\2\2\u029b"
          + "\u0297\3\2\2\2\u029b\u0298\3\2\2\2\u029b\u0299\3\2\2\2\u029cE\3\2\2\2"
          + "\u029d\u029e\7\u03b0\2\2\u029e\u02a5\7\u03b0\2\2\u029f\u02a0\7\u03af\2"
          + "\2\u02a0\u02a5\7\u03af\2\2\u02a1\u02a5\7\u03b4\2\2\u02a2\u02a5\7\u03b5"
          + "\2\2\u02a3\u02a5\7\u03b3\2\2\u02a4\u029d\3\2\2\2\u02a4\u029f\3\2\2\2\u02a4"
          + "\u02a1\3\2\2\2\u02a4\u02a2\3\2\2\2\u02a4\u02a3\3\2\2\2\u02a5G\3\2\2\2"
          + "\u02a6\u02a7\t\23\2\2\u02a7I\3\2\2\2\u02a8\u02a9\t\24\2\2\u02a9K\3\2\2"
          + "\2\u02aa\u02ab\t\25\2\2\u02abM\3\2\2\2\u02ac\u02ad\t\26\2\2\u02adO\3\2"
          + "\2\2\u02ae\u02af\t\27\2\2\u02afQ\3\2\2\2\u02b0\u02b1\t\30\2\2\u02b1S\3"
          + "\2\2\2\u02b2\u02b3\t\31\2\2\u02b3U\3\2\2\2\u02b4\u02b5\t\32\2\2\u02b5"
          + "W\3\2\2\2Sekprx|\u0083\u008e\u0093\u0098\u009c\u00a1\u00a4\u00a8\u00ac"
          + "\u00ae\u00b3\u00b8\u00c5\u00c8\u00cc\u00d0\u00d5\u00da\u00de\u00e0\u00f1"
          + "\u00f7\u00fa\u00ff\u0102\u0108\u010f\u0113\u0135\u0139\u0141\u0145\u014e"
          + "\u0156\u015b\u0163\u0168\u016d\u016f\u0178\u017d\u0185\u018a\u0192\u019a"
          + "\u019d\u01a7\u01b2\u01bf\u01c7\u01cb\u01e4\u01ea\u01f1\u01f5\u01fc\u0204"
          + "\u0209\u0211\u0217\u0221\u022e\u0234\u0240\u0246\u0248\u024a\u025c\u0268"
          + "\u0270\u027d\u027f\u0292\u029b\u02a4";
  public static final ATN _ATN = new ATNDeserializer().deserialize(_serializedATN.toCharArray());

  static {
    _decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
    for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
      _decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
    }
  }
}
