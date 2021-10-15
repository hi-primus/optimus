from enum import Enum

from optimus.helpers.logger import logger


# Python to PySpark reference
#
# type(None): NullType,
# bool: BooleanType,
# int: LongType,
# float: DoubleType,
# str: StringType,
# bytearray: BinaryType,
# decimal.Decimal: DecimalType,
# datetime.date: DateType,
# datetime.datetime: TimestampType,
# datetime.time: TimestampType,
# Profiler


class Actions(Enum):
    """
    Actions that modify a columns/rows.
    """
    # COLUMNS
    INFERRED_DATA_TYPE = "inferred_data_type"
    MATCH = "match"
    LOWER = "lower"
    UPPER = "upper"
    PROPER = "proper"
    PAD = "pad"
    TRIM = "trim"
    REVERSE = "reverse"
    REMOVE_ACCENTS = "remove"
    REMOVE_SPECIAL_CHARS = "remove"
    REMOVE_WHITE_SPACES = "remove"
    LEFT = "left"
    RIGHT = "right"
    MID = "mid"
    REPLACE = "replace"
    REPLACE_REGEX = "replace"
    FILL_NA = "fill_na"
    CAST = "cast"
    IS_NA = "is_na"
    Z_SCORE = "z_score"
    NEST = "nest"
    UNNEST = "unnest"
    SET = "set"
    STRING_TO_INDEX = "string_to_index"
    DATE_FORMAT = "date_format"
    FORMAT_DATE = "format_date"
    INDEX_TO_STRING = "index_to_string"
    MIN_MAX_SCALER = "min_max_scaler"
    MAX_ABS_SCALER = "max_abs_scaler"
    STANDARD_SCALER = "standard_scaler"
    APPLY_COLS = "apply_cols"
    YEARS_BETWEEN = "apply_cols"
    IMPUTE = "impute"
    EXTRACT = "extract"
    ABS = "abs"
    MATH = "math"
    VARIANCE = "variance"
    SLICE = "slice"
    CLIP = "clip"
    DROP = "drop"
    KEEP = "keep"
    CUT = "cut"
    CUM_SUM = "cumsum"
    CUM_PROD = "cumprod"
    CUM_MAX = "cummax"
    CUM_MIN = "cummin"
    TO_FLOAT = "to_float"
    TO_INTEGER = "to_integer"
    TO_BOOL = "to_bool"
    TO_BOOLEAN = "to_boolean"
    TO_STRING = "to_string"
    YEAR = "year"
    MONTH = "month"
    DAY = "day"
    HOUR = "hour"
    MINUTE = "minute"
    SECOND = "second"
    WEEKDAY = "weekday"
    APPEND = "append"
    COPY = "copy"
    RENAME = "rename"
    UNIQUE = "unique"
    INFER = "infer"
    WORD_TOKENIZE = "word_tokenize"
    LENGTH = "length"
    GET = "get"
    ITEM = "item"

    # URL Example	http://search.somedb.com:8080/history?era=darkages
    # scheme	http
    # hostname	search.somedb.com
    # port	    8080
    # origin	http://search.somedb.com:8080
    # path	    /history
    # query	    ?era=darkages
    DOMAIN = "domain"
    TOP_DOMAIN = "top_domain"
    SUB_DOMAIN = "sub_domain"
    URL_SCHEME = "url_scheme"
    URL_PATH = "url_path"
    URL_FILE = "url_file"
    URL_QUERY = "url_query"
    URL_FRAGMENT = "url_fragment"
    HOST = "host"
    PORT = "port"

    EMAIL_DOMAIN = "email_domain"
    EMAIL_USER = "email_user"

    FINGERPRINT = "fingerprint"
    NGRAM_FINGERPRINT = "ngram_fingerprint"
    NGRAMS = "ngrams"
    LEVENSHTEIN = "levenshtein"

    # PHONETIC ENCODING
    SOUNDEX = "soundex"
    METAPHONE = "metaphone"
    DOUBLE_METAPHONE = "double_metaphone"
    MATCH_RATING_CODEX = "match_rating_codex"
    NYSIIS = "nysiis"

    # ROWS
    SELECT_ROW = "select_row"
    DROP_ROW = "drop_row"
    BETWEEN_ROW = "between_drop"
    SORT_ROW = "sort_row"

    @staticmethod
    def list():
        return list(map(lambda c: c.value, Actions))


class ProfilerDataTypesQuality(Enum):
    MISMATCH = 0
    MISSING = 1
    MATCH = 2


class ProfilerDataTypes(Enum):
    INT = "int"
    FLOAT = "float"
    STRING = "str"
    BOOL = "bool"
    BOOLEAN = "boolean"
    DATETIME = "datetime"
    ARRAY = "array"
    OBJECT = "object"
    GENDER = "gender"
    IP = "ip"
    URL = "url"
    EMAIL = "email"
    CREDIT_CARD_NUMBER = "credit_card_number"
    ZIP_CODE = "zip_code"
    MISSING = "missing"
    CATEGORICAL = "categorical"
    PHONE_NUMBER = "phone_number"
    SOCIAL_SECURITY_NUMBER = "social_security_number"
    HTTP_CODE = "http_code"
    US_STATE = "us_state"
    NULL = "null"

    @staticmethod
    def list():
        return list(map(lambda c: c.value, ProfilerDataTypes))


class Schemas(Enum):
    S3 = 's3://'
    GCS = 'gcs://'
    GC = 'gc://'
    HTTP = 'http://'
    HTTPS = 'https://'
    FTP = 'ftp://'
    FILE = 'file://'
    AZ = 'az://'
    ADL = 'adl://'
    ABFS = 'abfs://'

    @staticmethod
    def list():
        return list(map(lambda c: c.value, Schemas))


PROFILER_NUMERIC_DTYPES = [ProfilerDataTypes.INT.value, ProfilerDataTypes.FLOAT.value]
PROFILER_STRING_DTYPES = [ProfilerDataTypes.STRING.value, ProfilerDataTypes.BOOLEAN.value,
                          ProfilerDataTypes.DATETIME.value, ProfilerDataTypes.ARRAY.value,
                          ProfilerDataTypes.OBJECT.value, ProfilerDataTypes.GENDER.value,
                          ProfilerDataTypes.IP.value, ProfilerDataTypes.URL.value,
                          ProfilerDataTypes.EMAIL.value, ProfilerDataTypes.CREDIT_CARD_NUMBER.value,
                          ProfilerDataTypes.ZIP_CODE.value, ProfilerDataTypes.PHONE_NUMBER,
                          ProfilerDataTypes.SOCIAL_SECURITY_NUMBER.value,
                          ProfilerDataTypes.HTTP_CODE.value, ProfilerDataTypes.US_STATE.value]

# Strings and Function Messages
JUST_CHECKING = "Just check that all necessary environments vars are present..."
STARTING_OPTIMUS = "Transform and Roll out..."

SUCCESS = "Optimus successfully imported. Have fun :)."

CONFIDENCE_LEVEL_CONSTANT = [50, .67], [68, .99], [90, 1.64], [95, 1.96], [99, 2.57]


def print_check_point_config(filesystem):
    logger.print(
        "Setting checkpoint folder %s. If you are in a cluster initialize Optimus with master='your_ip' as param",
        filesystem)


# For Google Colab
JAVA_PATH_COLAB = "/usr/lib/jvm/java-8-openjdk-amd64"
RELATIVE_ERROR = 10000

# Buffer size in rows
BUFFER_SIZE = 500000
US_STATES_NAMES = ["alabama",
                   "alaska",
                   "american samoa",
                   "arizona",
                   "arkansas",
                   "california",
                   "colorado",
                   "connecticut",
                   "delaware",
                   "district of columbia",
                   "federated states of micronesia",
                   "florida",
                   "georgia",
                   "guam",
                   "hawaii",
                   "idaho",
                   "illinois",
                   "indiana",
                   "iowa",
                   "kansas",
                   "kentucky",
                   "louisiana",
                   "maine",
                   "marshall islands",
                   "maryland",
                   "massachusetts",
                   "michigan",
                   "minnesota",
                   "mississippi",
                   "missouri",
                   "montana",
                   "nebraska",
                   "nevada",
                   "new hampshire",
                   "new jersey",
                   "new mexico",
                   "new york",
                   "north carolina",
                   "north dakota",
                   "northern mariana islands",
                   "ohio",
                   "oklahoma",
                   "oregon",
                   "palau",
                   "pennsylvania",
                   "puerto rico",
                   "rhode island",
                   "south carolina",
                   "south dakota",
                   "tennessee",
                   "texas",
                   "utah",
                   "vermont",
                   "virgin islands",
                   "virginia",
                   "washington",
                   "west virginia",
                   "wisconsin",
                   "wyoming"
                   ]
US_STATES_CODE = [
    "al",
    "ak",
    "as",
    "az",
    "ar",
    "ca",
    "co",
    "ct",
    "de",
    "dc",
    "fm",
    "fl",
    "ga",
    "gu",
    "hi",
    "id",
    "il",
    "in",
    "ia",
    "ks",
    "ky",
    "la",
    "me",
    "mh",
    "md",
    "ma",
    "mi",
    "mn",
    "ms",
    "mo",
    "mt",
    "ne",
    "nv",
    "nh",
    "nj",
    "nm",
    "ny",
    "nc",
    "nd",
    "mp",
    "oh",
    "ok",
    "or",
    "pw",
    "pa",
    "pr",
    "ri",
    "sc",
    "sd",
    "tn",
    "tx",
    "ut",
    "vt",
    "vi",
    "va",
    "wa",
    "wv",
    "wi",
    "wy"
]
CURRENCIES = {"$": "dollar",
              "¢": "cent",
              "£": "point",
              "€": "euro",
              "¥": "yen",
              "₹": "indian rupee",
              "₽": "ruble",
              "元": "yuan",
              "¤": "currency",
              "₠": "euro-currency",
              "₡": "colon",
              "₢": "cruzeiro",
              "₣": "french franc",
              "₤": "lira",
              "₥": "mill",
              "₦": "naira",
              "₧": "peseta",
              "₨": "rupee",
              "₩": "won",
              "₪": "new shequel",
              "₫": "dong",
              "₭": "kip",
              "₮": "tugrik",
              "₯": "drachma",
              "₰": "german penny",
              "₱": "peso",
              "₲": "guarani",
              "₳": "austral",
              "₴": "hryvnia",
              "₵": "cedi",
              "₶": "livre tournois",
              "₸": "tenge",
              "₺": "turkish lira",
              "₼": "manat",
              "৲": "bengali rupee mark",
              "৳": "bengali rupee sign",
              "૱": "gujarati rupee sign",
              "௹": "tamil rupee sign",
              "฿": "thai currency bath",
              "៛": "khmer currency reil",
              "㍐": "square yuan",
              "円": "yen character",
              "圆": "yen/yuan character variant one",
              "圎": "yen/yuan character variant two",
              "圓": "yuan character, in hong kong and taiwan",
              "圜": "yen/yuan character variant three",
              "원": "won character",
              "﷼": "rial sign",
              "＄": "fullwidth dollar sign",
              "￠": "fullwidth cent sign",
              "￡": "fullwidth pound sign",
              "￥": "fullwidth yen sign",
              "￦": "fullwidth won sign"}
PYTHON_SHORT_TYPES = {"string": "string",
                      "str": "string",
                      "integer": "int",
                      "int": "int",
                      "float": "float",
                      "double": "double",
                      "bool": "boolean",
                      "boolean": "boolean",
                      "array": "array",
                      "null": "null"
                      }
PYTHON_TYPES = {"string": str, "int": int, "float": float, "boolean": bool}
PROFILER_COLUMN_TYPES = {"categorical", "numeric", "date", "null", "array", "binary"}
PYTHON_TO_PROFILER = {"string": "categorical", "boolean": "categorical", "int": "numeric", "float": "numeric",
                      "float": "numeric", "date": "date", "array": "array", "binary": "binary", "null": "null"}

PROFILER_CATEGORICAL_DTYPES = [ProfilerDataTypes.BOOL.value,
                               ProfilerDataTypes.BOOLEAN.value, ProfilerDataTypes.ZIP_CODE.value,
                               ProfilerDataTypes.STRING.value, ProfilerDataTypes.HTTP_CODE.value,
                               ProfilerDataTypes.IP.value, ProfilerDataTypes.GENDER.value,
                               ProfilerDataTypes.PHONE_NUMBER.value, ProfilerDataTypes.US_STATE.value,
                               ProfilerDataTypes.SOCIAL_SECURITY_NUMBER.value
                               ]

PYTHON_DATE_TO_FORMAT = {"%a": "www",
                         "%A": "wwww",
                         "%w": "w",
                         "%d": "d",
                         "%m": "M",
                         "%b": "MMM",
                         "%B": "MMMM",
                         "%y": "yy",
                         "%Y": "yyyy",
                         "%H": "H",
                         "%I": "h",
                         "%p": "mi",
                         "%M": "mm",
                         "%S": "ss",
                         "%f": "yy",
                         "%z": "XXX"}

DATE_FORMAT_ITEMS = [("YYYY", "%Y"),
                     ("YYY", "%y"),
                     ("YY", "%y"),
                     ("Y", "%Y"),
                     ("yyyy", "%Y"),
                     ("yyy", "%y"),
                     ("yy", "%y"),
                     ("y", "%y"),
                     ("MMMMM", "%B"),
                     ("MMMM", "%B"),
                     ("MMM", "%b"),
                     ("MM", "%m"),
                     ("M", "%m"),
                     ("mmmm", "%B"),
                     ("mmm", "%b"),
                     ("mm", "%M"),
                     ("m", "%M"),
                     ('DD', '%d'),
                     ('dd', '%d'),
                     ('D', '%d'),
                     ('d', '%d'),
                     ('HH', '%H'),
                     ('hh', '%I'),
                     ('sss', '%f'),
                     ('ss', '%S'),
                     ('SS', '%S'),
                     ('TT', '%p'),
                     ('tt', '%p'),
                     ('WWWW', '%A'),
                     ('WWW', '%a'),
                     ('WW', '%A'),
                     ('W', '%a'),
                     ('wwww', '%A'),
                     ('www', '%a'),
                     ('ww', '%A'),
                     ('w', '%a'),
                     ('XXX', '%z')]

DATE_FORMAT_ITEMS_MONTH = [("YYYY", "%Y"),
                           ("YYY", "%y"),
                           ("YY", "%y"),
                           ("Y", "%Y"),
                           ("yyyy", "%Y"),
                           ("yyy", "%y"),
                           ("yy", "%y"),
                           ("y", "%y"),
                           ("MMMMM", "%B"),
                           ("MMMM", "%B"),
                           ("MMM", "%b"),
                           ("MM", "%m"),
                           ("M", "%m"),
                           ("mmmm", "%B"),
                           ("mmm", "%b"),
                           ("mm", "%m"),
                           ("m", "%m"),
                           ('DD', '%d'),
                           ('dd', '%d'),
                           ('D', '%d'),
                           ('d', '%d'),
                           ('HH', '%H'),
                           ('hh', '%I'),
                           ('sss', '%f'),
                           ('ss', '%S'),
                           ('SS', '%S'),
                           ('TT', '%p'),
                           ('tt', '%p'),
                           ('WWWW', '%A'),
                           ('WWW', '%a'),
                           ('WW', '%A'),
                           ('W', '%a'),
                           ('wwww', '%A'),
                           ('www', '%a'),
                           ('ww', '%A'),
                           ('w', '%a'),
                           ('XXX', '%z')]

CONTRACTIONS = [
    ("a'ight", "alright"),
    ("ain't", "am not"),
    ("amn't", "am not"),
    ("arencha", "are not you"),
    ("aren't", "are not"),
    ("'bout", "about"),
    ("cannot", "can not"),
    ("can't", "cannot"),
    ("cap'n", "captain"),
    ("'cause", "because"),
    ("'cept", "except"),
    ("could've", "could have"),
    ("couldn't", "could not"),
    ("couldn't've", "could not have"),
    ("dammit", "damn it"),
    ("daren't", "dare not"),
    ("daresn't", "dare not"),
    ("dasn't", "dare not"),
    ("didn't", "did not"),
    ("doesn't", "does not"),
    ("don't", "do not "),
    ("dunno", "do not know"),
    ("d'ye", "do you "),
    ("e'en", "even"),
    ("e'er", "ever"),
    ("'em", "them"),
    ("everybody's", "everybody is"),
    ("everyone's", "everyone is"),
    ("fo'c'sle", "forecastle"),
    ("'gainst", "against"),
    ("g'day", "good day"),
    ("gimme", "give me"),
    ("giv'n", "given"),
    ("gonna", "going to"),
    ("gon't", "go not "),
    ("gotta", "got to"),
    ("hadn't", "had not"),
    ("had've", "had have"),
    ("hasn't", "has not"),
    ("haven't", "have not"),
    ("he'd", "he would"),
    ("he'll", "he will"),
    ("helluva", "hell of a"),
    ("he's", "he is"),
    ("here's", "here is"),
    ("he've", "he have"),
    ("how'd", "how would"),
    ("howdy", "how do you do "),
    ("how'll", "how will"),
    ("how're", "how are"),
    ("how's", "how is "),
    ("I'd", "I would"),
    ("I'd've", "I would have"),
    ("I'll", "I shall / I will"),
    ("I'm", "I am"),
    ("I'm'a", "I am about to"),
    ("I'm'o", "I am going to"),
    ("innit", "is it not"),
    ("I've", "I have"),
    ("isn't", "is not"),
    ("it'd", "it would"),
    ("it'll", "it will"),
    ("it's", "it is"),
    ("iunno", "I don't know"),
    ("kinda", "kind of"),
    ("let's", "let us"),
    ("ma'am", "madam"),
    ("mayn't", "may not"),
    ("may've", "may have"),
    ("methinks", "me thinks"),
    ("mightn't", "might not"),
    ("might've", "might have"),
    ("mustn't", "must not"),
    ("mustn't've", "must not have"),
    ("must've", "must have"),
    ("'neath", "beneath"),
    ("needn't", "need not"),
    ("nal", "and all"),
    ("ne'er", "never"),
    ("o'clock", "of the clock"),
    ("o'er", "over"),
    ("ol'", "old"),
    ("oughtn't", "ought not"),
    ("'round", "around"),
    ("'s", "is"),
    ("shalln't", "shall not"),
    ("shan't", "shall not"),
    ("she'd", "she would"),
    ("she'll", "she will"),
    ("she's", "she is"),
    ("should've", "should have"),
    ("shouldn't", "should not"),
    ("shouldn't've", "should not have"),
    ("somebody's", "somebody is"),
    ("someone's", "someone is"),
    ("something's", "something is"),
    ("so're", "so are"),
    ("so's", "so is"),
    ("so've", "so have"),
    ("that'll", "that will"),
    ("that're", "that are"),
    ("that's", "that is"),
    ("that'd", "that would "),
    ("there'd", "there would"),
    ("there'll", "there will"),
    ("there're", "there are"),
    ("there's", "there is"),
    ("these're", "these are"),
    ("these've", "these have"),
    ("they'd", "they would"),
    ("they'll", "they will"),
    ("they're", "they are "),
    ("they've", "they have"),
    ("this's", "this is"),
    ("those're", "those are"),
    ("those've", "those have"),
    ("'thout", "without"),
    ("'til", "until"),
    ("'tis", "it is"),
    ("to've", "to have"),
    ("'twas", "it was"),
    ("'tween", "between"),
    ("'twere", "it were"),
    ("wanna", "want to"),
    ("wasn't", "was not"),
    ("we'd", "we would"),
    ("we'd've", "we would have"),
    ("we'll", "we will"),
    ("we're", "we are"),
    ("we've", "we have"),
    ("weren't", "were not"),
    ("whatcha", "what are you"),
    ("what'd", "what did"),
    ("what'll", "what will"),
    ("what're", "what are"),
    ("what's", "what is"),
    ("what've", "what have"),
    ("when's", "when is"),
    ("where'd", "where did"),
    ("where'll", "where will"),
    ("where're", "where are"),
    ("where's", "where is "),
    ("where've", "where have"),
    ("which'd", "which would"),
    ("which'll", "which will"),
    ("which're", "which are"),
    ("which's", "which is"),
    ("which've", "which have"),
    ("who'd", "who would "),
    ("who'd've", "who would have"),
    ("who'll", "who will"),
    ("who're", "who are"),
    ("who's", "who is "),
    ("who've", "who have"),
    ("why'd", "why did"),
    ("why're", "why are"),
    ("why's", "why is "),
    ("willn't", "will not"),
    ("won't", "will not"),
    ("wonnot", "will not "),
    ("would've", "would have"),
    ("wouldn't", "would not"),
    ("wouldn't've", "would not have"),
    ("y'all", "you all"),
    ("y'all'd've", "you all would have"),
    ("y'all'd'n've", "you all would not have "),
    ("y'all're", "you all are "),
    ("y'at", "you at"),
    ("yes'm", "yes madam"),
    ("yes ma'am", "yes madam"),
    ("yessir", "yes sir"),
    ("you'd", "you would"),
    ("you'll", "you will"),
    ("you're", "you are"),
    ("you've", "you have")
]
