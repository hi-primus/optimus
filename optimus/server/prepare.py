from optimus.helpers.core import val_to_list
import sys
sys.path.append('../optimus')


alias_map_list = [

    # engine methods

    [["optimus", "engine", "createEngine"], "Optimus"],

    # op methods

    [["createDataframe", "dataframe"], "create.dataframe"],

    # connection methods

    ["connectMYSQL", "connect.mysql"],
    ["connectPostgres", "connect.postgres"],
    ["connectMSSQL", "connect.mssql"],
    ["connectRedshift", "connect.redshift"],
    ["connectSQLite", "connect.sqlite"],
    ["connectBigQuery", "connect.bigquery"],
    ["connectPresto", "connect.presto"],
    ["connectCassandra", "connect.cassandra"],
    ["connectRedis", "connect.redis"],
    ["connectOracle", "connect.oracle"],
    ["connectS3", "connect.s3"],
    ["connectLocal", "connect.local"],
    ["connectHDFS", "connect.hdfs"],
    ["connectGCS", "connect.gcs"],
    ["connectMAS", "connect.mas"],

    # string clustering methods

    ["setSuggestion", "set_suggestion"],
    ["setSuggestions", "set_suggestions"],

    # save methods

    ["saveFile", "save.file"],
    ["saveCSV", "save.csv"],
    ["saveXML", "save.xml"],
    ["saveJSON", "save.json"],
    ["saveExcel", "save.excel"],
    ["saveAvro", "save.avro"],
    ["saveParquet", "save.parquet"],
    ["saveORC", "save.orc"],
    ["saveHDF5", "save.hdf5"],

    # load methods

    ["loadFile", "load.file"],
    ["loadCSV", {"operation": "load.file", "file_type": "csv"}],
    ["loadXML", {"operation": "load.file", "file_type": "xml"}],
    ["loadJSON", {"operation": "load.file", "file_type": "json"}],
    ["loadExcel", {"operation": "load.file", "file_type": "excel"}],
    ["loadAvro", {"operation": "load.file", "file_type": "avro"}],
    ["loadParquet", {"operation": "load.file", "file_type": "parquet"}],
    ["loadORC", {"operation": "load.file", "file_type": "orc"}],
    ["loadZIP", {"operation": "load.file", "file_type": "zip"}],
    ["loadHDF5", {"operation": "load.file", "file_type": "hdf5"}],

    # methods

    ["profile", "profile"],
    [["profileColumns"], "profile.columns"],
    [["profileStats"], "profile.stats"],
    [["display", "toDict", "toObject"], "to_dict"],
    ["toJSON", "to_json"],
    ["size", "cols.size"],
    ["optimize", "cols.optimize"],
    ["run", "cols.run"],
    ["query", "cols.query"],
    ["partitions", "cols.partitions"],
    ["repartition", "cols.repartition"],
    [["print"], "ascii"],
    ["reset", "cols.reset"],
    [["agg", "aggregate", "groupyby", "groupBy"], "agg"],
    ["report", "cols.report"],
    [["selectColumns", "select"], "cols.select"],
    [["copyColumns", "copy"], "cols.copy"],
    [["dropColumns", "drop"], "cols.drop"],
    [["keepColumns", "keep"], "cols.keep"],
    [["renameColumns"], "cols.rename"],
    [["names", "columnNames", "columns"], "cols.names"],
    [["datatype", "dtype", "data_type", "datatypes", "dtypes", "data_types"], "cols.data_type"],
    [["patterCounts", "pattern_counts"], "cols.pattern_counts"],
    ["mad", "cols.mad"],
    ["min", "cols.min"],
    ["max", "cols.max"],
    ["mode", "cols.mode"],
    ["range", "cols.range"],
    ["percentile", "cols.percentile"],
    ["median", "cols.median"],
    ["kurtosis", "cols.kurtosis"],
    ["skew", "cols.skew"],
    ["mean", "cols.mean"],
    ["sum", "cols.sum"],
    ["cumsum", "cols.cumsum"],
    ["cumprod", "cols.cumprod"],
    ["cummax", "cols.cummax"],
    ["cummin", "cols.cummin"],
    ["var", "cols.var"],
    ["std", "cols.std"],
    ["iqr", "cols.iqr"],
    [["countNA", "count_na"], "cols.count_na"],
    ["unique", "cols.unique"],
    [["countUniques", "count_uniques"], "cols.count_uniques"],
    ["heatmap", "cols.heatmap"],
    ["hist", "cols.hist"],
    [["countMismatch", "count_mismatch"], "cols.count_mismatch"],
    ["quality", "cols.quality"],
    [
        ["inferProfilerDtypes", "infer_data_types"],
        "cols.infer_data_types",
    ],
    ["frequency", "cols.frequency"],
    ["boxplot", "cols.boxplot"],
    [["countZeros", "count_zeros"], "cols.count_zeros"],
    [["countRows", "length", "len"], "rows.count"],
    [["approxCount", "approx_count"], "rows.approx_count"],
    ["sample", "sample"],
    [["stratifiedSample", "stratified_sample"], "stratified_sample"],
    [["wordCount", "word_count", "cols.word_count"], "cols.word_count"],
    [["toTimestamp", "to_timestamp", "cols.to_timestamp"], "cols.to_timestamp"],
    [["setColumn", "set"], "cols.set"],
    [["castColumn", "cast"], "cols.cast"],
    [
        ["setDatatype", "setDtype", "set_data_type", "cols.set_data_type"],
        "cols.set_data_type",
    ],
    [
        ["unsetDatatype", "unsetDtype", "unset_data_type", "cols.unset_data_type"],
        "cols.unset_data_type",
    ],
    ["replace", "cols.replace"],
    [["replaceRegex", "replace_regex"], "cols.replace_regex"],
    ["pattern", "cols.pattern"],
    ["join", "cols.join"],
    ["move", "cols.move"],
    ["sort", "cols.sort"],
    ["item", "cols.item"],
    ["get", "cols.get"],
    ["abs", "cols.abs"],
    ["exp", "cols.exp"],
    ["mod", "cols.mod"],
    ["log", "cols.log"],
    ["ln", "cols.ln"],
    ["pow", "cols.pow"],
    ["sqrt", "cols.sqrt"],
    ["reciprocal", "cols.reciprocal"],
    ["round", "cols.round"],
    ["floor", "cols.floor"],
    ["ceil", "cols.ceil"],
    ["sin", "cols.sin"],
    ["cos", "cols.cos"],
    ["tan", "cols.tan"],
    ["asin", "cols.asin"],
    ["acos", "cols.acos"],
    ["atan", "cols.atan"],
    ["sinh", "cols.sinh"],
    ["cosh", "cols.cosh"],
    ["tanh", "cols.tanh"],
    ["asinh", "cols.asinh"],
    ["acosh", "cols.acosh"],
    ["atanh", "cols.atanh"],
    ["extract", "cols.extract"],
    ["slice", "cols.slice"],
    ["left", "cols.left"],
    ["right", "cols.right"],
    ["mid", "cols.mid"],
    [["toFloat", "to_float"], "cols.to_float"],
    [["toInteger", "to_integer"], "cols.to_integer"],
    [["toBoolean", "to_boolean"], "cols.to_boolean"],
    [["toString", "to_string"], "cols.to_string"],
    ["matchRegex", "cols.match"],
    [["lowercase", "lower"], "cols.lower"],
    [["uppercase", "upper"], "cols.upper"],
    [["propercase", "proper"], "cols.proper"],
    ["title", "cols.title"],
    ["capitalize", "cols.capitalize"],
    ["pad", "cols.pad"],
    ["trim", "cols.trim"],
    [["stripHtml", "strip_html"], "cols.strip_html"],
    [["dateFormat", "date_format"], "cols.date_format"],
    [["formatDate", "format_date"], "cols.format_date"],
    [["wordTokenize", "word_tokenize"], "cols.word_tokenize"],
    [["wordCount", "word_count"], "cols.word_count"],
    ["len", "cols.len"],
    ["reverseString", "cols.reverse"],
    ["remove", "cols.remove"],
    [
        ["normalizeCharacters", "normalizeChars", "normalize_chars"],
        "cols.normalize_chars",
    ],
    [["removeNumbers", "remove_numbers"], "cols.remove_numbers"],
    [["removeWhiteSpaces", "remove_white_spaces"], "cols.remove_white_spaces"],
    [["removeStopWords", "remove_stopwords"], "cols.remove_stopwords"],
    [["removeUrls", "remove_urls"], "cols.remove_urls"],
    [["normalizeSpaces", "normalize_spaces"], "cols.normalize_spaces"],
    [["removeSpecialChars", "remove_special_chars"], "cols.remove_special_chars"],
    [["toDatetime", "to_datetime"], "cols.to_datetime"],
    ["year", "cols.year"],
    ["month", "cols.month"],
    ["day", "cols.day"],
    ["hour", "cols.hour"],
    ["minute", "cols.minute"],
    ["second", "cols.second"],
    ["weekday", "cols.weekday"],
    [["yearsBetween", "years_between"], "cols.years_between"],
    [["lemmatizeVerb", "lemmatize_verb"], "cols.lemmatize_verb"],
    ["impute", "cols.impute"],
    [["fillNa", "fill_na"], "cols.fill_na"],
    ["countCols", "cols.count"],
    ["add", "cols.add"],
    ["sub", "cols.sub"],
    ["mul", "cols.mul"],
    ["div", "cols.div"],
    [["zScore", "z_score"], "cols.z_score"],
    [["modifiedZScore", "modified_z_score"], "cols.modified_z_score"],
    [["minMaxScaler", "min_max_scaler"], "cols.min_max_scaler"],
    [["standardScaler", "standard_scaler"], "cols.standard_scaler"],
    [["maxAbsScaler", "max_abs_scaler"], "cols.max_abs_scaler"],
    ["nest", "cols.nest"],
    ["unnest", "cols.unnest"],
    ["qcut", "cols.qcut"],
    ["cut", "cols.cut"],
    ["clip", "cols.clip"],
    [["stringToIndex", "string_to_index"], "cols.string_to_index"],
    [["indexToString", "index_to_string"], "cols.index_to_string"],
    [["topDomain", "top_domain"], "cols.top_domain"],
    ["domain", "cols.domain"],
    [["URLScheme", "url_scheme"], "cols.url_scheme"],
    [["URLParams", "url_params"], "cols.url_params"],
    [["URLPath", "url_path"], "cols.url_path"],
    ["port", "cols.port"],
    [["URLQuery", "url_query"], "cols.url_query"],
    ["subdomain", "cols.subdomain"],
    [["emailUsername", "email_username"], "cols.email_username"],
    [["emailDomain", "email_domain"], "cols.email_domain"],
    ["missing", "cols.missing"],
    ["fingerprint", "cols.fingerprint"],
    [["nGramFingerprint", "n_gram_fingerprint"], "cols.n_gram_fingerprint"],
    ["metaphone", "cols.metaphone"],
    ["nysiis", "cols.nysiis"],
    [["matchRatingCodex", "match_rating_codex"], "cols.match_rating_codex"],
    [["doubleMethaphone", "double_metaphone"], "cols.double_metaphone"],
    ["soundex", "cols.soundex"],
    ["append", "rows.append"],
    [["greaterThan", "greater_than"], "rows.greater_than"],
    [["greaterThanEqual", "greater_than_equal"], "rows.greater_than_equal"],
    [["lessThan", "less_than"], "rows.less_than"],
    [["lessThanEqual", "less_than_equal"], "rows.less_than_equal"],
    ["equal", "rows.equal"],
    [["notEqual", "not_equal"], "rows.not_equal"],
    ["missing", "rows.missing"],
    ["mismatch", "rows.mismatch"],
    ["match", "rows.match"],
    ["find", "rows.find"],
    ["selectRows", "rows.select"],
    ["sortRows", "rows.sort"],
    ["reverse", "rows.reverse"],
    ["dropRows", "rows.drop"],
    ["between", "rows.between"],
    [["dropNA", "drop_na"], "rows.drop_missings"],
    [["dropDuplicated", "drop_duplicated"], "rows.drop_duplicated"],
    ["limit", "rows.limit"],
    [["isIn", "is_in"], "rows.is_in"]
]

alias_map = {}

for alias_group, value in alias_map_list:
    for alias in val_to_list(alias_group):
        alias_map.update({alias: value})


def use_alias(body: dict):

    if body["operation"] in alias_map:
        found = alias_map[body["operation"]]
        if isinstance(found, str):
            body["operation"] = found
        else:
            body.update(found)

    return body


def default_to_engine(body: dict):

    if (not "operation" in body) and ("engine" in body):
        body["operation"] = "Optimus"

    return body


def default_to_display(body: dict):

    if (not "operation" in body) and ("source" in body):
        body["operation"] = "to_dict"

    return body


preparers = [
    default_to_engine,
    default_to_display,
    use_alias
]


def prepare(body: dict):
    for preparer in preparers:
        body = preparer(body)

    return body
