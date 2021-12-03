import json

from rply import LexerGenerator

functions = {
    # Aggregations
    "MIN": {
        "description": "Returns the minimum value in a numeric dataset.",
        "parameters": [
            {
                "type": ["column", "number"],
                "name": "col_name",
                "description": "Column name or value."
            },
        ],
        "example": "MIN(COL_NAME)",
        "text": "MIN",
    },
    "MAX": {
        "description": "Returns the maximum value in a numeric dataset.",
        "parameters": [
            {
                "type": ["column", "number"],
                "name": "col_name",
                "description": "Column name or value."
            },
        ],
        "example": "MIN(COL_NAME)",
        "text": "MAX",
    },
    "MEAN": {
        "description": "Returns the numerical average value in a dataset, ignoring text.",
        "parameters": [
            {
                "type": ["column", "number"],
                "name": "col_name",
                "description": "Column name or value."
            },
        ],
        "example": "MEAN(COL_NAME)",
        "text": "MEAN",
    },
    "MODE": {
        "description": "Returns the most commonly occurring value in a dataset.",
        "parameters": [
            {
                "type": ["column", "number"],
                "name": "col_name",
                "description": "Column name or value."
            },
        ],
        "example": "MODE(COL_NAME)",
        "text": "MODE",
    },
    "STD": {
        "description": "Calculates the standard deviation based on a sample.",
        "parameters": [
            {
                "type": ["column", "number"],
                "name": "col_name",
                "description": "Column name or value."
            },
        ],
        "example": "STD(COL_NAME)",
        "text": "STD",
    },
    "SUM": {
        "description": "Returns the sum of a series of numbers and/or cells.",
        "parameters": [
            {
                "type": ["column", "number"],
                "name": "col_name",
                "description": "Column name or value."
            },
        ],
        "example": "SUM(COL_NAME)",
        "text": "SUM",
    },
    "VAR": {
        "description": "Calculates the variance based on a sample.",
        "parameters": [
            {
                "type": ["column", "number"],
                "name": "col_name",
                "description": "Column name or value."
            },
        ],
        "example": "VAR(COL_NAME)",
        "text": "VAR",
    },
    "KURTOSIS": {
        "description": "Calculates the kurtosis of a dataset, which describes the shape, and in particular the 'peakedness' of that dataset.",
        "parameters": [
            {
                "type": ["column", "number"],
                "name": "col_name",
                "description": "Column name or value."
            },
        ],
        "example": "KURT(COL_NAME)",
        "text": "KURT",
    },
    "SKEW": {
        "description": "Calculates the skewness of a dataset, which describes the symmetry of that dataset about the mean.",
        "parameters": [
            {
                "type": ["column", "number"],
                "name": "col_name",
                "description": "Column name or value."
            },
        ],
        "example": "SKEW(COL_NAME)",
        "text": "SKEW",
    },
    "MAD": {
        "description": "Calculates the skewness of a dataset, which describes the symmetry of that dataset about the mean.",
        "parameters": [
            {
                "type": ["column", "number"],
                "name": "col_name",
                "description": "Column name or value"
            },
        ],
        "example": "MAD(COL_NAME)",
        "text": "MAD",
    },
    # Math functions
    "ABS": {
        "description": "Returns the absolute value of a number.",
        "parameters": [
            {
                "type": ["column", "number"],
                "name": "col_name",
                "description": "The number of which to return the absolute value."
            },
        ],
        "example": "ABS(-2)",
        "text": "ABS",
    },

    "MOD": {
        "description": "Returns the result of the modulo operator, the remainder after a division operation.",
        "parameters": [
            {
                "type": ["column", "number"],
                "name": "col_name",
                "description": "The number to be divided to find the remainder."
            },
            {
                "type": "number",
                "name": "divisor",
                "description": "The number to divide by."
            }
        ],
        "example": "MOD(10, 4)",
        "text": "MOD",
    },

    "EXP": {
        "description": "Returns Euler's number, e (~2.718) raised to a power.",
        "parameters": [
            {
                "type": ["column", "number"],
                "name": "col_name",
                "description": "The number of which to return the absolute value."
            },
        ],
        "example": "ABS(-2)",
        "text": "EXP",
    },
    "LOG": {
        "description": "Returns the logarithm of a number with respect to a base.",
        "parameters": [
            {
                "type": ["column", "number"],
                "name": "col_name",
                "description": "The value for which to calculate the logarithm."
            },
        ],
        "example": "LOG(128, 2)",
        "text": "LOG",
    },
    "LN": {
        "description": "Returns the logarithm of a number, base e (Euler's number).",
        "parameters": [
            {
                "type": ["column", "number"],
                "name": "col_name",
                "description": "The value for which to calculate the logarithm, base e."
            },
        ],
        "example": "LN(100)",
        "text": "LN",
    },
    "POW": {
        "description": "Returns a number raised to a power.",
        "parameters": [
            {
                "type": ["column", "number"],
                "name": "col_name",
                "description": "The number to raise to the exponent power."
            },
            {
                "type": "number",
                "name": "exponent",
                "description": "The exponent to raise base to."
            }
        ],
        "example": "POW(4, 0.5)",
        "text": "POW",
    },
    "CEIL": {
        "description": "Rounds a number up to the nearest integer multiple of specified significance factor.",
        "parameters": [
            {
                "type": ["column", "number"],
                "name": "col_name",
                "description": "The value to round up to the nearest integer multiple of factor."
            },

        ],
        "example": "CEILING(23.25)",
        "text": "CEIL",
    },
    "SQRT": {
        "description": "Returns the positive square root of a positive number.",
        "parameters": [
            {
                "type": ["column", "number"],
                "name": "col_name",
                "description": "The number for which to calculate the positive square root."
            },

        ],
        "example": "SQRT(9)",
        "text": "SQRT",
    },
    "FLOOR": {
        "description": "Rounds a number down to the nearest integer multiple of specified significance factor.",
        "parameters": [
            {
                "type": ["column", "number"],
                "name": "col_name",
                "description": "The value to round down to the nearest integer multiple of factor."
            },

        ],
        "example": "FLOOR(23.25)",
        "text": "FLOOR",
    },
    "ROUND": {
        "description": "Rounds a number to a certain number of decimal places according to standard rules.",
        "parameters": [
            {
                "type": ["column", "number"],
                "name": "col_name",
                "description": "The value to round to places number of places."
            },

            {
                "type": "number",
                "name": "decimals",
                "description": "The value to round to places number of places."
            }

        ],
        "example": "ROUND(99.44, 1)",
        "text": "ROUND",
    },
    "SIN": {
        "description": "Returns the sin of an angle provided in radians.",
        "parameters": [
            {
                "type": ["column", "number"],
                "name": "col_name",
                "description": "The angle to find the sine of, in radians."
            },

        ],
        "example": "SIN(3.14)",
        "text": "SIN",
    },
    "COS": {
        "description": "Returns the cosine of an angle provided in radians.",
        "parameters": [
            {
                "type": ["column", "number"],
                "name": "col_name",
                "description": "The angle to find the cosine of, in radians."
            },

        ],
        "example": "COS(3.14)",
        "text": "COS",
    },
    "TAN": {
        "description": "Returns the tangent of an angle provided in radians.",
        "parameters": [
            {
                "type": ["column", "number"],
                "name": "col_name",
                "description": "The angle to find the tangent of, in radians."
            },

        ],
        "example": "TAN(3.14)",
        "text": "TAN",
    },
    "ASIN": {
        "description": "Returns the inverse sine of a value, in radians.",
        "parameters": [
            {
                "type": ["column", "number"],
                "name": "col_name",
                "description": "The value for which to calculate the inverse sine. Must be between -1 and 1, inclusive."
            },

        ],
        "example": "ASIN(0)",
        "text": "ASIN",
    },
    "ACOS": {
        "description": "Returns the inverse cosine of a value, in radians.",
        "parameters": [
            {
                "type": ["column", "number"],
                "name": "col_name",
                "description": "The value for which to calculate the inverse cosine. Must be between -1 and 1, inclusive."
            },

        ],
        "example": "ACOS(0)",
        "text": "ACOS",
    },
    "ATAN": {
        "description": "Returns the inverse tangent of a value, in radians.",
        "parameters": [
            {
                "type": ["column", "number"],
                "name": "col_name",
                "description": "The value for which to calculate the inverse tangent."
            },

        ],
        "example": "ATAN(0)",
        "text": "ATAN",
    },
    "SINH": {
        "description": "Returns the hyperbolic sine of any real number.",
        "parameters": [
            {
                "type": ["column", "number"],
                "name": "col_name",
                "description": "Any real value to calculate the hyperbolic sine of."
            },

        ],
        "example": "SINH(2)",
        "text": "SINH",
    },
    "COSH": {
        "description": "Returns the hyperbolic cosine of any real number.",
        "parameters": [
            {
                "type": ["column", "number"],
                "name": "col_name",
                "description": "Any real value to calculate the hyperbolic cosine of."
            },

        ],
        "example": "COSH(0.48)",
        "text": "COSH",
    },

    "TANH": {
        "description": "Returns the hyperbolic tangent of any real number.",
        "parameters": [
            {
                "type": ["column", "number"],
                "name": "col_name",
                "description": "Any real value to calculate the hyperbolic tangent of."
            },

        ],
        "example": "TANH(1)",
        "text": "TANH",
    },
    "ASINH": {
        "description": "Returns the hyperbolic tangent of any real number.",
        "parameters": [
            {
                "type": ["column", "number"],
                "name": "col_name",
                "description": "The value for which to calculate the inverse hyperbolic sine."
            },

        ],
        "example": "ASINH(0.9)",
        "text": "ASINH",
    },

    "ACOSH": {
        "description": "Returns the inverse hyperbolic cosine of a number.",
        "parameters": [
            {
                "type": ["column", "number"],
                "name": "col_name",
                "description": "The value for which to calculate the inverse hyperbolic cosine. Must be greater than or equal to 1."
            },

        ],
        "example": "ACOSH(2)",
        "text": "ACOSH",
    },
    "ATANH": {
        "description": "Returns the inverse hyperbolic tangent of a number.",
        "parameters": [
            {
                "type": ["column", "number"],
                "name": "col_name",
                "description": "The value for which to calculate the inverse hyperbolic tangent. Must be between -1 and 1, exclusive."
            },

        ],
        "example": "ATANH(0.9)",
        "text": "ATANH",
    },
    "UPPER": {  # String Transformations
        "description": "Returns the inverse hyperbolic tangent of a number.",
        "parameters": [
            {
                "type": ["column", "string"],
                "name": "col_name",
                "description": "Converts a specified string to uppercase."
            },

        ],
        "example": "UPPER('lorem ipsum')",
        "text": "UPPER",
    },
    "LOWER": {
        "description": "Converts a specified string to lowercase.",
        "parameters": [
            {
                "type": ["column", "string"],
                "name": "col_name",
                "description": "The string to convert to lowercase."
            },

        ],
        "example": "LOWER('LOREM IPSUM')",
        "text": "LOWER",
    },
    "PROPER": {
        "description": "Capitalizes each word in a specified string.",
        "parameters": [
            {
                "type": ["column", "string"],
                "name": "col_name",
                "description": "The text which will be returned with the first letter of each word in uppercase and all other letters in lowercase."
            },

        ],
        "example": "PROPER('optimus prime')",
        "text": "PROPER",
    },
    "TRIM": {
        "description": "Removes leading, trailing, and repeated spaces in text.",
        "parameters": [
            {
                "type": ["column", "string"],
                "name": "col_name",
                "description": "The text or reference to a cell containing text to be trimmed."
            },

        ],
        "example": "TRIM('optimus prime')",
        "text": "TRIM",
    },
    "LEN": {
        "description": "Returns the length of a string.",
        "parameters": [
            {
                "type": ["column", "string"],
                "name": "col_name",
                "description": "The string whose length will be returned."
            },

        ],
        "example": "LEN('optimus prime')",
        "text": "LEN",
    },
    "FIND": {
        "description": "Returns the position at which a string is first found.",
        "parameters": [
            {
                "type": ["column", "string"],
                "name": "col_name",
                "description": "The string to look for."
            },
            {
                "type": "string",
                "name": "text_to_search",
                "description": "The text to search for the first occurrence of search_for."
            }

        ],
        "example": "LEN('optimus prime')",
        "text": "LEN",
    },
    "RFIND": "RFIND",
    "LEFT": {
        "description": "Returns a substring from the beginning of a specified string.",
        "parameters": [
            {
                "type": ["column", "string"],
                "name": "col_name",
                "description": "The string from which the left portion will be returned."
            },
        ],
        "example": "LEFT('Optimus', 2)",
        "text": "LEN",
    },
    "RIGHT": {
        "description": "Returns a substring from the end of a specified string.",
        "parameters": [
            {
                "type": ["column", "string"],
                "name": "col_name",
                "description": "The string from which the right portion will be returned."
            },
        ],
        "example": "LEN('optimus prime')",
        "text": "LEN",
    },
    "STARTS_WITH": "STARTS_WITH",
    "ENDS_WITH": "ENDS_WITH",
    "EXACT": "EXACT",
    "YEAR": {  # Date Functions
        "description": "Returns the year specified by a given date.",
        "parameters": [
            {
                "type": ["column", "string"],
                "name": "col_name",
                "description": "The date from which to extract the year."
            },
        ],
        "example": "YEAR()",
        "text": "YEAR",
    },
    "MONTH": {
        "description": "Returns the month of the year a specific date falls in, in numeric format.",
        "parameters": [
            {
                "type": ["column", "string"],
                "name": "col_name",
                "description": "The date from which to extract the month."
            },
        ],
        "example": "MONTH()",
        "text": "MONTH",
    },
    "DAY": {
        "description": "Returns the day of the month that a specific date falls on, in numeric format.",
        "parameters": [
            {
                "type": ["column", "string"],
                "name": "col_name",
                "description": "The date from which to extract the day."
            },
        ],
        "example": 'DAY()',
        "text": "DAY",
    },
    "HOUR": {
        "description": "Returns the hour component of a specific time, in numeric format.",
        "parameters": [
            {
                "type": ["column", "string"],
                "name": "col_name",
                "description": "The time from which to calculate the hour value."
            },
        ],
        "example": 'HOUR()',
        "text": "HOUR",
    },
    "MINUTE": {
        "description": "Returns the minute component of a specific time, in numeric format.",
        "parameters": [
            {
                "type": ["column", "string"],
                "name": "col_name",
                "description": "The time from which to calculate the minute value."
            },
        ],
        "example": 'MINUTE()',
        "text": "MINUTE",
    },
    "SECOND": {
        "description": "Returns the second component of a specific time, in numeric format.",
        "parameters": [
            {
                "type": ["column", "string"],
                "name": "col_name",
                "description": "The time from which to calculate the second value"
            },
        ],
        "example": 'SECOND()',
        "text": "SECOND",
    },

}
unary_operators = {
    "~": "~ function description",
    "|": "| function description",
    "&": "& function description",
    "+": "+ function description",
    "-": "- function description",

}
binary_operators = {
    "+": "+ function description",
    "-": "- function description",
    "*": "* function description",
    "/": "/ function description",
    "%": "- function description"
}

# functions = op_functions
reserved_words = json.dumps(
    {"functions": functions, "operators": {"unary": unary_operators, "binary": binary_operators}}, ensure_ascii=False)

"""
Parse an expression to optimus code
"""

lexer_generator = LexerGenerator()
lexer = lexer_generator.build()

l_g = lexer_generator

for f in functions:
    rx = f'(?i){f}(?!\w)'
    l_g.add(f, rx)

# Parenthesis
l_g.add('OPEN_PAREN', r'\(')
l_g.add('CLOSE_PAREN', r'\)')

# Semi Colon
l_g.add('SEMI_COLON', r'\;')
l_g.add('COMMA', r'\,')

l_g.add('IDENTIFIER', r'{(.*?)}')  # Reference
l_g.add('IDENTIFIER', r'[^\W0-9 ]\w*')  # Column names
l_g.add('STRINGS', r'"(.*?)"')  # Reference

# Operators
l_g.add('SUM_OPERATOR', r'\+')
l_g.add('SUB_OPERATOR', r'\-')
l_g.add('MUL_OPERATOR', r'\*')
l_g.add('DIV_OPERATOR', r'\/')

# Number
l_g.add('FLOAT', r'[-+]?[0-9]*\.?[0-9]+')
l_g.add('INTEGER', r'\d+')

# Ignore spaces
l_g.ignore('\s+')


def parse(text_input, df_name="df"):
    """

    :param text_input: string to be parsed
    :param df_name: dataframe name

    :return:
    """
    tokens = lexer.lex(text_input)
    result = []
    for token in tokens:
        token_value = token.value
        if token.name == "IDENTIFIER":
            for r in (("{", ""), ("}", "")):
                token_value = token_value.replace(*r)

            result_element = f"""{df_name}.data['{token_value}']"""
        elif token.name in functions:
            result_element = f"""{df_name}.functions.{token_value.lower()}"""
        else:
            result_element = token_value
        result.append(result_element)
    result = "".join(result)
    return result
