from rply import LexerGenerator

op_functions = {
    # Math functions
    "MOD": {
        "description": "Returns the result of the modulo operator, the remainder after a division operation.",
        "parameters": [
            {
                "type": "series",
                "name": "series",
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
    "ABS": {
        "description": "Returns the absolute value of a number.",
        "parameters": [
            {
                "type": "series",
                "name": "series",
                "description": "The number of which to return the absolute value."
            },
        ],
        "example": "ABS(-2)",
        "text": "ABS",
    },

    "EXP": {
        "description": "Returns Euler's number, e (~2.718) raised to a power.",
        "parameters": [
            {
                "type": "series",
                "name": "series",
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
                "type": "series",
                "name": "series",
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
                "type": "series",
                "name": "series",
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
                "type": "series",
                "name": "series",
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
                "type": "series",
                "name": "series",
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
                "type": "series",
                "name": "series",
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
                "type": "series",
                "name": "series",
                "description": "The value to round down to the nearest integer multiple of factor."
            },

        ],
        "example": "FLOOR(23.25)",
        "text": "FLOOR",
    },
    # "TRUNC": {
    #     "description": "Truncates a number to a certain number of significant digits by omitting less significant digits.",
    #     "parameters": [
    #         {
    #             "type": "series",
    #             "name": "series",
    #             "description": "The value to round down to the nearest integer multiple of factor."
    #         },
    #
    #     ],
    #     "example": "FLOOR(23.25)",
    #     "text": "TRUNC",
    # },
    # "RADIANS": "RADIANS function description",  # Trigonometric Functions
    # "DEGREES": "DEGREES function description",
    "SIN": {
        "description": "Returns the sin of an angle provided in radians.",
        "parameters": [
            {
                "type": "series",
                "name": "series",
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
                "type": "series",
                "name": "series",
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
                "type": "series",
                "name": "series",
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
                "type": "series",
                "name": "series",
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
                "type": "series",
                "name": "series",
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
                "type": "series",
                "name": "series",
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
                "type": "series",
                "name": "series",
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
                "type": "series",
                "name": "series",
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
                "type": "series",
                "name": "series",
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
                "type": "series",
                "name": "series",
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
                "type": "series",
                "name": "series",
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
                "type": "series",
                "name": "series",
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
                "type": "series",
                "name": "series",
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
                "type": "series",
                "name": "series",
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
                "type": "series",
                "name": "series",
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
                "type": "series",
                "name": "series",
                "description": "The text or reference to a cell containing text to be trimmed."
            },

        ],
        "example": "TRIM('optimus prime')",
        "text": "TRIM",
    },
    "REMOVE": "ATANH function description",
    "LEN": {
        "description": "Returns the length of a string.",
        "parameters": [
            {
                "type": "series",
                "name": "series",
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
                "type": "series",
                "name": "series",
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
    "RFIND": "ATANH function description",
    "LEFT": "ATANH function description",
    "RIGHT": "ATANH function description",
    "STARTS_WITH": "ATANH function description",
    "ENDS_WITH": "ATANH function description",
    "EXACT": "ATANH function description",
    "YEAR": {                                   # Date Functions
        "description": "Returns the year specified by a given date.",
        "parameters": [
            {
                "type": "series",
                "name": "series",
                "description": "The date from which to extract the year."
            },
        ],
        "example": "YEAR('7/20/1969')",
        "text": "YEAR",
    },
    "MONTH": {
        "description": "Returns the month of the year a specific date falls in, in numeric format.",
        "parameters": [
            {
                "type": "series",
                "name": "series",
                "description": "The date from which to extract the month."
            },
        ],
        "example": "MONTH('7/20/1969')",
        "text": "MONTH",
    },
    "DAY": {
        "description": "Returns the day of the month that a specific date falls on, in numeric format.",
        "parameters": [
            {
                "type": "series",
                "name": "series",
                "description": "The date from which to extract the day."
            },
        ],
        "example": 'DAY("7/20/1969")',
        "text": "DAY",
    },
    "HOUR": {
        "description": "Returns the hour component of a specific time, in numeric format.",
        "parameters": [
            {
                "type": "series",
                "name": "series",
                "description": "The time from which to calculate the hour value."
            },
        ],
        "example": 'DAY("7/20/1969")',
        "text": "DAY",
    },
    "MINUTE": {
        "description": "Returns the minute component of a specific time, in numeric format.",
        "parameters": [
            {
                "type": "series",
                "name": "series",
                "description": "The time from which to calculate the minute value."
            },
        ],
        "example": 'MINUTE("11:40:59 AM")',
        "text": "MINUTE",
    },
    "SECOND": {
        "description": "Returns the second component of a specific time, in numeric format.",
        "parameters": [
            {
                "type": "series",
                "name": "series",
                "description": "The time from which to calculate the second value"
            },
        ],
        "example": 'SECOND("11:40:59 AM")',
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
reserved_words = {"functions": op_functions, "operators": {"unary": unary_operators, "binary": binary_operators}}


class Parser:
    """
    Parse an expression to optimus code
    """

    def __init__(self):
        self.lexer_generator = LexerGenerator()
        self._add_tokens()
        self.lexer = self.lexer_generator.build()

    def _add_tokens(self):
        l_g = self.lexer_generator

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
        l_g.add('IDENTIFIER', "[^\W0-9 ]\w*")  # Column names
        l_g.add('STRINGS', r'"(.*?)"')  # Reference

        # Operators
        l_g.add('SUM', r'\+')
        l_g.add('SUB', r'\-')
        l_g.add('MUL', r'\*')
        l_g.add('DIV', r'\/')

        # Number
        l_g.add('NUMBER', r'\d+')

        # Ignore spaces
        l_g.ignore('\s+')

    def parse(self, text_input):
        """

        :param text_input:
        :return:
        """
        tokens = self.lexer.lex(text_input)

        result = []
        for token in tokens:
            if token.name == "IDENTIFIER":
                t = token.value
                for r in (("{", ""), ("}", "")):
                    t = t.replace(*r)

                r = "df['" + t + "']"
            elif token.name in functions:
                r = "F." + token.value.lower()
            else:
                r = token.value
            result.append(r)
        result = "".join(result)
        return result
