from rply import LexerGenerator

#
# op_functions = {
#     "ABS": {
#         "type": "function",
#         "description": "Absolute value of a columns",
#         "params": "abs(column name)",
#         "example": "ABS(COL_NAME)"
#     }
# }

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
        "text": "ABS",
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
        "text": "ABS",
    },
    "LN": "LN function description",
    "POW": "POW function description",
    "CEILING": "CEILING function description",
    "SQRT": "SQRT function description",
    "FLOOR": "FLOOR function description",
    "TRUNC": "TRUNC function description",
    "RADIANS": "RADIANS function description",  # Trigonometric Functions
    "DEGREES": "DEGREES function description",
    "SIN": "SIN function description",
    "COS": "COS function description",
    "TAN": "TAN function description",
    "ASIN": "ASIN function description",
    "ACOS": "ACOS function description",
    "ATAN": "ATAN function description",
    "SINH": "SINH function description",
    "ASINH": "ASINH function description",
    "COSH": "COSH function description",
    "TANH": "TANH function description",
    "ACOSH": "ACOSH function description",
    "ATANH": "ATANH function description",
    "UPPER": "ATANH function description",  # String Transformations
    "LOWER": "ATANH function description",
    "PROPER": "ATANH function description",
    "TRIM": "ATANH function description",
    "REMOVE": "ATANH function description",
    "LEN": "ATANH function description",
    "FIND": "ATANH function description",
    "RFIND": "ATANH function description",
    "LEFT": "ATANH function description",
    "RIGHT": "ATANH function description",
    "STARTS_WITH": "ATANH function description",
    "ENDS_WITH": "ATANH function description",
    "EXACT": "ATANH function description",
    "YEAR": "ATANH function description",  # Date Functions
    "MONTH": "ATANH function description",
    "DAY": "ATANH function description",
    "HOUR": "ATANH function description",
    "MINUTE": "ATANH function description",
    "SECOND": "ATANH function description",

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
functions = list(op_functions.keys())
reserved_words = {"functions": functions, "operators": {"unary": unary_operators, "binary": binary_operators}}


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
