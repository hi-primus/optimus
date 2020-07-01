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
    "MOD": {    # Math functions               # Numeric Transformations
        "description": "MOD function description",
        "example": "MOD(COL_NAME)"},
    "ABS": "ABS function description",
    "EXP": "EXP function description",
    "LOG": "LOG function description",
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
    "UPPER": "ATANH function description",  # String Tranformations
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
    "YEAR": "ATANH function description",  # Date
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


functions = op_functions

reserved_words = {"functions": functions, "operators": {"unary": unary_operators, "binary": binary_operators}}
functions = list(op_functions.keys())


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
