from rply import LexerGenerator

op_functions = ["SQRT", "ROUND"]


class Parser:
    """
    Parse an expression to optimus code
    """
    def __init__(self):
        self.lexer_generator = LexerGenerator()
        self._add_tokens()
        self.lexer = self.lexer_generator.build()

    def _add_tokens(self):
        # Print
        l_g = self.lexer_generator
        l_g.add('PRINT', r'print(?!\w)')

        for f in op_functions:
            rx = f'{f}(?!\w)'
            l_g.add(f, rx)

        # Parenthesis
        l_g.add('OPEN_PAREN', r'\(')
        l_g.add('CLOSE_PAREN', r'\)')

        # Semi Colon
        l_g.add('SEMI_COLON', r'\;')

        l_g.add('IDENTIFIER', "[^\W0-9]\w*")  # Reference
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
            #     print(token.name)
            if token.name == "IDENTIFIER":
                r = "df['" + token.value + "']"
            elif token.name in op_functions:
                r = "op." + token.value.lower()
            else:
                r = token.value
            result.append(r)
        result = "".join(result)
        return result
