import re


class Utils():
    def __init__(self, COMMA_DELIMITER):
        COMMA_DELIMITER = re.compile(''',(?=(?:[^"]*"[^"]*")*[^"]*$)''')
        self.COMMA_DELIMITER = COMMA_DELIMITER
