package io.gtools.sql

class SqlFormatter {

    static final List<String> KEYWORDS = [
        'SELECT', 'FROM', 'WHERE', 'GROUP BY', 'HAVING', 'ORDER BY',
        'INNER JOIN', 'LEFT JOIN', 'RIGHT JOIN', 'FULL JOIN', 'JOIN',
        'ON', 'AND', 'OR', 'UNION', 'LIMIT', 'OFFSET'
    ]

    static String format(String sql) {
        def result = new StringBuilder()
        def lines = sql.trim()
            .replaceAll("\\s+", " ")
            .replaceAll("(?i)(${KEYWORDS.join('|')})") { full, keyword ->
                // for JOIN types that must be kept together
                keyword.toUpperCase()
            }
            .split(" ")
            .toList()

        def indent = ''
        def currentLine = ''

        lines.eachWithIndex { token, i ->
            def upperToken = token.toUpperCase()

            if (KEYWORDS.contains(upperToken)) {
                if (currentLine) {
                    result << indent << currentLine.trim() << '\n'
                    currentLine = ''
                }

                if (['AND', 'OR', 'ON'].contains(upperToken)) {
                    indent = '    '
                } else {
                    indent = ''
                }

                currentLine = upperToken + ' '
            } else {
                currentLine += token + ' '
            }
        }

        if (currentLine) {
            result << indent << currentLine.trim() << '\n'
        }

        return result.toString().trim()
    }
}
