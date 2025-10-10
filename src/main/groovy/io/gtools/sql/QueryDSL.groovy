package io.gtools.sql

import groovy.util.logging.Slf4j
//import org.hibernate.engine.jdbc.internal.BasicFormatterImpl


@Slf4j
class QueryDSL {
    
    static class CompiledQuery {
        String sql
        List args

        //private static formatter = new BasicFormatterImpl()

        String toString() {
            def formatted = SqlFormatter.format(sql)
            "SQL: ${formatted} ARGS: ${args.collect { "$it" }.join(",")}"
        }

    }

    static class CompiledCondition {
        String sql
        CondTyp typ
    }

    static class CompiledConditions {
        List<CompiledCondition> conditions
        List args
    }

    static enum CondTyp {
        Eq,
        Ne,
        Lte,
        Gte,
        Gt,
        Lt,
        Exists,
        NotExists,
        In,
        NotIn,
        Like,
        ILike,
        Between,
        And,
        Or,
        Col,
        On,
        Having,
        Is,
        IsNot
    }

    enum JoinType {
        Left,
        Right,
        Cross,
        Inner
    }

    static class Table {
        String name
        String alias
    }

    static class Join {
        String table
        String alias
        JoinType joinType
        QueryDSL dsl
        On on
    }

    static class Column {
        String name
        String alias
    }

    static interface Cond {
        CondTyp getTyp()
    }

    static class CondVal implements Cond {
        CondTyp typ
        Object value
    }

    static class CondVals implements Cond {
        CondTyp typ
        Object values = []
    }

    static class Condition {
        CondTyp typ = CondTyp.And // and or or
        Column col
        Cond cond
    }

    static class OrderBy {
        String col
        String order
    }

    static class On {
        String colLeft
        String colRight
    }

    static class Col {
        String name
        String alias
    }

    static class CustomColumn extends Column {
        String value
    }

    static class Having {
        Column col
        Cond val
    }


    private Table table
    private List<Column> columns = []
    private List<Condition> conditions = []
    private List<Join> joins = []
    private int _limit = -1
    private int _offset = -1
    private Closure computation
    private Closure whereClosure
    private Having _having
    private List<OrderBy> orders = []
    private List<String> groups = []
    private boolean dslApplied = false

    QueryDSL(Closure computation) {
        this.computation = computation
        this.computation.delegate = this
    }

    static synchronized QueryDSL sql(@DelegatesTo(QueryDSL) Closure computation) {
        new QueryDSL(computation)
    }

    QueryDSL sub(@DelegatesTo(QueryDSL) Closure closure) {
        sql(closure)
    }

    /**
     * Select all (*) columns
     * @return
     */
    QueryDSL selectAll() {
        select '*'
    }

    /**
     * Var args of columns to select. Exemple:
     * <pre>
     * select(
     *  col('c.id','id'),
     *  col('c.nome','nome'),
     *  col('c.frequencia_avaliacao', 'set_freq'),
     *  max('a.data_cadastro', 'last_av'),
     *  raw('TIMESTAMPDIFF( DAY, MAX(a.data_cadastro), NOW() )', 'days_since_last_av')
     * )
     * </pre>
     * @param cols
     * @return
     */
    QueryDSL select(Object... cols) {
        select(Arrays.asList(cols))
    }

    /**
     * List of columns to select. Ex.:
     * <pre>
     *     select ['col1', 'col2']
     * </pre>
     * @param cols
     * @return
     */
    QueryDSL select(List cols) {
        for (def it in cols) {
            if (it instanceof Col) {
                this.columns << new Column(name: it.name, alias: it.alias)
            } else if (it instanceof CustomColumn) {
                this.columns << it
            } else {
                def sp = it.toString().split(" ")
                if (sp.length > 1) {
                    this.columns << new Column(name: sp[0], alias: sp[1])
                } else {
                    this.columns << new Column(name: it)
                }
            }
        }
        this
    }

    /**
     * Apply sum to column {@code select { sum('col') }}
     * @param colName
     * @param alias
     * @return
     */
    CustomColumn sum(String colName, String alias = "") {
        new CustomColumn(value: "SUM($colName)", alias: alias)
    }

    /**
     * Apply min to column {@code select { min('col') }}
     * @param colName
     * @param alias
     * @return
     */
    CustomColumn min(String colName, String alias = "") {
        new CustomColumn(value: "MIN($colName)", alias: alias)
    }

    /**
     * Apply max to column {@code select { max('col') }}
     * @param colName
     * @param alias
     * @return
     */
    CustomColumn max(String colName, String alias = "") {
        new CustomColumn(value: "MAX($colName)", alias: alias)
    }

    /**
     * Apply avg to column {@code select { avg('col') }}
     * @param colName
     * @param alias
     * @return
     */
    CustomColumn avg(String colName, String alias = "") {
        new CustomColumn(value: "AVG($colName)", alias: alias)
    }

    /**
     * Apply count to column {@code select { count('col') }}
     * @param colName
     * @param alias
     * @return
     */
    CustomColumn count(String colName, String alias = "") {
        new CustomColumn(value: "COUNT($colName)", alias: alias)
    }

    /**
     * Apply distinct to column {@code select { avg('col') }}
     * @param colName
     * @param alias
     * @return
     */
    CustomColumn distinct(String colName, String alias = "") {
        new CustomColumn(value: "DISTINCT($colName)", alias: alias)
    }

    /**
     * Apply date to column {@code select { date('col') }}, same of SQL {@code SELECT DATE(col)}
     * @param colName
     * @param alias
     * @return
     */
    CustomColumn date(String colName, String alias = "") {
        new CustomColumn(value: "DATE($colName)", alias: alias)
    }

    /**
     * Apply day to column {@code select { day('col') }}, same of SQL {@code SELECT DAY(col)}
     * @param colName
     * @param alias
     * @return
     */
    CustomColumn day(String colName, String alias = "") {
        new CustomColumn(value: "DAY($colName)", alias: alias)
    }

    /**
     * Apply day name to column {@code select { dayName('col') }}, same of SQL {@code SELECT DAYNAME(col)}
     * @param colName
     * @param alias
     * @return
     */
    CustomColumn dayName(String colName, String alias = "") {
        new CustomColumn(value: "DAYNAME($colName)", alias: alias)
    }

    /**
     * Apply month to column {@code select { month('col') }}, same of SQL {@code SELECT MONTH(col)}
     * @param colName
     * @param alias
     * @return
     */
    CustomColumn month(String colName, String alias = "") {
        new CustomColumn(value: "MONTH($colName)", alias: alias)
    }

    /**
     * Apply month name to column {@code select { monthName('col') }}, same of SQL {@code SELECT MONTHNAME(col)}
     * @param colName
     * @param alias
     * @return
     */
    CustomColumn monthName(String colName, String alias = "") {
        new CustomColumn(value: "MONTHNAME($colName)", alias: alias)
    }

    /**
     * Apply year to column {@code select { year('col') }}, same of SQL {@code SELECT YEAR(col)}
     * @param colName
     * @param alias
     * @return
     */
    CustomColumn year(String colName, String alias = "") {
        new CustomColumn(value: "YEAR($colName)", alias: alias)
    }

    /**
     * Apply hour to column {@code select { hour('col') }}, same of SQL {@code SELECT HOUR(col)}
     * @param colName
     * @param alias
     * @return
     */
    CustomColumn hour(String colName, String alias = "") {
        new CustomColumn(value: "HOUR($colName)", alias: alias)
    }

    /**
     * Apply mintute to column {@code select { mintute('col') }}, same of SQL {@code SELECT MINUTE(col)}
     * @param colName
     * @param alias
     * @return
     */
    CustomColumn minute(String colName, String alias = "") {
        new CustomColumn(value: "MINUTE($colName)", alias: alias)
    }

    /**
     * Get current time {@code select { currtime() }}, same of SQL {@code SELECT currtime()}
     * @param alias
     * @return
     */
    CustomColumn currtime(String alias = "") {
        new CustomColumn(value: "currtime()", alias: alias)
    }

    /**
     * Apply any transformation to column {@code select { raw('sum(col)') }}, same of SQL {@code SELECT sum(col)}
     * @param val
     * @param alias
     * @return
     */
    CustomColumn raw(String val, String alias = "") {
        new CustomColumn(value: val, alias: alias)
    }

    /**
     * Get current date {@code select { currdate() }}, same of SQL {@code SELECT CURDATE()}
     * @param alias
     * @return
     */
    CustomColumn currdate(String alias = "") {
        new CustomColumn(value: "CURDATE()", alias: alias)
    }

    /**
     * Apply date format to column {@code select { dateFormat('col', 'DD/MM/YYYY') }}, same of SQL {@code SELECT DATE_FORMAT(col, 'DD/MM/YYYY')}
     * @param colName
     * @param format
     * @param alias
     * @return
     */
    CustomColumn dateFormat(String colName, String format, String alias = "") {
        new CustomColumn(value: "DATE_FORMAT($colName, '$format')", alias: alias)
    }

    /**
     *
     * @param table Table name
     * @return
     */
    QueryDSL from(String table) {
        from(table, "")
    }

    /**
     *
     * @param table Table name
     * @param alias Table alias
     * @return
     */
    QueryDSL from(String table, String alias) {
        this.table = new Table(name: table, alias: alias)
        this
    }

    /**
     * Create left join with custom where
     *
     * <pre>
     *     from 'tablea', 'a'
     *     join 'tableb', 'b' {
     *         eq 'b.a_id', col('a.id')
     *     }
     * </pre>
     * @param table Join table
     * @param alias Join alias
     * @param closure
     * @return
     */
    QueryDSL join(String table, String alias, @DelegatesTo(QueryDSL) Closure closure) {
        def dsl = new QueryDSL(closure)
        this.joins << new Join(table: table, alias: alias, joinType: JoinType.Left, dsl: dsl)
        this
    }

    /**
     * Create left join with on
     *
     * <pre>
     *     from 'tablea', 'a'
     *     join 'tableb', 'b', on('b.id', 'a.id')
     * </pre>
     *
     * @param table
     * @param alias
     * @param on
     * @return
     */
    QueryDSL join(String table, String alias, On on) {
        this.joins << new Join(table: table, alias: alias, joinType: JoinType.Left, on: on)
        this
    }


    /**
     * Create right join with custom where
     *
     * <pre>
     *     from 'tablea', 'a'
     *     rightJoin 'tableb', 'b' {
     *         eq 'b.a_id', col('a.id')
     *     }
     * </pre>
     * @param table Join table
     * @param alias Join alias
     * @param closure
     * @return
     */
    QueryDSL rightJoin(String table, String alias, @DelegatesTo(QueryDSL) Closure closure) {
        def dsl = new QueryDSL(closure)
        this.joins << new Join(table: table, alias: alias, joinType: JoinType.Right, dsl: dsl)
        this
    }

    /**
     * Create right join with on
     *
     * <pre>
     *     from 'tablea', 'a'
     *     rigthJoin 'tableb', 'b', on('b.id', 'a.id')
     * </pre>
     *
     * @param table
     * @param alias
     * @param on
     * @return
     */
    QueryDSL rightJoin(String table, String alias, On on) {
        this.joins << new Join(table: table, alias: alias, joinType: JoinType.Right, on: on)
        this
    }

    /**
     * Create inner join with custom where
     *
     * <pre>
     *     from 'tablea', 'a'
     *     innerJoin 'tableb', 'b' {
     *         eq 'b.a_id', col('a.id')
     *     }
     * </pre>
     * @param table Join table
     * @param alias Join alias
     * @param closure
     * @return
     */
    QueryDSL innerJoin(String table, String alias, @DelegatesTo(QueryDSL) Closure closure) {
        def dsl = new QueryDSL(closure)
        this.joins << new Join(table: table, alias: alias, joinType: JoinType.Inner, dsl: dsl)
        this
    }

    /**
     * Create inner join with on
     *
     * <pre>
     *     from 'tablea', 'a'
     *     innerJoin 'tableb', 'b', on('b.id', 'a.id')
     * </pre>
     *
     * @param table
     * @param alias
     * @param on
     * @return
     */
    QueryDSL innerJoin(String table, String alias, On on) {
        this.joins << new Join(table: table, alias: alias, joinType: JoinType.Inner, on: on)
        this
    }

    /**
     * Create cross join with custom where
     *
     * <pre>
     *     from 'tablea', 'a'
     *     crossJoin 'tableb', 'b' {
     *         eq 'b.a_id', col('a.id')
     *     }
     * </pre>
     * @param table Join table
     * @param alias Join alias
     * @param closure
     * @return
     */
    QueryDSL crossJoin(String table, String alias, @DelegatesTo(QueryDSL) Closure closure) {
        def dsl = new QueryDSL(closure)
        this.joins << new Join(table: table, alias: alias, joinType: JoinType.Cross, dsl: dsl)
        this
    }

    /**
     * Create cross join with on
     *
     * <pre>
     *     from 'tablea', 'a'
     *     crossJoin 'tableb', 'b', on('b.id', 'a.id')
     * </pre>
     *
     * @param table
     * @param alias
     * @param on
     * @return
     */
    QueryDSL crossJoin(String table, String alias, On on) {
        this.joins << new Join(table: table, alias: alias, joinType: JoinType.Cross, on: on)
        this
    }

    /**
     * SQL condition
     * <pre>
     *   where {
     *      eq 'act.enviar_email_avaliacoes_que_vao_vencer', true
     *      gt 'c.frequencia_avaliacao', 0
     *   }
     * </pre>
     * @param closure
     * @return
     */
    QueryDSL where(@DelegatesTo(QueryDSL) Closure closure) {
        closure.delegate = this
        this.whereClosure = closure
        this
    }

    /**
     * SQL condition
     * <pre>
     *   where 'name', 'join' {
     *      gt 'age', 21
     *   }
     * </pre>
     * @param closure
     * @return
     */
    QueryDSL where(String colName, String val, @DelegatesTo(QueryDSL) Closure closure = null) {
        where(colName, eq(val), closure)
    }

    /**
     * SQL condition
     * <pre>
     *   where 'id', 1 {
     *      gt 'age', 21
     *   }
     * </pre>
     * @param closure
     * @return
     */
    QueryDSL where(String colName, int val, @DelegatesTo(QueryDSL) Closure closure = null) {
        where(colName, eq(val), closure)
    }

    /**
     * SQL condition
     * <pre>
     *   where 'id', 1L {
     *      gt 'age', 21
     *   }
     * </pre>
     * @param closure
     * @return
     */
    QueryDSL where(String colName, long val, @DelegatesTo(QueryDSL) Closure closure = null) {
        where(colName, eq(val), closure)
    }

    /**
     * SQL condition
     * <pre>
     *   where 'val', 1.2 {
     *      gt 'age', 21
     *   }
     * </pre>
     * @param closure
     * @return
     */
    QueryDSL where(String colName, double val, @DelegatesTo(QueryDSL) Closure closure = null) {
        where(colName, eq(val), closure)
    }

    /**
     * SQL condition
     * <pre>
     *   where 'val', 1.2 {
     *      gt 'age', 21
     *   }
     * </pre>
     * @param closure
     * @return
     */
    QueryDSL where(String colName, float val, @DelegatesTo(QueryDSL) Closure closure = null) {
        where(colName, eq(val), closure)
    }


    /**
     * SQL condition
     * <pre>
     *   where 'id', eq(1) {
     *      gt 'age', 21
     *   }
     * </pre>
     * @param closure
     * @return
     */
    QueryDSL where(String colName, Cond cond, @DelegatesTo(QueryDSL) Closure closure = null) {
        def col = columns.find { it.name == colName } ?: new Column(name: colName)
        assert col
        this.conditions << new Condition(col: col, cond: cond)
        if (closure)
            where(closure)
        this
    }

    /**
     * SQL: where x = ?
     * @param colName
     * @param value
     * @return
     */
    QueryDSL eq(String colName, Object value) {
        where(colName, eq(value))
    }

    /**
     * SQL: where x = ?
     * @param colName
     * @param value
     * @return
     */
    CondVal eq(Object value) {
        new CondVal(typ: CondTyp.Eq, value: value)
    }

    /**
     * SQL: where x is ?
     * @param colName
     * @param value
     * @return
     */
    QueryDSL isEq(String colName, Object value) {
        where(colName, isEq(value))
    }

    /**
     * SQL: where x is ?
     * @param colName
     * @param value
     * @return
     */
    CondVal isEq(Object value) {
        new CondVal(typ: CondTyp.Is, value: value)
    }

    /**
     * SQL: where x is not ?
     * @param colName
     * @param value
     * @return
     */
    QueryDSL isNot(String colName, Object value) {
        where(colName, isNot(value))
    }

    /**
     * SQL: where x is not ?
     * @param colName
     * @param value
     * @return
     */
    CondVal isNot(Object value) {
        new CondVal(typ: CondTyp.IsNot, value: value)
    }

    /**
     * SQL: where x <> ?
     * @param colName
     * @param value
     * @return
     */
    QueryDSL ne(String colName, Object value) {
        where(colName, ne(value))
    }

    /**
     * SQL: where x <> ?
     * @param colName
     * @param value
     * @return
     */
    CondVal ne(Object value) {
        new CondVal(typ: CondTyp.Ne, value: value)
    }

    /**
     * SQL: where x < ?
     * @param colName
     * @param value
     * @return
     */
    QueryDSL lt(String colName, Object value) {
        where(colName, lt(value))
    }

    /**
     * SQL: where x < ?
     * @param colName
     * @param value
     * @return
     */
    CondVal lt(Object value) {
        new CondVal(typ: CondTyp.Lt, value: value)
    }

    /**
     * SQL: where x <= ?
     * @param colName
     * @param value
     * @return
     */
    QueryDSL lte(String colName, Object value) {
        where(colName, lte(value))
    }

    /**
     * SQL: where x <= ?
     * @param colName
     * @param value
     * @return
     */
    CondVal lte(Object value) {
        new CondVal(typ: CondTyp.Lte, value: value)
    }

    /**
     * SQL: where x > ?
     * @param colName
     * @param value
     * @return
     */
    QueryDSL gt(String colName, Object value) {
        where(colName, gt(value))
    }

    /**
     * SQL: where x > ?
     * @param colName
     * @param value
     * @return
     */
    CondVal gt(Object value) {
        new CondVal(typ: CondTyp.Gt, value: value)
    }

    /**
     * SQL: where x >= ?
     * @param colName
     * @param value
     * @return
     */
    QueryDSL gte(String colName, Object value) {
        where(colName, gte(value))
    }

    /**
     * SQL: where x >= ?
     * @param colName
     * @param value
     * @return
     */
    CondVal gte(Object value) {
        new CondVal(typ: CondTyp.Gte, value: value)
    }


    /**
     * <pre>
     *     selectAll()
     *     from 'tablea', 'a'
     *     where {
     *          exists {
     *              from 'tenant_erp'
     *              select 'id'
     *              where('id', eq(col('a.id'))) {
     *                  eq 'id', 2
     *              }
     *         }
     *     }
     * </pre>
     * @param closure
     * @return
     */
    QueryDSL exists(@DelegatesTo(QueryDSL) Closure closure) {
        def dsl = sql(closure)
        this.conditions << new Condition(cond: new CondVal(typ: CondTyp.Exists, value: dsl))
        this
    }

    /**
     * <pre>
     *     selectAll()
     *     from 'tablea', 'a'
     *     where {
     *          notExists {
     *              from 'tenant_erp'
     *              select 'id'
     *              where {
     *                  eq 'id', col('p.id')
     *              }
     *         }
     *     }
     * </pre>
     * @param closure
     * @return
     */
    QueryDSL notExists(@DelegatesTo(QueryDSL) Closure closure) {
        def dsl = sql(closure)
        this.conditions << new Condition(cond: new CondVal(typ: CondTyp.NotExists, value: dsl))
        this
    }

    /**
     * <pre>
     *     where {
     *         and {
     *              eq 'name', 'ricardo'
     *              or 'id', '1'
     *           }
     *     }
     * </pre>
     * @param closure
     * @return
     */
    QueryDSL and(@DelegatesTo(QueryDSL) Closure closure) {
        def dsl = sql(closure)
        dsl.table = this.table
        this.conditions << new Condition(cond: new CondVal(typ: CondTyp.And, value: dsl))
        this
    }

    /**
     * SQL and
     * @param colName
     * @param val
     * @return
     */
    QueryDSL and(String colName, String val) {
        and(colName, eq(val))
    }

    /**
     * SQL and
     * @param colName
     * @param val
     * @return
     */
    QueryDSL and(String colName, int val) {
        and(colName, eq(val))
    }

    /**
     * SQL and
     * @param colName
     * @param val
     * @return
     */
    QueryDSL and(String colName, long val) {
        and(colName, eq(val))
    }

    /**
     * SQL and
     * @param colName
     * @param val
     * @return
     */
    QueryDSL and(String colName, double val) {
        and(colName, eq(val))
    }

    /**
     * SQL and
     * @param colName
     * @param val
     * @return
     */
    QueryDSL and(String colName, float val) {
        and(colName, eq(val))
    }

    /**
     * SQL and
     * @param colName
     * @param val
     * @return
     */
    QueryDSL and(String colName, Cond cond) {
        def col = columns.find { it.name == colName } ?: new Column(name: colName)
        assert col
        this.conditions << new Condition(col: col, cond: cond)
        this
    }

    /**
     * SQL or
     * <pre>
     *   or {
     *      eq 'id', 5
     *      and 'id', eq(6)
     *   }
     * </pre>
     * @param colName
     * @param val
     * @return
     */
    QueryDSL or(@DelegatesTo(QueryDSL) Closure closure) {
        def dsl = new QueryDSL(closure)
        dsl.table = this.table
        this.conditions << new Condition(cond: new CondVal(typ: CondTyp.Or, value: dsl), typ: CondTyp.Or)
        this
    }

    /**
     * SQL or
     * @param colName
     * @param val
     * @return
     */
    QueryDSL or(String colName, String val) {
        or(colName, eq(val))
    }

    /**
     * SQL or
     * @param colName
     * @param val
     * @return
     */
    QueryDSL or(String colName, int val) {
        or(colName, eq(val))
    }

    /**
     * SQL or
     * @param colName
     * @param val
     * @return
     */
    QueryDSL or(String colName, long val) {
        or(colName, eq(val))
    }

    /**
     * SQL or
     * @param colName
     * @param val
     * @return
     */
    QueryDSL or(String colName, double val) {
        or(colName, eq(val))
    }

    /**
     * SQL or
     * @param colName
     * @param val
     * @return
     */
    QueryDSL or(String colName, float val) {
        or(colName, eq(val))
    }

    /**
     * SQL or
     * @param colName
     * @param val
     * @return
     */
    QueryDSL or(String colName, Cond cond) {
        def col = columns.find { it.name == colName } ?: new Column(name: colName)
        assert col
        this.conditions << new Condition(col: col, cond: cond, typ: CondTyp.Or)
        this
    }

    /**
     * SQL in
     * <pre>
     *     where {
     *         inList 'id', [1,2,3]
     *     }
     *
     *     // or
     *
     *     where 'id', inList [1,2,3]
     *
     * </pre>
     * @param colName
     * @param val
     * @return
     */
    CondVals inList(List values) {
        new CondVals(typ: CondTyp.In, values: values)
    }

    /**
     * SQL in
     * <pre>
     *     selectAll()
     *     from 'tablea', 'a'
     *     where 'id', inList {
     *         from 'tableb'
     *         select 'id'
     *         where {
     *             eq 'id', col('a.id')
     *         }
     *     }
     * </pre>
     * @param colName
     * @param val
     * @return
     */
    CondVal inList(@DelegatesTo(QueryDSL) Closure closure) {
        def dsl = new QueryDSL(closure)
        new CondVal(typ: CondTyp.In, value: dsl)
    }

    /**
     * Select in
     * <pre>
     *     inList 'id', [1,2,3]
     * </pre>
     * @param colName
     * @param value
     * @return
     */
    QueryDSL inList(String colName, Object value) {
        where(colName, inList(value))
    }

    /**
     * SQL between
     * @param value1
     * @param value2
     * @return
     */
    CondVals between(Object value1, Object value2) {
        new CondVals(typ: CondTyp.Between, values: [value1, value2])
    }

    // não faz sentido betwwen com Closure
    //CondVal between(@DelegatesTo(QueryDSL) Closure closure) {
    //    def dsl = new QueryDSL(closure)
    //    new CondVal(typ: CondTyp.Between, value: dsl)
    //}

    /**
     * SQL between
     * @param value1
     * @param value2
     * @return
     */
    QueryDSL between(String colName, Object value1, Object value2) {
        where(colName, between(value1, value2))
    }

    /**
     * SQL not in, see {@code inList}
     * @param values
     * @return
     */
    CondVals notInList(List values) {
        new CondVals(typ: CondTyp.NotIn, values: values)
    }

    /**
     * SQL not in, see {@code inList}
     * @param values
     * @return
     */
    CondVal notInList(@DelegatesTo(QueryDSL) Closure closure) {
        def dsl = new QueryDSL(closure)
        new CondVal(typ: CondTyp.NotIn, value: dsl)
    }

    /**
     * SQL not in, see {@code inList}
     * @param values
     * @return
     */
    QueryDSL notInList(String colName, Object value) {
        where(colName, notInList(value))
    }


    /**
     * SQL like
     * <pre>
     *     like 'name', '%ricardo%'
     * </pre>
     * @param value
     * @return
     */
    CondVal like(String value) {
        new CondVal(typ: CondTyp.Like, value: value)
    }

    /**
     * SQL like
     * <pre>
     *     like 'name', '%ricardo%'
     * </pre>
     * @param value
     * @return
     */
    QueryDSL like(String colName, String value) {
        where(colName, like(value))
    }

    /**
     * Apply upper function on column and value
     * <pre>
     *     ilike 'name', '%ricardo%'
     * </pre>
     * @param value
     * @return
     */
    CondVal ilike(String value) {
        new CondVal(typ: CondTyp.ILike, value: value)
    }

    /**
     * Apply upper function on column and value
     * <pre>
     *     ilike 'name', '%ricardo%'
     * </pre>
     * @param value
     * @return
     */
    QueryDSL ilike(String colName, String value) {
        where(colName, ilike(value))
    }

    /**
     * Column definition
     * @param value Column name
     * @return
     */
    CondVal col(String value) {
        new CondVal(typ: CondTyp.Col, value: value)
    }

    /**
     * Column definition
     * @param name Column name
     * @param alias Column alias
     * @return
     */
    Col col(String name, String alias) {
        new Col(name: name, alias: alias)
    }

    /**
     * Join on. See {@code join}
     * @param colLeft
     * @param colRight
     * @return
     */
    On on(String colLeft, String colRight) {
        new On(colLeft: colLeft, colRight: colRight)
        //new CondVal(typ: CondTyp.On, value: )
    }

    /**
     * Order by
     * @param colName column name
     * @param order asc or desc
     * @return
     */
    QueryDSL orderBy(String colName, String order = "asc") {
        this.orders << new OrderBy(col: colName, order: order)
        this
    }

    /**
     * Group by
     * @param cols
     * @return
     */
    QueryDSL groupBy(String... cols) {
        groups.addAll(cols)
        this
    }

    /**
     * Having
     * <pre>
     *     having 'id', get(0)
     * </pre>
     * @param colName
     * @param val
     * @return
     */
    QueryDSL having(String colName, Cond val) {
        this._having = new Having(col: new Column(name: colName), val: val)
        this
    }

    /**
     * <pre>
     *     having raw('(set_freq - days_since_last_av)'), between(0, 7)
     *
     *     // or
     *
     *     having count('id'), gt(0)
     *
     * </pre>
     * @param col
     * @param val
     * @return
     */
    QueryDSL having(CustomColumn col, Cond val) {
        this._having = new Having(col: col, val: val)
        this
    }

    /**
     * SQL limit
     * @param l
     * @return
     */
    QueryDSL limit(int l) {
        this._limit = l
        this
    }

    /**
     * SQL offset
     * @param o
     * @return
     */
    QueryDSL offset(int o) {
        this._offset = o
        this
    }

    boolean getCompleteQuery() {
        !this.columns.empty
    }

    private List makeCondition(Condition conditon) {

        def args = []

        def cond = new StringBuilder()

        switch (conditon.cond.typ) {
            case CondTyp.Eq:
                cond << makeColName(conditon.col) << " = "
                break
            case CondTyp.Is:
                cond << makeColName(conditon.col) << " IS "
                break
            case CondTyp.IsNot:
                cond << makeColName(conditon.col) << " IS NOT "
                break
            case CondTyp.Ne:
                cond << makeColName(conditon.col) << " <> "
                break
            case CondTyp.Gt:
                cond << makeColName(conditon.col) << " > "
                break
            case CondTyp.Gte:
                cond << makeColName(conditon.col) << " >= "
                break
            case CondTyp.Lt:
                cond << makeColName(conditon.col) << " < "
                break
            case CondTyp.Lte:
                cond << makeColName(conditon.col) << " <= "
                break
            case CondTyp.Exists:
                if (conditon.col)
                    cond << makeColName(conditon.col)
                cond << " EXISTS "
                break
            case CondTyp.NotExists:
                if (conditon.col)
                    cond << makeColName(conditon.col)
                cond << " NOT EXISTS "
                break
            case CondTyp.In:
                cond << makeColName(conditon.col) << " IN "
                break
            case CondTyp.NotIn:
                cond << makeColName(conditon.col) << " NOT IN "
                break
            case CondTyp.Like:
                cond << makeColName(conditon.col) << " LIKE "
                break
            case CondTyp.ILike:
                cond << "UPPER(" << conditon.col.name << ")" << " LIKE "
                break
            case CondTyp.Between:
                cond << makeColName(conditon.col) << " BETWEEN "
                break
            case CondTyp.And:
            case CondTyp.Or:
            case CondTyp.On:
                //pass
                break
            default:
                throw new Exception("invalid condition type: ${conditon.cond.typ}")
        }
        //def cond = new StringBuilder()

        Closure compileDsl = { CondVal condVal ->
            def dsl = condVal.value as QueryDSL
            String compiled

            dsl.applyDsl()

            if (dsl.completeQuery) {
                def compiledQuery = dsl.compile()
                compiled = compiledQuery.sql
                args.addAll(compiledQuery.args)
            } else {
                def compiledConditions = dsl.makeConditions()
                compiled = dsl.compileConditions(compiledConditions.conditions)
                args.addAll(compiledConditions.args)
            }
            cond.append("(")
                    .append(compiled)
                    .append(") ")
        }

        switch (conditon.cond.typ) {
            case CondTyp.Between:
                cond.append("? AND ? ")
                args.addAll((conditon.cond as CondVals).values)
                break
            case CondTyp.In:
            case CondTyp.NotIn:
                if (conditon.cond instanceof CondVals) {
                    def condVals = conditon.cond as CondVals
                    cond.append("(")
                            .append(condVals.values.collect { "?" }.join(","))
                            .append(") ")
                    args.addAll(condVals.values)
                } else if (conditon.cond instanceof CondVal) {
                    compileDsl(conditon.cond as CondVal)
                } else {
                    throw new Exception("invalid in condition type: ${conditon.cond?.class?.name}")
                }
                break
            case CondTyp.ILike:
                cond.append("UPPER(?) ")
                args << (conditon.cond as CondVal).value
                break
            case CondTyp.Exists:
            case CondTyp.NotExists:
            case CondTyp.And:
            case CondTyp.Or:
                compileDsl(conditon.cond as CondVal)
                break
            case CondTyp.On:
                def vals = (conditon.cond as CondVals).values
                cond.append(vals[0]).append(" = ").append(vals[1])
                break
            default:
                def val = (conditon.cond as CondVal).value
                def isCol = false
                if (val instanceof CondVal) {
                    def valCond = val as CondVal
                    switch (valCond.typ) {
                        case CondTyp.Col:
                        case CondTyp.On:
                            cond.append(valCond.value).append(" ")
                            isCol = true
                    }
                }

                if (!isCol) {
                    cond.append("? ")
                    args << val
                }
        }

        [cond.toString(), args]
    }

    private CompiledConditions makeConditions() {

        this.applyDsl()

        def args = []
        def conds = []

        def copy = conditions.collect { it }

        for (def it in copy) {
            def result = makeCondition(it)
            conds << new CompiledCondition(typ: it.typ, sql: result[0])
            args.addAll(result[1])
        }

        new CompiledConditions(conditions: conds, args: args)
    }

    private void applyDsl() {
        if (!this.dslApplied) {
            this.computation.call()
            this.whereClosure?.call()
            this.dslApplied = true
        }
    }

    private String compileConditions(List<CompiledCondition> conds) {
        def whereSql = new StringBuilder()

        if (conds) {
            if (this.completeQuery)
                whereSql << " WHERE "

            whereSql << conds.first().sql

            for (def it in conds.drop(1)) {
                def op = it.typ == CondTyp.Or ? "OR" : "AND"
                whereSql << "$op " << it.sql
            }
        }

        whereSql.toString()
    }

    private CompiledQuery compileJoins() {
        def joinList = new StringBuilder()
        def args = []
        for (def it in this.joins) {

            switch (it.joinType) {
                case JoinType.Left:
                    joinList.append(" LEFT JOIN ")
                    break
                case JoinType.Right:
                    joinList.append(" RIGHT JOIN ")
                    break
                case JoinType.Inner:
                    joinList.append(" INNER JOIN ")
                    break
                case JoinType.Cross:
                    joinList.append(" CROSS JOIN ")
                    break
            }

            def table = it.table

            if (it.alias)
                table += " $it.alias"

            joinList.append(table)

            if (it.on) {
                joinList.append(" ON ")
                        .append(it.on.colLeft)
                        .append(" = ")
                        .append(it.on.colRight)
            } else {
                def conditions = it.dsl.makeConditions()
                def compiled = it.dsl.compileConditions(conditions.conditions)
                joinList.append(" ON ")
                        .append(compiled)
                args.addAll(conditions.args)
            }
        }

        return new CompiledQuery(sql: joinList.toString(), args: args)
    }

    private String makeColName(Column col) {
        if (col instanceof CustomColumn)
            col.value
        else
            makeColName(col.name)
    }

    private String makeColName(String col) {
        if (table?.alias && !col.contains("."))
            return "${table.alias}.$col"
        col
    }

    private String compileColumns() {
        def cols = []
        for (def it in this.columns) {
            def col = ""
            if (it instanceof CustomColumn) {
                col = (it as CustomColumn).value
                if (it.alias)
                    col += " $it.alias"
                cols << col
            } else {
                col = it.name
                if (it.alias)
                    col += " $it.alias"

                cols << makeColName(col)
            }
        }
        cols.join(", ")
    }

    CompiledQuery compile() {


        def compiledConditions = this.makeConditions()

        def cols = this.compileColumns()
        def conds = compiledConditions.conditions
        def args = []
        def compiledArgs = compiledConditions.args

        def joins = compileJoins()

        def whereSql = compileConditions(conds)
        def limitSql = ""
        def offsetSql = ""
        def orderBy = new StringBuilder()
        def groupBy = new StringBuilder()
        def having = new StringBuilder()

        args.addAll(joins.args)

        args.addAll(compiledArgs)

        if (_limit > 0) {
            limitSql = " LIMIT $_limit "
        }

        if (_offset > 0) {
            offsetSql = " OFFSET $_offset "
        }

        if (this.orders) {
            orderBy << " ORDER BY " << this.orders
                    .collect { "$it.col $it.order" }
                    .join(", ").trim() << " "
        }

        if (this.groups) {
            groupBy << " GROUP BY " << this.groups.join(", ") << " "
        }

        if (this._having) {
            having.append(" HAVING ")
            def result = makeCondition(
                    new Condition(typ: CondTyp.Having, cond: this._having.val, col: this._having.col))
            having.append(result[0])
            args.addAll(result[1])

        }

        def table = "$table.name "

        if (this.table.alias)
            table += "${table.alias} "

        def query = new StringBuilder("SELECT ")
                .append(cols)
                .append(" FROM ")
                .append(table)
                .append(joins.sql)
                .append(whereSql.toString())
                .append(groupBy.toString())
                .append(having.toString())
                .append(orderBy.toString())
                .append(limitSql)
                .append(offsetSql)


        return new CompiledQuery(sql: query.toString(), args: args)
    }
}
