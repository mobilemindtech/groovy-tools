package io.gtools.sql

import gio.core.Result
import gio.core.Option
import groovy.sql.GroovyRowResult
import groovy.sql.Sql
import groovy.transform.TupleConstructor
import groovy.util.logging.Slf4j

import javax.sql.DataSource
import java.lang.reflect.Field

@Slf4j
class Query {

    trait Converter {
        Closure converter
    }

    @TupleConstructor
    static class ColConverter implements Converter {
        List<String> cols = []

        ColConverter(List<String> cols, Closure converter) {
            this.cols = cols
            this.converter = converter
        }
    }

    @TupleConstructor
    static class TypeConverter implements Converter {
        Class type

        TypeConverter(Class type, Closure converter) {
            this.type = type
            this.converter = converter
        }
    }

    @TupleConstructor
    static class EnumConverter implements Converter {

        EnumConverter(Closure converter) {
            this.converter = converter
        }
    }

    private Sql _sql
    private QueryDSL _dsl
    private boolean _debug
    private List<Converter> converters = []

    static synchronized Query of(DataSource dataSource) {
        new Query(_sql: new Sql(dataSource))
    }

    /**
     * <pre>
     *      sql(dataSource) {
     *           select(
     *              col('c.id','id'),
     *              col('c.nome','nome'),
     *              col('c.frequencia_avaliacao', 'set_freq'),
     *              max('a.data_cadastro', 'last_av'),
     *              raw('TIMESTAMPDIFF( DAY, MAX(a.data_cadastro), NOW() )', 'days_since_last_av')
     *           )
     *           from 'cliente', 'c'
     *           innerJoin 'avaliacao', 'a', on('a.cliente_id', 'c.id')
     *           innerJoin 'tenant_erp', 't', on('c.tenant_id','t.id')
     *           innerJoin 'app_configs_tenant', 'act', on('act.tenant_id', 't.id')
     *           where {
     *              eq 'act.enviar_email_avaliacoes_que_vao_vencer', true
     *              gt 'c.frequencia_avaliacao', 0
     *           }
     *           groupBy 'c.id'
     *           having raw('(set_freq - days_since_last_av)'), lt(7)
     *      }
     *      .list()
     *
     * </pre>
     * <pre>
     *     sql {
     *          seletc 'id', 'name', 'username'
     *          from 'users'
     *          where {
     *              eq 'username', 'user@email.com'
     *          }
     *     }
     *     .firstAs(User)
     *     .rethrow
     *     .get() // Option<User>
     * </pre>
     * @param dataSource datasource
     * @param closure SQL description
     * @return Query instance
     */
    static synchronized Query sql(DataSource dataSource, @DelegatesTo(QueryDSL) Closure closure) {
        new Query(_sql: new Sql(dataSource), _dsl: new QueryDSL(closure))
    }


    Query debug() {
        this._debug = true
        this
    }

    /**
     * Apply converter to column name
     *
     * <code>
     *     withConverter 'colA' { col, val ->
     *      val.toInteger() // return a new converted value *
     *     }
     * </code>
     * @param col
     * @param f
     * @return
     */
    Query withConverter(String col, Closure f) {
        converters << new ColConverter([col], f)
        this
    }

    /**
     * Apply converter to columns names
     *
     * <code>
     *     withConverter ['colA', 'colB'] { col, val ->
     *      switch col {
     *          case 'colA':
     *              // convert colA
     *              break
     *          case 'colB':
     *              // convert colB
     *              break
     *      }
     *     }
     * </code>
     * @param col
     * @param f
     * @return
     */
    Query withConverter(List<String> cols, Closure f) {
        converters << new ColConverter(cols, f)
        this
    }

    /**
     * Apply converter to given type
     *
     * <code>
     *     withConverter MyClass { col, val ->
     *      // return a new converted value *
     *     }
     * </code>
     * @param col
     * @param f
     * @return
     */
    Query withConverter(Class type, Closure f) {
        converters << new TypeConverter(type, f)
        this
    }

    /**
     * Apply converter to given type
     *
     * <code>
     *     withConverter MyClass { col, val ->
     *      // return a new converted value *
     *     }
     * </code>
     * @param col
     * @param f
     * @return
     */
    Query withEnumConverter(Closure f) {
        converters << new EnumConverter(f)
        this
    }

    /**
     * DSL to build SQL
     * @param closure
     * @return
     */
    Query sql(@DelegatesTo(QueryDSL) Closure closure) {
        this._dsl = new QueryDSL(closure)
        this
    }

    Result<Boolean> exec(String sql, List args = []) {
        Result.tryOf {
            this._sql.execute(sql, args)
        }
    }

    Result<List<List<Object>>> insert(String sql, List args = []) {
        Result.tryOf {
            this._sql.executeInsert(sql, args)
        }
    }

    Result<List<GroovyRowResult>> insert(String sql, List args, List<String> keyColumnNames) {
        Result.tryOf {
            this._sql.executeInsert(sql, args, keyColumnNames)
                .collect {it?.fixedLegacyDate() as GroovyRowResult }
        }
    }


    Result<Integer> update(String sql, List args = []) {
        Result.tryOf {
            this._sql.executeUpdate(sql, args)
        }
    }

    Result<Option<GroovyRowResult>> first() {
        get(this._dsl)
    }

    Result<Option<GroovyRowResult>> get() {
        get(this._dsl)
    }

    Result<Option<GroovyRowResult>> get(QueryDSL dsl) {
        Result.tryOf {
            def compiled = dsl.compile()

            if (_debug) {
                log.info compiled.toString()
            }

            Option.of(this._sql.firstRow(compiled.sql, compiled.args)?.fixedLegacyDate() as GroovyRowResult)
        }
    }

    Result<Option<GroovyRowResult>> first(String sql, List args = []) {
        get(sql, args)
    }

    Result<Option<GroovyRowResult>> get(String sql, List args = []) {
        Result.tryOf {

            if (_debug) {
                log.info "SQL: $sql, ARGS: $args"
            }

            Option.of(this._sql.firstRow(sql, args)?.fixedLegacyDate() as GroovyRowResult)
        }
    }

    def <T extends Map> Result<Option<T>> firstAs(Class<T> clazz) {
        getAs(clazz)
    }

    def <T extends Map> Result<Option<T>> getAs(Class<T> clazz) {
        getAs(clazz, this._dsl)
    }

    def <T extends Map> Result<Option<T>> getAs(Class<T> clazz, QueryDSL dsl) {
        Result.tryOf {
            def compiled = dsl.compile()

            if (_debug) {
                log.info compiled.toString()
            }

            getAs(clazz, compiled.sql, compiled.args).orThrow
        }
    }

    def <T extends Map> Result<Option<T>> firstAs(Class<T> clazz, String sql, List args = []) {
        getAs(clazz, sql, args)
    }

    def <T extends Map> Result<Option<T>> getAs(Class<T> clazz, String sql, List args = []) {
        Result.tryOf {

            if (_debug) {
                log.info "SQL: $sql, ARGS: $args"
            }

            def row = this._sql.firstRow(sql, args)?.fixedLegacyDate() as GroovyRowResult

            if (!row) Option.<T> none()
            else Option.of(fillEntity(clazz, row))
        }
    }

    Result<List<GroovyRowResult>> list() {
        this.list(this._dsl)
    }

    Result<List<GroovyRowResult>> list(QueryDSL dsl) {
        Result.tryOf {
            def compiled = dsl.compile()

            if (_debug) {
                log.info compiled.toString()
            }

            this._sql.rows(compiled.sql, compiled.args)
                .collect { it?.fixedLegacyDate() as GroovyRowResult }
        }
    }

    Result<List<GroovyRowResult>> list(String sql, List args = []) {
        Result.tryOf {

            if (_debug) {
                log.info "SQL: $sql, ARGS: $args"
            }

            this._sql.rows(sql, args)
                .collect { it?.fixedLegacyDate() as GroovyRowResult }
        }
    }

    def <T extends Map> Result<List<T>> listAs(Class<T> clazz) {
        listAs(clazz, this._dsl)
    }

    def <T extends Map> Result<List<T>> listAs(Class<T> clazz, QueryDSL dsl) {
        Result.tryOf {
            def compiled = dsl.compile()

            if (_debug) {
                log.info compiled.toString()
            }

            listAs(clazz, compiled.sql, compiled.args).orThrow
        }
    }

    def <T extends Map> Result<List<T>> listAs(Class<T> clazz, String sql, List args = []) {
        Result.tryOf {

            if (_debug) {
                log.info "SQL: $sql, ARGS: $args"
            }

            this._sql.rows(sql, args).collect { row ->
                fillEntity(clazz, row?.fixedLegacyDate() as GroovyRowResult)
            }
        }
    }

    Result each(Closure eachRow) {
        this.each(this._dsl, eachRow)
    }

    Result each(QueryDSL dsl, Closure eachRow) {
        Result.tryOf {
            def compiled = dsl.compile()

            if (_debug) {
                log.info compiled.toString()
            }

            this._sql.rows(compiled.sql, compiled.args)
                .collect { it?.fixedLegacyDate() as GroovyRowResult }
                .each(eachRow)
            true
        }
    }

    private <T> T fillEntity(Class<T> clazz, GroovyRowResult row) {
        final result = clazz.getConstructor().newInstance() as T
        final fields = clazz.properties.declaredFields as List<Field>
        row.keySet().each { col ->

            final rowValue = row[col]
            final colName = col.toString()
            final fieldName = colNameToFieldName(colName)
            def field = fields.find { fd -> fd.name.toLowerCase() == fieldName.toLowerCase() }

            // maybe is relation
            if (colName.endsWith("_id") && !field) {
                final relName = fieldName.substring(0, fieldName.length() - 2)
                field = fields.find { fd -> fd.name.toLowerCase() == relName.toLowerCase() }

                if (field) { // set relation
                    final relValue = field.type.getConstructor().newInstance()
                    relValue."id" = rowValue
                    result."$field.name" = relValue
                } else {
                    log.warn "field name not found to column $colName for class $clazz.simpleName"
                }
            } else {
                result."$field.name" = Option.of(
                    converters.find {
                        switch (it) {
                            case TypeConverter:
                                return field.type.isAssignableFrom(it.type)
                            case ColConverter:
                                return it.cols.contains(fieldName)
                            case EnumConverter:
                                return field.type.isEnum()
                            default:
                                null
                        }
                    }
                ).orElseF {
                    // default enum converter
                    if (field.type.isEnum()) {
                        new EnumConverter({
                            _, value -> field.type.enumConstants.find { ev -> ev == field.type }
                        })
                    } else null
                }.map { ColConverter c ->
                    c.converter.call(field.name, rowValue)
                } | rowValue
            }
        }
        result
    }

    /**
     * Get field name based on column name:
     *
     * user_id -> userId
     *
     * @param colName
     * @return
     */
    private static char UNDERLINE = '_'
    private static String colNameToFieldName(String colName) {
        def fieldName = ""
        def chars = colName.chars
        for (int i = 0; i < chars.length; i++) {
            if (chars[i] == UNDERLINE) {
                fieldName += chars[++i].toUpperCase()
                continue
            }
            fieldName += chars[i]
        }
        fieldName
    }
}
