package io.gtools.sql

import groovy.util.logging.Slf4j
import spock.lang.Specification

import static io.gtools.sql.QueryDSL.sql



//test-app unit: com.sql.dsl.*
@Slf4j
class SqlDSLSpec extends Specification{

    void "test sql dsl"() {

        setup:
        given:
        def dsl = sql {
            select 'id', 'name', 'e.id', col('id', 'codigo'), sum("x", "count")
            from 'person', 'p'
            join 'employ', 'e', on('p.id', 'e.id')
            where 'id', eq(1), {
                eq 'e.id', 7
                and {
                    eq 'name', 'ricardo'
                    or 'id', '1'
                }
                or {
                    eq 'id', 5
                    and 'id', eq(6)
                }
                exists {
                    select 'id'
                    from 'customer'
                    where {
                        eq 'id', col('p.id')
                    }
                }
            }
            groupBy "p.id"
            having count('id'), gt(0)
            orderBy "id"
            limit 1
            offset 1
        }

        expect:
        dsl.compile().sql instanceof String
    }
}
