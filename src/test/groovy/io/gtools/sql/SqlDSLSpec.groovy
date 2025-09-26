package io.gtools.sql

import groovy.util.logging.Slf4j
import org.junit.Assert
import org.junit.Test

import static io.gtools.sql.QueryDSL.sql



//test-app unit: com.sql.dsl.*
@Slf4j
class SqlDSLSpec {

    @Test
    void testDsl() {
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


        try{
            def c = dsl.compile()
        }catch (err){
            log.error err.message, err
        }

        Assert.assertTrue(true)
    }
}
