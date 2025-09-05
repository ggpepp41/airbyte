package io.airbyte.integrations.source.postgres

import io.airbyte.cdk.command.JdbcSourceConfiguration
import io.airbyte.cdk.data.AirbyteSchemaType
import io.airbyte.cdk.data.ArrayAirbyteSchemaType
import io.airbyte.cdk.data.LeafAirbyteSchemaType
import io.airbyte.cdk.test.fixtures.cleanup.TestAssetResourceNamer
import io.airbyte.cdk.test.fixtures.connector.JdbcTestDbExecutor
import io.airbyte.cdk.test.fixtures.tests.AnsiSql
import io.airbyte.cdk.test.fixtures.tests.FieldTypeMapperTest
import io.airbyte.integrations.source.postgres.config.PostgresSourceConfigurationFactory
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Timeout
import org.testcontainers.containers.PostgreSQLContainer

class PostgresSourceFieldTypeMapperTest : FieldTypeMapperTest() {

    private val schema = TestAssetResourceNamer().getName()
    override val configSpec = PostgresContainerFactory.config(container, listOf(schema))
    override val executor = JdbcTestDbExecutor(schema, jdbcConfig)
    private val jdbcConfig: JdbcSourceConfiguration
        get() = PostgresSourceConfigurationFactory().make(configSpec)

    companion object {
        lateinit var container: PostgreSQLContainer<*>

        @JvmStatic
        @BeforeAll
        @Timeout(value = 300)
        fun startAndProvisionTestContainer() {
            container = PostgresContainerFactory.shared17()
        }
    }


    override val testCases: List<TestCase> = buildList {
        // Numeric types
        scalarAndArray("SMALLINT", LeafAirbyteSchemaType.INTEGER, AnsiSql.smallIntValues)
        scalarAndArray("INTEGER", LeafAirbyteSchemaType.INTEGER, AnsiSql.intValues)
        scalarAndArray("INT", LeafAirbyteSchemaType.INTEGER, AnsiSql.intValues)
        scalarAndArray("BIGINT", LeafAirbyteSchemaType.INTEGER, AnsiSql.bigIntValues)
        scalarAndArray("DECIMAL", LeafAirbyteSchemaType.INTEGER, AnsiSql.decimalValues
                .plus(AnsiSql.nulledNanValues))
        scalarAndArray("DECIMAL(20,9)", LeafAirbyteSchemaType.NUMBER, AnsiSql.decimalValues)
        scalarAndArray("NUMERIC(20,9)", LeafAirbyteSchemaType.NUMBER, AnsiSql.decimalValues)
        scalarAndArray("REAL", LeafAirbyteSchemaType.NUMBER, AnsiSql.realValues
                .plus(AnsiSql.preservedNanValues))
        scalarAndArray("DOUBLE PRECISION", LeafAirbyteSchemaType.NUMBER, AnsiSql.doubleValues
                .plus(AnsiSql.preservedNanValues))

        scalarAndArray("CHAR(10)", LeafAirbyteSchemaType.STRING, AnsiSql.charValues.withLength(10))
        scalarAndArray("CHARACTER(10)", LeafAirbyteSchemaType.STRING, AnsiSql.charValues.withLength(10))
        scalarAndArray("VARCHAR(100)", LeafAirbyteSchemaType.STRING, AnsiSql.varcharValues)
        scalarAndArray("CHARACTER VARYING(100)", LeafAirbyteSchemaType.STRING, AnsiSql.varcharValues)
        scalarAndArray("CHAR VARYING(100)", LeafAirbyteSchemaType.STRING, AnsiSql.varcharValues)

        // Binary types
        //testCase("BINARY(20)", LeafAirbyteSchemaType.BINARY, AnsiSql.binary20Values),
        //testCase("VARBINARY(100)", LeafAirbyteSchemaType.BINARY, AnsiSql.varbinaryValues),
        //testCase("BINARY VARYING(100)", LeafAirbyteSchemaType.BINARY, AnsiSql.varbinaryValues),

        // TODO: add test cases
        // varbit / bit varying to string

        // Date/time types
        scalarAndArray(
            "DATE",
            LeafAirbyteSchemaType.DATE,
            AnsiSql.dateValues.plus(AnsiSql.preservedInfiniteValues)
                .mapKeys { "${it.key}::date" })
        scalarAndArray("TIME", LeafAirbyteSchemaType.TIME_WITHOUT_TIMEZONE, AnsiSql.timeValues
            .mapKeys { "${it.key}::time" })
        scalarAndArray(
                "TIMESTAMP",
                LeafAirbyteSchemaType.TIMESTAMP_WITHOUT_TIMEZONE,
                AnsiSql.timestampValues
                    .plus(AnsiSql.preservedInfiniteValues)
                    .mapKeys { "${it.key}::timestamp" })
        scalarAndArray(
            "TIMESTAMP WITH TIME ZONE",
            LeafAirbyteSchemaType.TIMESTAMP_WITH_TIMEZONE,
            AnsiSql.timestampWithTzValues
                .plus(AnsiSql.preservedInfiniteValues)
                .mapKeys { "${it.key}::timestamptz" })
        // Boolean type
        scalarAndArray("BOOLEAN", LeafAirbyteSchemaType.BOOLEAN, AnsiSql.booleanValues)

        // Special types
        scalarAndArray("XML", LeafAirbyteSchemaType.STRING, AnsiSql.xmlValues
            .mapKeys { "${it.key}::xml" })
    }
    /*"_bit" -> JDBCType.BIT
    "_text", "_varchar", "_name" -> JDBCType.VARCHAR
    "_char", "_bpchar" -> JDBCType.CHAR
    "_int2" -> JDBCType.SMALLINT
    "_int4", "_oid" -> JDBCType.INTEGER
    "_int8",  -> JDBCType.BIGINT
    "_numeric", "_money" -> JDBCType.NUMERIC
    "_float4" -> JDBCType.REAL
    "_float8" -> JDBCType.DOUBLE
    "_timestamptz" -> JDBCType.TIMESTAMP_WITH_TIMEZONE
    "_timestamp" -> JDBCType.TIMESTAMP
    "_timetz" -> JDBCType.TIME_WITH_TIMEZONE
    "_time" -> JDBCType.TIME
    "_date" -> JDBCType.DATE
    "_bytea" -> JDBCType.VARBINARY
    else -> JDBCType.OTHER*/

    override val setupDdl: List<String> =
        listOf("CREATE SCHEMA \"$schema\"")
            .plus(
                testCases.map {
                    """
                    CREATE TABLE "$schema"."${it.tableName}"
                    ("id" BIGSERIAL PRIMARY KEY,
                    "${it.columnName}" ${it.sqlType})
                    """.trimIndent()
                }
            )

    private fun testCase(
        sqlType: String,
        type: AirbyteSchemaType,
        values: Map<String, String>
    ): TestCase {
        return TestCase(schema, sqlType, values, type)
    }

    private fun MutableList<TestCase>.scalar(
        sqlType: String,
        type: AirbyteSchemaType,
        values: Map<String, String>
    ) {
        this.add(testCase(sqlType, type, values))
    }

    private fun MutableList<TestCase>.scalarAndArray(
        sqlType: String,
        type: AirbyteSchemaType,
        values: Map<String, String>
    ) {
        this.add(testCase(sqlType, type, values))
        this.add(testCase(
                "$sqlType[]",
                ArrayAirbyteSchemaType(type),
                values.toArrayVals()
            )
        )
    }

    // We created maps of inserted values to expected values to be used in tests of scalar types.
    // Here we flatten one of these for use in an array test, e.g.:
    //   input:  {"'1'" to "\"1\"", "'A'" to "\"A\""}
    //   output: {"['1', 'A']" to "[\"1\", \"A\"]"}
    private fun Map<String, String>.toArrayVals(): Map<String, String> {
        return mapOf(
            this.keys.joinToString(", ", "array[", "]") { it }
            to
            this.values.joinToString(",", "[", "]") { it }
        )
    }
}
