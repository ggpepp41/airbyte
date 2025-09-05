/* Copyright (c) 2025 Airbyte, Inc., all rights reserved. */
package io.airbyte.integrations.source.postgres.operations

import io.airbyte.cdk.data.LeafAirbyteSchemaType
import io.airbyte.cdk.data.LocalDateCodec
import io.airbyte.cdk.data.LocalDateTimeCodec
import io.airbyte.cdk.data.OffsetDateTimeCodec
import io.airbyte.cdk.discover.FieldType
import io.airbyte.cdk.discover.JdbcMetadataQuerier
import io.airbyte.cdk.discover.SystemType
import io.airbyte.cdk.jdbc.ArrayFieldType
import io.airbyte.cdk.jdbc.BigDecimalFieldType
import io.airbyte.cdk.jdbc.BigIntegerFieldType
import io.airbyte.cdk.jdbc.BinaryStreamFieldType
import io.airbyte.cdk.jdbc.BooleanFieldType
import io.airbyte.cdk.jdbc.BytesFieldType
import io.airbyte.cdk.jdbc.DateAccessor
import io.airbyte.cdk.jdbc.DoubleFieldType
import io.airbyte.cdk.jdbc.FloatFieldType
import io.airbyte.cdk.jdbc.IntFieldType
import io.airbyte.cdk.jdbc.JdbcFieldType
import io.airbyte.cdk.jdbc.LocalTimeFieldType
import io.airbyte.cdk.jdbc.LongFieldType
import io.airbyte.cdk.jdbc.NullFieldType
import io.airbyte.cdk.jdbc.ObjectGetter
import io.airbyte.cdk.jdbc.PokemonFieldType
import io.airbyte.cdk.jdbc.ShortFieldType
import io.airbyte.cdk.jdbc.StringFieldType
import io.airbyte.cdk.jdbc.TimestampAccessor
import io.micronaut.context.annotation.Primary
import jakarta.inject.Singleton
import java.sql.JDBCType
import java.time.OffsetDateTime

@Singleton
@Primary
class PostgresSourceFieldTypeMapper : JdbcMetadataQuerier.FieldTypeMapper {

    override fun toFieldType(c: JdbcMetadataQuerier.ColumnMetadata): FieldType =
        when (val type = c.type) {
            is SystemType -> {
                val pgType = PgSystemType(type)
                if (pgType.isArray) {
                    ArrayFieldType(scalarType(pgType))
                }
                else scalarType(pgType)
            }
            else -> PokemonFieldType
        }


    private fun scalarType(type: PgSystemType): JdbcFieldType<*> {
        return when (type.scalarJdbcType) {
            JDBCType.BIT -> if (type.precision == 1) BooleanFieldType else BytesFieldType
            JDBCType.BOOLEAN -> BooleanFieldType
            JDBCType.SMALLINT -> ShortFieldType
            JDBCType.INTEGER ->
                // oid type is unsigned and must be cast to Long to avoid truncation
                if ("oid" == type.scalarTypeName) LongFieldType
                else IntFieldType

            JDBCType.BIGINT -> LongFieldType

            JDBCType.REAL -> FloatFieldType
            JDBCType.FLOAT, JDBCType.DOUBLE -> DoubleFieldType
            JDBCType.NUMERIC, JDBCType.DECIMAL -> {
                if (type.scale == 0) BigIntegerFieldType else BigDecimalFieldType
            }

            // These types can contain infinity, -infinity and NaN, but the internal java.time type
            // does not, so they require special handling with boxing/unboxing
            JDBCType.DATE -> InfFieldType(
                LeafAirbyteSchemaType.DATE,
                DateAccessor,
                LocalDateCodec,
            )
            JDBCType.TIMESTAMP ->
                // JDBC driver reports timestamptz as TIMESTAMP instead of TIMESTAMP_WITH_TIMEZONE
                // for complex and historical reasons
                if (type.scalarTypeName == "timestamptz") InfFieldType(
                    LeafAirbyteSchemaType.TIMESTAMP_WITH_TIMEZONE,
                    ObjectGetter(OffsetDateTime::class.java),
                    OffsetDateTimeCodec,
                )
                else InfFieldType(
                    LeafAirbyteSchemaType.TIMESTAMP_WITHOUT_TIMEZONE,
                    TimestampAccessor,
                    LocalDateTimeCodec,
                )

            JDBCType.TIME -> LocalTimeFieldType
            JDBCType.CHAR, JDBCType.VARCHAR -> StringFieldType
//            JDBCType.JSON -> StringFieldType // TODO: replace this with JsonStringFieldType
//            JDBCType.TINYBLOB,
            JDBCType.BLOB,
//            JDBCType.MEDIUMBLOB,
//            JDBCType.LONGBLOB,
            JDBCType.BINARY,
            JDBCType.VARBINARY,
                /*JDBCType.GEOMETRY*/
                -> BinaryStreamFieldType

            JDBCType.NULL -> NullFieldType
//            JDBCType.VECTOR,
//            JDBCType.UNKNOWN,
            /*null*/
            else -> PokemonFieldType
        }
    }

    class PgSystemType(systemType: SystemType) {
        val isArray: Boolean = systemType.typeName!!.startsWith("_")
        val scalarTypeName =
            if (isArray) systemType.typeName!!.substring(1)
            else systemType.typeName!!
        val scalarJdbcType: JDBCType =
            if (isArray) scalarJDBCType(systemType.typeName!!)
            else systemType.jdbcType!!
        val scale = systemType.scale
        val precision = systemType.precision

        init {
            // TODO: Better user-facing message? Alert Extract team?
            require(systemType.typeName != null) { "systemType type name must not be null" }
            require(systemType.jdbcType != null) { "systemType jdbcType name must not be null" }
        }

        // JDBC metadata reports the JDBC type of all arrays as JDBCType.ARRAY. Here, we use the name
        // of the array type to determine the JDBC type of its elements.
        // Array type names are prefixed with an underscore. These names are canonical, not aliases.
        // But be warned: the same is not true of scalar types, whose type names can contain aliases.
        private fun scalarJDBCType(arrayTypeName: String): JDBCType =
            when (arrayTypeName) {
                "_bit" -> JDBCType.BIT
                "_bool" -> JDBCType.BOOLEAN
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
                else -> JDBCType.OTHER // TODO: throw exception here instead?
            }
    }

}
