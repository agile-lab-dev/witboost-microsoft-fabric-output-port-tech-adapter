class SQLSchemaMapper:
    @staticmethod
    def map_data_type(column):
        """
        Map data types from OpenMetadataColumn to SQL.
        """
        if column.dataType == "TEXT":
            return (
                f"varchar({column.dataLength})" if column.dataLength else "varchar(max)"
            )
        elif column.dataType == "INT":
            return "int"
        elif column.dataType == "DECIMAL":
            return (
                f"decimal({column.precision},{column.scale})"
                if column.precision and column.scale
                else "decimal"
            )
        elif column.dataType == "DATE":
            return "date"
        else:
            return column.dataType.lower()

    @staticmethod
    def generate_sql_schema(schema, nullable=True):
        """
        Generate SQL schema based on the provided columns.
        :param schema: List of OpenMetadataColumn objects.
        :param nullable: If True, adds 'NULL' to each column.
        :return: String with the complete SQL schema.
        """
        sql_columns = []
        for col in schema:
            sql_type = SQLSchemaMapper.map_data_type(col)
            nullability = "NULL" if nullable else "NOT NULL"
            if sql_type.startswith("varchar"):
                sql_columns.append(
                    f"\t[{col.name}] [varchar]({col.dataLength}) {nullability}"
                )
            elif "decimal" in sql_type:
                sql_columns.append(
                    f"\t[{col.name}] [decimal]({col.precision},{col.scale}) {nullability}"
                )
            else:
                sql_columns.append(f"\t[{col.name}] [{sql_type}] {nullability}")

        return ",\n".join(sql_columns)
