from composio import action


@action(toolname="pg_tool", requires=["psycopg2"])
def run_postgresql_query(connection_string: str, query: str) -> dict:
    """
    Postgresql will run any query.

    :param connection_string: Postgres connection string
    :param query: SQL query to run.
    :return response: dict message.
    """
    # Import PostgreSQL-specific libraries inside the execute function
    import psycopg2  # pylint: disable=import-outside-toplevel
    from psycopg2 import sql  # pylint: disable=import-outside-toplevel

    try:
        # Connect to the PostgreSQL database
        with psycopg2.connect(connection_string) as connection:
            with connection.cursor() as cursor:
                # Execute the query
                cursor.execute(sql.SQL(query))

                # Fetch all rows for SELECT queries
                if cursor.description:
                    response_data = cursor.fetchall()
                else:
                    response_data = "Query executed successfully. No data to fetch."

                connection.commit()

        # Prepare the response data
        return {
            "execution_details": {"executed": True},
            "response_data": response_data,
        }
    except psycopg2.Error as e:
        print(f"PostgreSQL error: {str(e)}")

        return {
            "execution_details": {"executed": False},
            "response_data": f"PostgreSQL error: {str(e)}",
        }


@action(toolname="pg_tool", requires=["psycopg2"])
def get_postgresql_table_info(connection_string: str, table_name: str) -> dict:
    """
    Get the table information from the PostgreSQL database.

    :param connection_string: Postgres connection string
    :param table_name: Table name to get information.
    :return response: dict message.
    """
    # Import PostgreSQL-specific libraries inside the execute function
    import psycopg2  # pylint: disable=import-outside-toplevel
    from psycopg2 import sql  # pylint: disable=import-outside-toplevel

    # Query to get table information
    get_table_info_query = f"""
        SELECT table_name, column_name, data_type, column_default, is_nullable
        FROM information_schema.columns
        WHERE table_schema = 'public' and table_name = '{table_name}'
        ORDER BY table_name, ordinal_position
    """
    # Query to get table constraints
    get_table_constraints_query = f"""
        SELECT tc.table_name, tc.constraint_name, tc.constraint_type, 
               kcu.column_name, ccu.table_name AS foreign_table_name, 
               ccu.column_name AS foreign_column_name
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu 
          ON tc.constraint_name = kcu.constraint_name
        LEFT JOIN information_schema.constraint_column_usage ccu 
          ON ccu.constraint_name = tc.constraint_name
        WHERE tc.table_schema = 'public' and tc.table_name = '{table_name}'
    """

    try:
        # Connect to the PostgreSQL database
        with psycopg2.connect(connection_string) as connection:
            with connection.cursor() as cursor:
                # Execute the query
                cursor.execute(sql.SQL(get_table_info_query))
                columns = cursor.fetchall()
                cursor.execute(sql.SQL(get_table_constraints_query))
                constraints = cursor.fetchall()

        response_data = {
            "table_name": table_name,
            "columns": columns,
            "constraints": constraints,
        }
        # Prepare the response data
        return {
            "execution_details": {"executed": True},
            "response_data": response_data,
        }
    except psycopg2.Error as e:
        print(f"PostgreSQL error: {str(e)}")

        return {
            "execution_details": {"executed": False},
            "response_data": f"PostgreSQL error: {str(e)}",
        }
