from pydantic import BaseModel, Field

from composio.tools.local.base import Action


class PostgresqlQueryRequest(BaseModel):
    query: str = Field(..., description="SQL query to be executed on PostgreSQL")
    connection_string: str = Field(..., description="PostgreSQL connection string")


class PostgresqlQueryResponse(BaseModel):
    execution_details: dict = Field(..., description="Execution details")
    response_data: str = Field(..., description="Result after executing the query")


class PostgresqlQuery(Action):
    """
    Executes a SQL Query on PostgreSQL and returns the results
    """

    _display_name = "Execute a PostgreSQL query"
    _request_schema = PostgresqlQueryRequest
    _response_schema = PostgresqlQueryResponse
    _tags = ["sql", "postgresql", "sql_query"]
    _tool_name = "postgresqltool"

    def execute(
        self, request_data: PostgresqlQueryRequest, authorisation_data: dict = {}
    ) -> dict:
        # Import PostgreSQL-specific libraries inside the execute function
        import psycopg2  # pylint: disable=import-outside-toplevel
        from psycopg2 import sql  # pylint: disable=import-outside-toplevel

        try:
            # Connect to the PostgreSQL database
            with psycopg2.connect(request_data.connection_string) as connection:
                with connection.cursor() as cursor:
                    # Execute the query
                    cursor.execute(sql.SQL(request_data.query))

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
