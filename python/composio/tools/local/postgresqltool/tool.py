from typing import Type

from composio.tools.local.base import Action, Tool

from .actions.postgresql_query import PostgresqlQuery


class PostgresqlTool(Tool):
    """
    This class enables us to execute SQL queries in a PostgreSQL database
    """

    def actions(self) -> list[Type[Action]]:
        return [PostgresqlQuery]

    def triggers(self) -> list:
        return []
