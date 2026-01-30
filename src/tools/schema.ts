/**
 * Schema Introspection Tools
 *
 * MCP tools for exploring Snowflake databases, schemas, tables, and views.
 */

import type { McpServer } from '@modelcontextprotocol/sdk/server/mcp.js';
import { z } from 'zod';
import type { SnowflakeClient } from '../client.js';
import { formatError, formatResponse } from '../utils/formatters.js';

/**
 * Register all schema introspection tools
 *
 * @param server - MCP server instance
 * @param client - Snowflake client instance
 */
export function registerSchemaTools(server: McpServer, client: SnowflakeClient): void {
  // ===========================================================================
  // List Databases
  // ===========================================================================
  server.tool(
    'snowflake_list_databases',
    `List all databases accessible to the current user.

Returns information about each database including name, owner, creation date,
and other metadata.

Args:
  - format: Response format ('json' or 'markdown')

Returns:
  List of databases with their metadata.`,
    {
      format: z.enum(['json', 'markdown']).default('json').describe('Response format'),
    },
    async ({ format }) => {
      try {
        const result = await client.listDatabases();
        return formatResponse(result, format, 'databases');
      } catch (error) {
        return formatError(error);
      }
    }
  );

  // ===========================================================================
  // List Schemas
  // ===========================================================================
  server.tool(
    'snowflake_list_schemas',
    `List all schemas in a database.

Returns information about each schema including name, owner, and creation date.

Args:
  - database: Database name to list schemas from (required)
  - format: Response format ('json' or 'markdown')

Returns:
  List of schemas in the specified database.`,
    {
      database: z.string().describe('Database name'),
      format: z.enum(['json', 'markdown']).default('json').describe('Response format'),
    },
    async ({ database, format }) => {
      try {
        const result = await client.listSchemas(database);
        return formatResponse(result, format, 'schemas');
      } catch (error) {
        return formatError(error);
      }
    }
  );

  // ===========================================================================
  // List Tables
  // ===========================================================================
  server.tool(
    'snowflake_list_tables',
    `List all tables in a schema.

Returns information about each table including name, row count, size,
owner, and other metadata.

Args:
  - database: Database name (required)
  - schema: Schema name (required)
  - format: Response format ('json' or 'markdown')

Returns:
  List of tables in the specified schema.`,
    {
      database: z.string().describe('Database name'),
      schema: z.string().describe('Schema name'),
      format: z.enum(['json', 'markdown']).default('json').describe('Response format'),
    },
    async ({ database, schema, format }) => {
      try {
        const result = await client.listTables(database, schema);
        return formatResponse(result, format, 'tables');
      } catch (error) {
        return formatError(error);
      }
    }
  );

  // ===========================================================================
  // List Views
  // ===========================================================================
  server.tool(
    'snowflake_list_views',
    `List all views in a schema.

Returns information about each view including name, whether it's secure,
materialized, and other metadata.

Args:
  - database: Database name (required)
  - schema: Schema name (required)
  - format: Response format ('json' or 'markdown')

Returns:
  List of views in the specified schema.`,
    {
      database: z.string().describe('Database name'),
      schema: z.string().describe('Schema name'),
      format: z.enum(['json', 'markdown']).default('json').describe('Response format'),
    },
    async ({ database, schema, format }) => {
      try {
        const result = await client.listViews(database, schema);
        return formatResponse(result, format, 'views');
      } catch (error) {
        return formatError(error);
      }
    }
  );

  // ===========================================================================
  // Describe Table
  // ===========================================================================
  server.tool(
    'snowflake_describe_table',
    `Get detailed column information for a table or view.

Returns information about each column including name, data type, nullability,
default value, and constraints.

Args:
  - database: Database name (required)
  - schema: Schema name (required)
  - table: Table or view name (required)
  - format: Response format ('json' or 'markdown')

Returns:
  List of columns with their definitions.`,
    {
      database: z.string().describe('Database name'),
      schema: z.string().describe('Schema name'),
      table: z.string().describe('Table or view name'),
      format: z.enum(['json', 'markdown']).default('json').describe('Response format'),
    },
    async ({ database, schema, table, format }) => {
      try {
        const result = await client.describeTable(database, schema, table);
        return formatResponse(result, format, 'columns');
      } catch (error) {
        return formatError(error);
      }
    }
  );
}
