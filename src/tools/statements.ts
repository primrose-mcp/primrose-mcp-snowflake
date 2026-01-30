/**
 * SQL Statement Execution Tools
 *
 * MCP tools for executing SQL statements and managing query execution.
 */

import type { McpServer } from '@modelcontextprotocol/sdk/server/mcp.js';
import { z } from 'zod';
import type { SnowflakeClient } from '../client.js';
import { formatError, formatStatementResultAsMarkdown } from '../utils/formatters.js';

/**
 * Register all SQL statement execution tools
 *
 * @param server - MCP server instance
 * @param client - Snowflake client instance
 */
export function registerStatementTools(server: McpServer, client: SnowflakeClient): void {
  // ===========================================================================
  // Execute SQL Statement (Synchronous)
  // ===========================================================================
  server.tool(
    'snowflake_execute',
    `Execute a SQL statement synchronously in Snowflake.

This tool executes a SQL query and waits for the result. Use this for queries
that complete quickly (under 45 seconds). For long-running queries, use
snowflake_execute_async instead.

Args:
  - statement: SQL statement to execute (required)
  - database: Database context for the query
  - schema: Schema context for the query
  - warehouse: Warehouse to use for execution
  - role: Role to use for the session
  - timeout: Query timeout in seconds (default: 60)
  - format: Response format ('json' or 'markdown')

Returns:
  Query results including columns, data, and execution statistics.

Example:
  statement: "SELECT * FROM my_table LIMIT 10"
  database: "MY_DB"
  schema: "PUBLIC"`,
    {
      statement: z.string().describe('SQL statement to execute'),
      database: z.string().optional().describe('Database context'),
      schema: z.string().optional().describe('Schema context'),
      warehouse: z.string().optional().describe('Warehouse to use'),
      role: z.string().optional().describe('Role to use'),
      timeout: z.number().int().min(1).max(3600).default(60).describe('Timeout in seconds'),
      format: z.enum(['json', 'markdown']).default('json').describe('Response format'),
    },
    async ({ statement, database, schema, warehouse, role, timeout, format }) => {
      try {
        const result = await client.executeStatement({
          statement,
          database,
          schema,
          warehouse,
          role,
          timeout,
        });

        if (format === 'markdown') {
          return {
            content: [{ type: 'text', text: formatStatementResultAsMarkdown(result) }],
          };
        }

        return {
          content: [{ type: 'text', text: JSON.stringify(result, null, 2) }],
        };
      } catch (error) {
        return formatError(error);
      }
    }
  );

  // ===========================================================================
  // Execute SQL Statement (Asynchronous)
  // ===========================================================================
  server.tool(
    'snowflake_execute_async',
    `Execute a SQL statement asynchronously in Snowflake.

This tool submits a SQL query for asynchronous execution and returns immediately
with a statement handle. Use snowflake_get_status to check execution progress
and snowflake_get_result to retrieve results when ready.

Args:
  - statement: SQL statement to execute (required)
  - database: Database context for the query
  - schema: Schema context for the query
  - warehouse: Warehouse to use for execution
  - role: Role to use for the session
  - timeout: Query timeout in seconds (default: 3600)

Returns:
  Statement handle and status URL for polling.

Example:
  statement: "SELECT * FROM large_table"`,
    {
      statement: z.string().describe('SQL statement to execute'),
      database: z.string().optional().describe('Database context'),
      schema: z.string().optional().describe('Schema context'),
      warehouse: z.string().optional().describe('Warehouse to use'),
      role: z.string().optional().describe('Role to use'),
      timeout: z.number().int().min(1).max(86400).default(3600).describe('Timeout in seconds'),
    },
    async ({ statement, database, schema, warehouse, role, timeout }) => {
      try {
        const result = await client.executeStatementAsync({
          statement,
          database,
          schema,
          warehouse,
          role,
          timeout,
        });

        return {
          content: [
            {
              type: 'text',
              text: JSON.stringify(
                {
                  statementHandle: result.statementHandle,
                  status: result.status,
                  message: 'Statement submitted for async execution. Use snowflake_get_status to check progress.',
                  statementStatusUrl: result.statementStatusUrl,
                },
                null,
                2
              ),
            },
          ],
        };
      } catch (error) {
        return formatError(error);
      }
    }
  );

  // ===========================================================================
  // Get Statement Status
  // ===========================================================================
  server.tool(
    'snowflake_get_status',
    `Check the execution status of an async SQL statement.

Use this tool to poll for completion of statements submitted with
snowflake_execute_async.

Args:
  - statementHandle: The statement handle from async execution (required)

Returns:
  Current execution status (running, queued, success, failed_with_error, etc.)`,
    {
      statementHandle: z.string().describe('Statement handle from async execution'),
    },
    async ({ statementHandle }) => {
      try {
        const result = await client.getStatementStatus(statementHandle);

        return {
          content: [
            {
              type: 'text',
              text: JSON.stringify(
                {
                  statementHandle: result.statementHandle,
                  status: result.status,
                  message: result.message,
                  hasResults: result.status === 'success' && result.data !== undefined,
                  numRows: result.resultSetMetaData?.numRows,
                },
                null,
                2
              ),
            },
          ],
        };
      } catch (error) {
        return formatError(error);
      }
    }
  );

  // ===========================================================================
  // Get Statement Result
  // ===========================================================================
  server.tool(
    'snowflake_get_result',
    `Retrieve results from a completed SQL statement.

Use this tool to fetch results after a statement has completed execution.
For large result sets, use the partition parameter to fetch additional data.

Args:
  - statementHandle: The statement handle (required)
  - partition: Partition number for large result sets (default: 0)
  - format: Response format ('json' or 'markdown')

Returns:
  Query results including columns, data, and partition information.`,
    {
      statementHandle: z.string().describe('Statement handle'),
      partition: z.number().int().min(0).default(0).describe('Partition number'),
      format: z.enum(['json', 'markdown']).default('json').describe('Response format'),
    },
    async ({ statementHandle, partition, format }) => {
      try {
        const result = await client.getStatementResult(statementHandle, partition);

        if (format === 'markdown') {
          return {
            content: [{ type: 'text', text: formatStatementResultAsMarkdown(result) }],
          };
        }

        return {
          content: [{ type: 'text', text: JSON.stringify(result, null, 2) }],
        };
      } catch (error) {
        return formatError(error);
      }
    }
  );

  // ===========================================================================
  // Cancel Statement
  // ===========================================================================
  server.tool(
    'snowflake_cancel',
    `Cancel a running SQL statement.

Use this tool to terminate execution of a statement that is still running.

Args:
  - statementHandle: The statement handle to cancel (required)

Returns:
  Confirmation of cancellation.`,
    {
      statementHandle: z.string().describe('Statement handle to cancel'),
    },
    async ({ statementHandle }) => {
      try {
        const result = await client.cancelStatement(statementHandle);

        return {
          content: [{ type: 'text', text: JSON.stringify(result, null, 2) }],
        };
      } catch (error) {
        return formatError(error);
      }
    }
  );
}
