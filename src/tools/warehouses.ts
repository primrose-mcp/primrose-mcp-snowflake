/**
 * Warehouse Management Tools
 *
 * MCP tools for managing Snowflake virtual warehouses.
 */

import type { McpServer } from '@modelcontextprotocol/sdk/server/mcp.js';
import { z } from 'zod';
import type { SnowflakeClient } from '../client.js';
import { formatError, formatResponse } from '../utils/formatters.js';

/**
 * Register all warehouse management tools
 *
 * @param server - MCP server instance
 * @param client - Snowflake client instance
 */
export function registerWarehouseTools(server: McpServer, client: SnowflakeClient): void {
  // ===========================================================================
  // List Warehouses
  // ===========================================================================
  server.tool(
    'snowflake_list_warehouses',
    `List all warehouses accessible to the current user.

Returns information about each warehouse including name, state (STARTED/SUSPENDED),
size, running queries, queued queries, and configuration.

Args:
  - format: Response format ('json' or 'markdown')

Returns:
  List of warehouses with their status and configuration.`,
    {
      format: z.enum(['json', 'markdown']).default('json').describe('Response format'),
    },
    async ({ format }) => {
      try {
        const result = await client.listWarehouses();
        return formatResponse(result, format, 'warehouses');
      } catch (error) {
        return formatError(error);
      }
    }
  );

  // ===========================================================================
  // Get Warehouse Status
  // ===========================================================================
  server.tool(
    'snowflake_get_warehouse',
    `Get detailed status of a specific warehouse.

Returns comprehensive information about the warehouse including state,
size, cluster count, running/queued queries, and all configuration settings.

Args:
  - name: Warehouse name (required)
  - format: Response format ('json' or 'markdown')

Returns:
  Warehouse details and current status.`,
    {
      name: z.string().describe('Warehouse name'),
      format: z.enum(['json', 'markdown']).default('json').describe('Response format'),
    },
    async ({ name, format }) => {
      try {
        const warehouse = await client.getWarehouseStatus(name);

        if (format === 'markdown') {
          const lines: string[] = [];
          lines.push(`## Warehouse: ${warehouse.name}`);
          lines.push('');
          lines.push(`**State:** ${warehouse.state}`);
          lines.push(`**Size:** ${warehouse.size || 'N/A'}`);
          lines.push(`**Type:** ${warehouse.type || 'STANDARD'}`);
          lines.push('');
          lines.push('### Configuration');
          lines.push(`- Auto Suspend: ${warehouse.autoSuspend ?? 'N/A'} seconds`);
          lines.push(`- Auto Resume: ${warehouse.autoResume ? 'Yes' : 'No'}`);
          lines.push(`- Min Clusters: ${warehouse.minClusterCount ?? 1}`);
          lines.push(`- Max Clusters: ${warehouse.maxClusterCount ?? 1}`);
          lines.push(`- Started Clusters: ${warehouse.startedClusters ?? 0}`);
          lines.push('');
          lines.push('### Activity');
          lines.push(`- Running Queries: ${warehouse.running ?? 0}`);
          lines.push(`- Queued Queries: ${warehouse.queued ?? 0}`);
          if (warehouse.resourceMonitor) {
            lines.push(`- Resource Monitor: ${warehouse.resourceMonitor}`);
          }
          lines.push('');
          lines.push('### Metadata');
          lines.push(`- Owner: ${warehouse.owner || 'N/A'}`);
          lines.push(`- Created: ${warehouse.createdOn || 'N/A'}`);
          lines.push(`- Last Resumed: ${warehouse.resumedOn || 'N/A'}`);
          if (warehouse.comment) {
            lines.push(`- Comment: ${warehouse.comment}`);
          }

          return {
            content: [{ type: 'text', text: lines.join('\n') }],
          };
        }

        return {
          content: [{ type: 'text', text: JSON.stringify(warehouse, null, 2) }],
        };
      } catch (error) {
        return formatError(error);
      }
    }
  );

  // ===========================================================================
  // Resume Warehouse
  // ===========================================================================
  server.tool(
    'snowflake_resume_warehouse',
    `Resume a suspended warehouse.

Starts a suspended warehouse so it can execute queries. The warehouse will
begin consuming credits once resumed.

Args:
  - name: Warehouse name to resume (required)

Returns:
  Confirmation of warehouse resumption.`,
    {
      name: z.string().describe('Warehouse name to resume'),
    },
    async ({ name }) => {
      try {
        const result = await client.resumeWarehouse(name);
        return {
          content: [{ type: 'text', text: JSON.stringify(result, null, 2) }],
        };
      } catch (error) {
        return formatError(error);
      }
    }
  );

  // ===========================================================================
  // Suspend Warehouse
  // ===========================================================================
  server.tool(
    'snowflake_suspend_warehouse',
    `Suspend an active warehouse.

Stops a running warehouse to conserve credits. Any running queries will
complete before the warehouse suspends.

Args:
  - name: Warehouse name to suspend (required)

Returns:
  Confirmation of warehouse suspension.`,
    {
      name: z.string().describe('Warehouse name to suspend'),
    },
    async ({ name }) => {
      try {
        const result = await client.suspendWarehouse(name);
        return {
          content: [{ type: 'text', text: JSON.stringify(result, null, 2) }],
        };
      } catch (error) {
        return formatError(error);
      }
    }
  );
}
