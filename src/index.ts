/**
 * Snowflake MCP Server - Main Entry Point
 *
 * This file sets up the MCP server using Cloudflare's Agents SDK.
 * It supports both stateless (McpServer) and stateful (McpAgent) modes.
 *
 * MULTI-TENANT ARCHITECTURE:
 * Tenant credentials (account identifiers, JWT tokens) are parsed from request headers,
 * allowing a single server deployment to serve multiple customers.
 *
 * Required Headers:
 * - X-Snowflake-Account: Snowflake account identifier
 * - X-Snowflake-Token: JWT token for authentication
 *
 * Optional Headers:
 * - X-Snowflake-Warehouse: Default warehouse for queries
 * - X-Snowflake-Database: Default database
 * - X-Snowflake-Schema: Default schema
 * - X-Snowflake-Role: Role to use for the session
 */

import { McpServer } from '@modelcontextprotocol/sdk/server/mcp.js';
import { McpAgent } from 'agents/mcp';
import { createSnowflakeClient } from './client.js';
import { registerSchemaTools } from './tools/schema.js';
import { registerStatementTools } from './tools/statements.js';
import { registerWarehouseTools } from './tools/warehouses.js';
import {
  type Env,
  type TenantCredentials,
  parseTenantCredentials,
  validateCredentials,
} from './types/env.js';

// =============================================================================
// MCP Server Configuration
// =============================================================================

const SERVER_NAME = 'primrose-mcp-snowflake';
const SERVER_VERSION = '1.0.0';

// =============================================================================
// MCP Agent (Stateful - uses Durable Objects)
// =============================================================================

/**
 * McpAgent provides stateful MCP sessions backed by Durable Objects.
 *
 * NOTE: For multi-tenant deployments, use the stateless mode (Option 2) instead.
 * The stateful McpAgent is better suited for single-tenant deployments where
 * credentials can be stored as wrangler secrets.
 *
 * @deprecated For multi-tenant support, use stateless mode with per-request credentials
 */
export class SnowflakeMcpAgent extends McpAgent<Env> {
  server = new McpServer({
    name: SERVER_NAME,
    version: SERVER_VERSION,
  });

  async init() {
    // NOTE: Stateful mode requires credentials to be configured differently.
    // For multi-tenant, use the stateless /mcp endpoint instead.
    throw new Error(
      'Stateful mode (McpAgent) is not supported for multi-tenant deployments. ' +
        'Use the stateless /mcp endpoint with X-Snowflake-Account and X-Snowflake-Token headers instead.'
    );
  }
}

// =============================================================================
// Stateless MCP Server (Recommended - no Durable Objects needed)
// =============================================================================

/**
 * Creates a stateless MCP server instance with tenant-specific credentials.
 *
 * MULTI-TENANT: Each request provides credentials via headers, allowing
 * a single server deployment to serve multiple tenants.
 *
 * @param credentials - Tenant credentials parsed from request headers
 */
function createStatelessServer(credentials: TenantCredentials): McpServer {
  const server = new McpServer({
    name: SERVER_NAME,
    version: SERVER_VERSION,
  });

  // Create client with tenant-specific credentials
  const client = createSnowflakeClient(credentials);

  // Register all tools
  registerStatementTools(server, client);
  registerSchemaTools(server, client);
  registerWarehouseTools(server, client);

  // Test connection tool
  server.tool(
    'snowflake_test_connection',
    'Test the connection to Snowflake using the provided credentials.',
    {},
    async () => {
      try {
        const result = await client.testConnection();
        return {
          content: [{ type: 'text', text: JSON.stringify(result, null, 2) }],
        };
      } catch (error) {
        return {
          content: [
            {
              type: 'text',
              text: `Connection failed: ${error instanceof Error ? error.message : 'Unknown error'}`,
            },
          ],
          isError: true,
        };
      }
    }
  );

  return server;
}

// =============================================================================
// Worker Export
// =============================================================================

export default {
  /**
   * Main fetch handler for the Worker
   */
  async fetch(request: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
    const url = new URL(request.url);

    // Health check endpoint
    if (url.pathname === '/health') {
      return new Response(JSON.stringify({ status: 'ok', server: SERVER_NAME }), {
        headers: { 'Content-Type': 'application/json' },
      });
    }

    // ==========================================================================
    // Option 1: Stateful MCP with McpAgent (requires Durable Objects)
    // ==========================================================================
    // Uncomment to use McpAgent for stateful sessions:
    //
    // if (url.pathname === '/sse' || url.pathname === '/mcp') {
    //   return SnowflakeMcpAgent.serveSSE('/sse').fetch(request, env, ctx);
    // }

    // ==========================================================================
    // Option 2: Stateless MCP with Streamable HTTP (Recommended for multi-tenant)
    // ==========================================================================
    if (url.pathname === '/mcp' && request.method === 'POST') {
      // Parse tenant credentials from request headers
      const credentials = parseTenantCredentials(request);

      // Validate credentials are present
      try {
        validateCredentials(credentials);
      } catch (error) {
        return new Response(
          JSON.stringify({
            error: 'Unauthorized',
            message: error instanceof Error ? error.message : 'Invalid credentials',
            required_headers: ['X-Snowflake-Account', 'X-Snowflake-Token'],
          }),
          {
            status: 401,
            headers: { 'Content-Type': 'application/json' },
          }
        );
      }

      // Create server with tenant-specific credentials
      const server = createStatelessServer(credentials);

      // Import and use createMcpHandler for streamable HTTP
      const { createMcpHandler } = await import('agents/mcp');
      const handler = createMcpHandler(server);
      return handler(request, env, ctx);
    }

    // SSE endpoint for legacy clients
    if (url.pathname === '/sse') {
      return new Response('SSE endpoint requires Durable Objects. Enable in wrangler.jsonc.', {
        status: 501,
      });
    }

    // Default response - API documentation
    return new Response(
      JSON.stringify({
        name: SERVER_NAME,
        version: SERVER_VERSION,
        description: 'Multi-tenant Snowflake MCP Server for SQL API access',
        endpoints: {
          mcp: '/mcp (POST) - Streamable HTTP MCP endpoint',
          health: '/health - Health check',
        },
        authentication: {
          description: 'Pass tenant credentials via request headers',
          required_headers: {
            'X-Snowflake-Account':
              'Snowflake account identifier (e.g., xy12345.us-east-1)',
            'X-Snowflake-Token': 'JWT token for key-pair authentication',
          },
          optional_headers: {
            'X-Snowflake-Warehouse': 'Default warehouse for query execution',
            'X-Snowflake-Database': 'Default database context',
            'X-Snowflake-Schema': 'Default schema context',
            'X-Snowflake-Role': 'Role to use for the session',
          },
        },
        tools: {
          statement_execution: [
            'snowflake_execute - Execute SQL synchronously',
            'snowflake_execute_async - Execute SQL asynchronously',
            'snowflake_get_status - Check async statement status',
            'snowflake_get_result - Retrieve statement results',
            'snowflake_cancel - Cancel a running statement',
          ],
          schema_introspection: [
            'snowflake_list_databases - List all databases',
            'snowflake_list_schemas - List schemas in a database',
            'snowflake_list_tables - List tables in a schema',
            'snowflake_list_views - List views in a schema',
            'snowflake_describe_table - Get column definitions',
          ],
          warehouse_management: [
            'snowflake_list_warehouses - List all warehouses',
            'snowflake_get_warehouse - Get warehouse status',
            'snowflake_resume_warehouse - Resume a warehouse',
            'snowflake_suspend_warehouse - Suspend a warehouse',
          ],
          connection: ['snowflake_test_connection - Test connection'],
        },
      }),
      {
        headers: { 'Content-Type': 'application/json' },
      }
    );
  },
};
