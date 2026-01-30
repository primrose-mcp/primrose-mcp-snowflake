# Snowflake MCP Server

[![Primrose MCP](https://img.shields.io/badge/Primrose-MCP-blue)](https://primrose.dev/mcp/snowflake)
[![TypeScript](https://img.shields.io/badge/TypeScript-5.0-blue)](https://www.typescriptlang.org/)
[![Cloudflare Workers](https://img.shields.io/badge/Cloudflare-Workers-orange)](https://workers.cloudflare.com/)

A Model Context Protocol (MCP) server for Snowflake, enabling data warehouse queries, schema exploration, and warehouse management.

## Features

- **Statements** - SQL statement execution and query management
- **Schema** - Database schema exploration and metadata
- **Warehouses** - Virtual warehouse management

## Quick Start

### Recommended: Primrose SDK

The easiest way to use this MCP server is with the Primrose SDK:

```bash
npm install primrose-mcp
```

```typescript
import { PrimroseMCP } from 'primrose-mcp';

const client = new PrimroseMCP({
  server: 'snowflake',
  credentials: {
    account: 'xy12345.us-east-1',
    token: 'your-jwt-token'
  }
});
```

### Manual Installation

1. Clone the repository
2. Install dependencies:
   ```bash
   npm install
   ```
3. Deploy to Cloudflare Workers:
   ```bash
   npm run deploy
   ```

## Configuration

### Required Headers

| Header | Description |
|--------|-------------|
| `X-Snowflake-Account` | Account identifier (e.g., xy12345.us-east-1) |
| `X-Snowflake-Token` | JWT token for authentication |

### Optional Headers

| Header | Description |
|--------|-------------|
| `X-Snowflake-Warehouse` | Default warehouse for queries |
| `X-Snowflake-Database` | Default database |
| `X-Snowflake-Schema` | Default schema |
| `X-Snowflake-Role` | Role to use for the session |

## Available Tools

### Statements
- `snowflake_execute_statement` - Execute a SQL statement
- `snowflake_get_statement_status` - Check statement execution status
- `snowflake_get_statement_result` - Get query results
- `snowflake_cancel_statement` - Cancel a running statement
- `snowflake_list_statements` - List recent statements

### Schema
- `snowflake_list_databases` - List all databases
- `snowflake_list_schemas` - List schemas in a database
- `snowflake_list_tables` - List tables in a schema
- `snowflake_list_views` - List views in a schema
- `snowflake_describe_table` - Get table structure
- `snowflake_list_columns` - List columns in a table
- `snowflake_get_table_ddl` - Get table DDL

### Warehouses
- `snowflake_list_warehouses` - List virtual warehouses
- `snowflake_get_warehouse` - Get warehouse details
- `snowflake_resume_warehouse` - Resume a suspended warehouse
- `snowflake_suspend_warehouse` - Suspend a warehouse
- `snowflake_resize_warehouse` - Change warehouse size

## Development

```bash
# Install dependencies
npm install

# Run locally
npm run dev

# Type checking
npm run typecheck

# Deploy to Cloudflare
npm run deploy
```

## Related Resources

- [Primrose SDK Documentation](https://primrose.dev/docs)
- [Snowflake SQL REST API Documentation](https://docs.snowflake.com/en/developer-guide/sql-api)
- [Snowflake Developer Portal](https://developers.snowflake.com/)
- [Model Context Protocol](https://modelcontextprotocol.io/)
