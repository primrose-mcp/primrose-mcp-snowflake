/**
 * Response Formatting Utilities
 *
 * Helpers for formatting tool responses in JSON or Markdown.
 */

import type {
  Column,
  Database,
  PaginatedResponse,
  ResponseFormat,
  Schema,
  StatementResult,
  Table,
  View,
  Warehouse,
} from '../types/entities.js';
import { SnowflakeApiError, formatErrorForLogging } from './errors.js';

/**
 * MCP tool response type
 * Note: Index signature required for MCP SDK 1.25+ compatibility
 */
export interface ToolResponse {
  [key: string]: unknown;
  content: Array<{ type: 'text'; text: string }>;
  isError?: boolean;
}

/**
 * Format a successful response
 */
export function formatResponse(
  data: unknown,
  format: ResponseFormat,
  entityType: string
): ToolResponse {
  if (format === 'markdown') {
    return {
      content: [{ type: 'text', text: formatAsMarkdown(data, entityType) }],
    };
  }
  return {
    content: [{ type: 'text', text: JSON.stringify(data, null, 2) }],
  };
}

/**
 * Format an error response
 */
export function formatError(error: unknown): ToolResponse {
  const errorInfo = formatErrorForLogging(error);

  let message: string;
  if (error instanceof SnowflakeApiError) {
    message = `Error: ${error.message}`;
    if (error.retryable) {
      message += ' (retryable)';
    }
    if (error.sqlState) {
      message += ` [SQL State: ${error.sqlState}]`;
    }
  } else if (error instanceof Error) {
    message = `Error: ${error.message}`;
  } else {
    message = `Error: ${String(error)}`;
  }

  return {
    content: [
      {
        type: 'text',
        text: JSON.stringify({ error: message, details: errorInfo }, null, 2),
      },
    ],
    isError: true,
  };
}

/**
 * Format data as Markdown
 */
function formatAsMarkdown(data: unknown, entityType: string): string {
  if (isPaginatedResponse(data)) {
    return formatPaginatedAsMarkdown(data, entityType);
  }

  if (Array.isArray(data)) {
    return formatArrayAsMarkdown(data, entityType);
  }

  if (typeof data === 'object' && data !== null) {
    return formatObjectAsMarkdown(data as Record<string, unknown>, entityType);
  }

  return String(data);
}

/**
 * Type guard for paginated response
 */
function isPaginatedResponse(data: unknown): data is PaginatedResponse<unknown> {
  return (
    typeof data === 'object' &&
    data !== null &&
    'items' in data &&
    Array.isArray((data as PaginatedResponse<unknown>).items)
  );
}

/**
 * Format paginated response as Markdown
 */
function formatPaginatedAsMarkdown(data: PaginatedResponse<unknown>, entityType: string): string {
  const lines: string[] = [];

  lines.push(`## ${capitalize(entityType)}`);
  lines.push('');

  if (data.total !== undefined) {
    lines.push(`**Total:** ${data.total} | **Showing:** ${data.count}`);
  } else {
    lines.push(`**Showing:** ${data.count}`);
  }

  if (data.hasMore) {
    lines.push(`**More available:** Yes (partition: ${data.nextPartition})`);
  }
  lines.push('');

  if (data.items.length === 0) {
    lines.push('_No items found._');
    return lines.join('\n');
  }

  // Format items based on entity type
  switch (entityType) {
    case 'databases':
      lines.push(formatDatabasesTable(data.items as Database[]));
      break;
    case 'schemas':
      lines.push(formatSchemasTable(data.items as Schema[]));
      break;
    case 'tables':
      lines.push(formatTablesTable(data.items as Table[]));
      break;
    case 'views':
      lines.push(formatViewsTable(data.items as View[]));
      break;
    case 'columns':
      lines.push(formatColumnsTable(data.items as Column[]));
      break;
    case 'warehouses':
      lines.push(formatWarehousesTable(data.items as Warehouse[]));
      break;
    default:
      lines.push(formatGenericTable(data.items));
  }

  return lines.join('\n');
}

/**
 * Format databases as Markdown table
 */
function formatDatabasesTable(databases: Database[]): string {
  const lines: string[] = [];
  lines.push('| Name | Owner | Origin | Created |');
  lines.push('|---|---|---|---|');

  for (const db of databases) {
    lines.push(
      `| ${db.name} | ${db.owner || '-'} | ${db.origin || '-'} | ${db.createdOn || '-'} |`
    );
  }

  return lines.join('\n');
}

/**
 * Format schemas as Markdown table
 */
function formatSchemasTable(schemas: Schema[]): string {
  const lines: string[] = [];
  lines.push('| Name | Database | Owner | Created |');
  lines.push('|---|---|---|---|');

  for (const schema of schemas) {
    lines.push(
      `| ${schema.name} | ${schema.databaseName || '-'} | ${schema.owner || '-'} | ${schema.createdOn || '-'} |`
    );
  }

  return lines.join('\n');
}

/**
 * Format tables as Markdown table
 */
function formatTablesTable(tables: Table[]): string {
  const lines: string[] = [];
  lines.push('| Name | Kind | Rows | Bytes | Owner |');
  lines.push('|---|---|---|---|---|');

  for (const table of tables) {
    const rows = table.rows !== undefined ? table.rows.toLocaleString() : '-';
    const bytes = table.bytes !== undefined ? formatBytes(table.bytes) : '-';
    lines.push(
      `| ${table.name} | ${table.kind || 'TABLE'} | ${rows} | ${bytes} | ${table.owner || '-'} |`
    );
  }

  return lines.join('\n');
}

/**
 * Format views as Markdown table
 */
function formatViewsTable(views: View[]): string {
  const lines: string[] = [];
  lines.push('| Name | Secure | Materialized | Owner | Created |');
  lines.push('|---|---|---|---|---|');

  for (const view of views) {
    lines.push(
      `| ${view.name} | ${view.isSecure || 'N'} | ${view.isMaterialized || 'N'} | ${view.owner || '-'} | ${view.createdOn || '-'} |`
    );
  }

  return lines.join('\n');
}

/**
 * Format columns as Markdown table
 */
function formatColumnsTable(columns: Column[]): string {
  const lines: string[] = [];
  lines.push('| Name | Type | Nullable | Default | PK |');
  lines.push('|---|---|---|---|---|');

  for (const col of columns) {
    lines.push(
      `| ${col.name} | ${col.type} | ${col.nullable ? 'YES' : 'NO'} | ${col.default || '-'} | ${col.primaryKey ? 'YES' : '-'} |`
    );
  }

  return lines.join('\n');
}

/**
 * Format warehouses as Markdown table
 */
function formatWarehousesTable(warehouses: Warehouse[]): string {
  const lines: string[] = [];
  lines.push('| Name | State | Size | Running | Queued |');
  lines.push('|---|---|---|---|---|');

  for (const wh of warehouses) {
    lines.push(
      `| ${wh.name} | ${wh.state} | ${wh.size || '-'} | ${wh.running ?? '-'} | ${wh.queued ?? '-'} |`
    );
  }

  return lines.join('\n');
}

/**
 * Format a generic array as Markdown table
 */
function formatGenericTable(items: unknown[]): string {
  if (items.length === 0) return '_No items_';

  const first = items[0] as Record<string, unknown>;
  const keys = Object.keys(first).slice(0, 6); // Limit columns

  const lines: string[] = [];
  lines.push(`| ${keys.join(' | ')} |`);
  lines.push(`|${keys.map(() => '---').join('|')}|`);

  for (const item of items) {
    const record = item as Record<string, unknown>;
    const values = keys.map((k) => {
      const v = record[k];
      if (v === null || v === undefined) return '-';
      if (typeof v === 'object') return JSON.stringify(v);
      return String(v);
    });
    lines.push(`| ${values.join(' | ')} |`);
  }

  return lines.join('\n');
}

/**
 * Format an array as Markdown
 */
function formatArrayAsMarkdown(data: unknown[], entityType: string): string {
  switch (entityType) {
    case 'databases':
      return formatDatabasesTable(data as Database[]);
    case 'schemas':
      return formatSchemasTable(data as Schema[]);
    case 'tables':
      return formatTablesTable(data as Table[]);
    case 'views':
      return formatViewsTable(data as View[]);
    case 'columns':
      return formatColumnsTable(data as Column[]);
    case 'warehouses':
      return formatWarehousesTable(data as Warehouse[]);
    default:
      return formatGenericTable(data);
  }
}

/**
 * Format statement result as Markdown
 */
export function formatStatementResultAsMarkdown(result: StatementResult): string {
  const lines: string[] = [];

  lines.push('## Query Result');
  lines.push('');
  lines.push(`**Statement Handle:** \`${result.statementHandle}\``);
  lines.push(`**Status:** ${result.status}`);

  if (result.stats) {
    const stats = result.stats;
    lines.push('');
    lines.push('### Statistics');
    if (stats.numRowsScanned !== undefined) lines.push(`- Rows Scanned: ${stats.numRowsScanned}`);
    if (stats.numRowsInserted !== undefined) lines.push(`- Rows Inserted: ${stats.numRowsInserted}`);
    if (stats.numRowsUpdated !== undefined) lines.push(`- Rows Updated: ${stats.numRowsUpdated}`);
    if (stats.numRowsDeleted !== undefined) lines.push(`- Rows Deleted: ${stats.numRowsDeleted}`);
    if (stats.elapsedTimeMs !== undefined) lines.push(`- Elapsed Time: ${stats.elapsedTimeMs}ms`);
  }

  if (result.resultSetMetaData && result.data) {
    lines.push('');
    lines.push(`### Results (${result.resultSetMetaData.numRows} rows)`);
    lines.push('');

    const columns = result.resultSetMetaData.rowType;
    const headers = columns.map((c) => c.name);
    lines.push(`| ${headers.join(' | ')} |`);
    lines.push(`|${columns.map(() => '---').join('|')}|`);

    for (const row of result.data.slice(0, 100)) {
      // Limit to 100 rows
      const values = row.map((v) => (v === null ? 'NULL' : String(v)));
      lines.push(`| ${values.join(' | ')} |`);
    }

    if (result.data.length > 100) {
      lines.push('');
      lines.push(`_Showing first 100 of ${result.data.length} rows_`);
    }
  }

  return lines.join('\n');
}

/**
 * Format a single object as Markdown
 */
function formatObjectAsMarkdown(data: Record<string, unknown>, entityType: string): string {
  const lines: string[] = [];
  lines.push(`## ${capitalize(entityType.replace(/s$/, ''))}`);
  lines.push('');

  for (const [key, value] of Object.entries(data)) {
    if (value === null || value === undefined) continue;

    if (typeof value === 'object') {
      lines.push(`**${formatKey(key)}:**`);
      lines.push('```json');
      lines.push(JSON.stringify(value, null, 2));
      lines.push('```');
    } else {
      lines.push(`**${formatKey(key)}:** ${value}`);
    }
  }

  return lines.join('\n');
}

/**
 * Capitalize first letter
 */
function capitalize(str: string): string {
  return str.charAt(0).toUpperCase() + str.slice(1);
}

/**
 * Format a key for display (camelCase to Title Case)
 */
function formatKey(key: string): string {
  return key
    .replace(/([A-Z])/g, ' $1')
    .replace(/^./, (str) => str.toUpperCase())
    .trim();
}

/**
 * Format bytes to human readable string
 */
function formatBytes(bytes: number): string {
  if (bytes === 0) return '0 B';
  const k = 1024;
  const sizes = ['B', 'KB', 'MB', 'GB', 'TB'];
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  return `${parseFloat((bytes / Math.pow(k, i)).toFixed(2))} ${sizes[i]}`;
}
