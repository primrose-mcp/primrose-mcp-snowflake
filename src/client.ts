/**
 * Snowflake SQL API Client
 *
 * Handles all HTTP communication with the Snowflake SQL API.
 *
 * MULTI-TENANT: This client receives credentials per-request via TenantCredentials,
 * allowing a single server to serve multiple tenants with different accounts/tokens.
 *
 * API Reference: https://docs.snowflake.com/en/developer-guide/sql-api/reference
 */

import type {
  Column,
  Database,
  PaginatedResponse,
  Schema,
  StatementRequest,
  StatementResult,
  StatementStatus,
  Table,
  View,
  Warehouse,
} from './types/entities.js';
import type { TenantCredentials } from './types/env.js';
import {
  AuthenticationError,
  RateLimitError,
  SnowflakeApiError,
  StatementError,
  TimeoutError,
} from './utils/errors.js';

// =============================================================================
// Snowflake Client Interface
// =============================================================================

export interface SnowflakeClient {
  // Connection
  testConnection(): Promise<{ connected: boolean; message: string; account: string }>;

  // SQL Statement Execution
  executeStatement(request: StatementRequest): Promise<StatementResult>;
  executeStatementAsync(request: StatementRequest): Promise<StatementResult>;
  getStatementStatus(statementHandle: string): Promise<StatementResult>;
  getStatementResult(statementHandle: string, partition?: number): Promise<StatementResult>;
  cancelStatement(statementHandle: string): Promise<{ success: boolean; message: string }>;

  // Schema Introspection
  listDatabases(): Promise<PaginatedResponse<Database>>;
  listSchemas(database: string): Promise<PaginatedResponse<Schema>>;
  listTables(database: string, schema: string): Promise<PaginatedResponse<Table>>;
  listViews(database: string, schema: string): Promise<PaginatedResponse<View>>;
  describeTable(database: string, schema: string, table: string): Promise<PaginatedResponse<Column>>;

  // Warehouse Management
  listWarehouses(): Promise<PaginatedResponse<Warehouse>>;
  getWarehouseStatus(warehouseName: string): Promise<Warehouse>;
  resumeWarehouse(warehouseName: string): Promise<{ success: boolean; message: string }>;
  suspendWarehouse(warehouseName: string): Promise<{ success: boolean; message: string }>;
}

// =============================================================================
// Snowflake Client Implementation
// =============================================================================

class SnowflakeClientImpl implements SnowflakeClient {
  private credentials: TenantCredentials;
  private baseUrl: string;

  constructor(credentials: TenantCredentials) {
    this.credentials = credentials;
    // Snowflake SQL API base URL format
    this.baseUrl = `https://${credentials.account}.snowflakecomputing.com/api/v2`;
  }

  // ===========================================================================
  // HTTP Request Helper
  // ===========================================================================

  private getAuthHeaders(): Record<string, string> {
    return {
      Authorization: `Bearer ${this.credentials.token}`,
      'Content-Type': 'application/json',
      Accept: 'application/json',
      'X-Snowflake-Authorization-Token-Type': 'KEYPAIR_JWT',
      'User-Agent': 'primrose-mcp-snowflake/1.0.0',
    };
  }

  private async request<T>(
    endpoint: string,
    options: RequestInit = {},
    queryParams?: Record<string, string>
  ): Promise<T> {
    let url = `${this.baseUrl}${endpoint}`;

    if (queryParams) {
      const params = new URLSearchParams(queryParams);
      url = `${url}?${params.toString()}`;
    }

    const response = await fetch(url, {
      ...options,
      headers: {
        ...this.getAuthHeaders(),
        ...(options.headers || {}),
      },
    });

    // Handle rate limiting (429)
    if (response.status === 429) {
      const retryAfter = response.headers.get('Retry-After');
      throw new RateLimitError('Rate limit exceeded', retryAfter ? parseInt(retryAfter, 10) : 60);
    }

    // Handle authentication errors
    if (response.status === 401 || response.status === 403) {
      throw new AuthenticationError(
        'Authentication failed. Check your Snowflake account and JWT token.'
      );
    }

    // Handle timeout (408)
    if (response.status === 408) {
      const body = await response.json().catch(() => ({})) as Record<string, unknown>;
      throw new TimeoutError(
        (body.message as string) || 'Statement execution timed out',
        body.statementHandle as string | undefined
      );
    }

    // Handle statement execution errors (422)
    if (response.status === 422) {
      const body = await response.json().catch(() => ({})) as Record<string, unknown>;
      throw new StatementError(
        (body.message as string) || 'Statement execution failed',
        body.statementHandle as string | undefined,
        body.sqlState as string | undefined,
        body.code as string | undefined
      );
    }

    // Handle other errors
    if (!response.ok) {
      const errorBody = await response.text();
      let message = `Snowflake API error: ${response.status}`;
      try {
        const errorJson = JSON.parse(errorBody);
        message = errorJson.message || errorJson.error || message;
      } catch {
        // Use default message
      }
      throw new SnowflakeApiError(message, response.status);
    }

    // Handle 200 with async execution (still running)
    // Note: 202 indicates async execution in progress

    return response.json() as Promise<T>;
  }

  // ===========================================================================
  // Connection
  // ===========================================================================

  async testConnection(): Promise<{ connected: boolean; message: string; account: string }> {
    try {
      // Execute a simple query to verify connectivity
      const result = await this.executeStatement({
        statement: 'SELECT CURRENT_USER(), CURRENT_ACCOUNT(), CURRENT_ROLE()',
        timeout: 30,
        warehouse: this.credentials.warehouse,
        database: this.credentials.database,
        schema: this.credentials.schema,
        role: this.credentials.role,
      });

      if (result.status === 'success' && result.data && result.data.length > 0) {
        const [user, account, role] = result.data[0];
        return {
          connected: true,
          message: `Connected as ${user} with role ${role}`,
          account: account,
        };
      }

      return {
        connected: false,
        message: 'Connection test returned unexpected response',
        account: this.credentials.account,
      };
    } catch (error) {
      return {
        connected: false,
        message: error instanceof Error ? error.message : 'Connection failed',
        account: this.credentials.account,
      };
    }
  }

  // ===========================================================================
  // SQL Statement Execution
  // ===========================================================================

  async executeStatement(request: StatementRequest): Promise<StatementResult> {
    const body: Record<string, unknown> = {
      statement: request.statement,
      timeout: request.timeout || 60,
    };

    // Add context from request or defaults from credentials
    if (request.database || this.credentials.database) {
      body.database = request.database || this.credentials.database;
    }
    if (request.schema || this.credentials.schema) {
      body.schema = request.schema || this.credentials.schema;
    }
    if (request.warehouse || this.credentials.warehouse) {
      body.warehouse = request.warehouse || this.credentials.warehouse;
    }
    if (request.role || this.credentials.role) {
      body.role = request.role || this.credentials.role;
    }
    if (request.bindings) {
      body.bindings = request.bindings;
    }
    if (request.parameters) {
      body.parameters = request.parameters;
    }

    const response = await this.request<SnowflakeStatementResponse>('/statements', {
      method: 'POST',
      body: JSON.stringify(body),
    });

    return this.mapStatementResponse(response);
  }

  async executeStatementAsync(request: StatementRequest): Promise<StatementResult> {
    const body: Record<string, unknown> = {
      statement: request.statement,
      timeout: request.timeout || 60,
    };

    // Add context
    if (request.database || this.credentials.database) {
      body.database = request.database || this.credentials.database;
    }
    if (request.schema || this.credentials.schema) {
      body.schema = request.schema || this.credentials.schema;
    }
    if (request.warehouse || this.credentials.warehouse) {
      body.warehouse = request.warehouse || this.credentials.warehouse;
    }
    if (request.role || this.credentials.role) {
      body.role = request.role || this.credentials.role;
    }
    if (request.bindings) {
      body.bindings = request.bindings;
    }
    if (request.parameters) {
      body.parameters = request.parameters;
    }

    const response = await this.request<SnowflakeStatementResponse>(
      '/statements',
      {
        method: 'POST',
        body: JSON.stringify(body),
      },
      { async: 'true' }
    );

    return this.mapStatementResponse(response);
  }

  async getStatementStatus(statementHandle: string): Promise<StatementResult> {
    const response = await this.request<SnowflakeStatementResponse>(
      `/statements/${statementHandle}`
    );
    return this.mapStatementResponse(response);
  }

  async getStatementResult(statementHandle: string, partition?: number): Promise<StatementResult> {
    const queryParams: Record<string, string> = {};
    if (partition !== undefined) {
      queryParams.partition = String(partition);
    }

    const response = await this.request<SnowflakeStatementResponse>(
      `/statements/${statementHandle}`,
      {},
      Object.keys(queryParams).length > 0 ? queryParams : undefined
    );
    return this.mapStatementResponse(response);
  }

  async cancelStatement(statementHandle: string): Promise<{ success: boolean; message: string }> {
    await this.request<unknown>(`/statements/${statementHandle}/cancel`, {
      method: 'POST',
    });

    return {
      success: true,
      message: `Statement ${statementHandle} cancelled successfully`,
    };
  }

  // ===========================================================================
  // Schema Introspection
  // ===========================================================================

  async listDatabases(): Promise<PaginatedResponse<Database>> {
    const result = await this.executeStatement({
      statement: 'SHOW DATABASES',
      timeout: 30,
    });

    return this.mapShowResultToDatabases(result);
  }

  async listSchemas(database: string): Promise<PaginatedResponse<Schema>> {
    const result = await this.executeStatement({
      statement: `SHOW SCHEMAS IN DATABASE "${database}"`,
      timeout: 30,
    });

    return this.mapShowResultToSchemas(result, database);
  }

  async listTables(database: string, schema: string): Promise<PaginatedResponse<Table>> {
    const result = await this.executeStatement({
      statement: `SHOW TABLES IN "${database}"."${schema}"`,
      timeout: 30,
    });

    return this.mapShowResultToTables(result, database, schema);
  }

  async listViews(database: string, schema: string): Promise<PaginatedResponse<View>> {
    const result = await this.executeStatement({
      statement: `SHOW VIEWS IN "${database}"."${schema}"`,
      timeout: 30,
    });

    return this.mapShowResultToViews(result, database, schema);
  }

  async describeTable(
    database: string,
    schema: string,
    table: string
  ): Promise<PaginatedResponse<Column>> {
    const result = await this.executeStatement({
      statement: `DESCRIBE TABLE "${database}"."${schema}"."${table}"`,
      timeout: 30,
    });

    return this.mapDescribeResultToColumns(result);
  }

  // ===========================================================================
  // Warehouse Management
  // ===========================================================================

  async listWarehouses(): Promise<PaginatedResponse<Warehouse>> {
    const result = await this.executeStatement({
      statement: 'SHOW WAREHOUSES',
      timeout: 30,
    });

    return this.mapShowResultToWarehouses(result);
  }

  async getWarehouseStatus(warehouseName: string): Promise<Warehouse> {
    const result = await this.executeStatement({
      statement: `SHOW WAREHOUSES LIKE '${warehouseName}'`,
      timeout: 30,
    });

    const warehouses = this.mapShowResultToWarehouses(result);
    if (warehouses.items.length === 0) {
      throw new SnowflakeApiError(`Warehouse '${warehouseName}' not found`, 404, 'NOT_FOUND');
    }

    return warehouses.items[0];
  }

  async resumeWarehouse(warehouseName: string): Promise<{ success: boolean; message: string }> {
    await this.executeStatement({
      statement: `ALTER WAREHOUSE "${warehouseName}" RESUME`,
      timeout: 60,
    });

    return {
      success: true,
      message: `Warehouse '${warehouseName}' resumed successfully`,
    };
  }

  async suspendWarehouse(warehouseName: string): Promise<{ success: boolean; message: string }> {
    await this.executeStatement({
      statement: `ALTER WAREHOUSE "${warehouseName}" SUSPEND`,
      timeout: 60,
    });

    return {
      success: true,
      message: `Warehouse '${warehouseName}' suspended successfully`,
    };
  }

  // ===========================================================================
  // Response Mapping Helpers
  // ===========================================================================

  private mapStatementResponse(response: SnowflakeStatementResponse): StatementResult {
    return {
      statementHandle: response.statementHandle,
      status: this.mapStatus(response.statementStatusUrl, response.message),
      resultSetMetaData: response.resultSetMetaData
        ? {
            numRows: response.resultSetMetaData.numRows,
            format: response.resultSetMetaData.format,
            partitionInfo: response.resultSetMetaData.partitionInfo,
            rowType: response.resultSetMetaData.rowType.map((col) => ({
              name: col.name,
              type: col.type,
              database: col.database,
              schema: col.schema,
              table: col.table,
              nullable: col.nullable ?? true,
              byteLength: col.byteLength,
              precision: col.precision,
              scale: col.scale,
              collation: col.collation,
            })),
          }
        : undefined,
      data: response.data,
      stats: response.stats,
      statementStatusUrl: response.statementStatusUrl,
      message: response.message,
      sqlState: response.sqlState,
      code: response.code,
    };
  }

  private mapStatus(statusUrl?: string, message?: string): StatementStatus {
    if (message?.toLowerCase().includes('success') || (!statusUrl && !message)) {
      return 'success';
    }
    if (statusUrl) {
      return 'running';
    }
    if (message?.toLowerCase().includes('failed') || message?.toLowerCase().includes('error')) {
      return 'failed_with_error';
    }
    return 'success';
  }

  private mapShowResultToDatabases(result: StatementResult): PaginatedResponse<Database> {
    if (!result.data || !result.resultSetMetaData) {
      return { items: [], count: 0, hasMore: false };
    }

    const columnMap = this.buildColumnMap(result.resultSetMetaData.rowType);
    const databases: Database[] = result.data.map((row) => ({
      name: row[columnMap.name ?? 0] || '',
      createdOn: row[columnMap.created_on ?? 1],
      origin: row[columnMap.origin ?? -1],
      owner: row[columnMap.owner ?? -1],
      comment: row[columnMap.comment ?? -1],
      options: row[columnMap.options ?? -1],
      retentionTime: row[columnMap.retention_time ?? -1],
      resourceGroup: row[columnMap.resource_group ?? -1],
      kind: row[columnMap.kind ?? -1],
    }));

    return {
      items: databases,
      count: databases.length,
      total: result.resultSetMetaData.numRows,
      hasMore: false,
    };
  }

  private mapShowResultToSchemas(
    result: StatementResult,
    database: string
  ): PaginatedResponse<Schema> {
    if (!result.data || !result.resultSetMetaData) {
      return { items: [], count: 0, hasMore: false };
    }

    const columnMap = this.buildColumnMap(result.resultSetMetaData.rowType);
    const schemas: Schema[] = result.data.map((row) => ({
      name: row[columnMap.name ?? 0] || '',
      databaseName: database,
      createdOn: row[columnMap.created_on ?? -1],
      owner: row[columnMap.owner ?? -1],
      comment: row[columnMap.comment ?? -1],
      options: row[columnMap.options ?? -1],
      retentionTime: row[columnMap.retention_time ?? -1],
    }));

    return {
      items: schemas,
      count: schemas.length,
      total: result.resultSetMetaData.numRows,
      hasMore: false,
    };
  }

  private mapShowResultToTables(
    result: StatementResult,
    database: string,
    schema: string
  ): PaginatedResponse<Table> {
    if (!result.data || !result.resultSetMetaData) {
      return { items: [], count: 0, hasMore: false };
    }

    const columnMap = this.buildColumnMap(result.resultSetMetaData.rowType);
    const tables: Table[] = result.data.map((row) => ({
      name: row[columnMap.name ?? 0] || '',
      databaseName: database,
      schemaName: schema,
      kind: row[columnMap.kind ?? -1],
      createdOn: row[columnMap.created_on ?? -1],
      owner: row[columnMap.owner ?? -1],
      comment: row[columnMap.comment ?? -1],
      clusterBy: row[columnMap.cluster_by ?? -1],
      rows: row[columnMap.rows ?? -1] ? parseInt(row[columnMap.rows ?? -1], 10) : undefined,
      bytes: row[columnMap.bytes ?? -1] ? parseInt(row[columnMap.bytes ?? -1], 10) : undefined,
      retentionTime: row[columnMap.retention_time ?? -1],
      automaticClustering: row[columnMap.automatic_clustering ?? -1],
      changeTracking: row[columnMap.change_tracking ?? -1],
      isExternal: row[columnMap.is_external ?? -1],
    }));

    return {
      items: tables,
      count: tables.length,
      total: result.resultSetMetaData.numRows,
      hasMore: false,
    };
  }

  private mapShowResultToViews(
    result: StatementResult,
    database: string,
    schema: string
  ): PaginatedResponse<View> {
    if (!result.data || !result.resultSetMetaData) {
      return { items: [], count: 0, hasMore: false };
    }

    const columnMap = this.buildColumnMap(result.resultSetMetaData.rowType);
    const views: View[] = result.data.map((row) => ({
      name: row[columnMap.name ?? 0] || '',
      databaseName: database,
      schemaName: schema,
      createdOn: row[columnMap.created_on ?? -1],
      owner: row[columnMap.owner ?? -1],
      comment: row[columnMap.comment ?? -1],
      text: row[columnMap.text ?? -1],
      isSecure: row[columnMap.is_secure ?? -1],
      isMaterialized: row[columnMap.is_materialized ?? -1],
    }));

    return {
      items: views,
      count: views.length,
      total: result.resultSetMetaData.numRows,
      hasMore: false,
    };
  }

  private mapDescribeResultToColumns(result: StatementResult): PaginatedResponse<Column> {
    if (!result.data || !result.resultSetMetaData) {
      return { items: [], count: 0, hasMore: false };
    }

    const columnMap = this.buildColumnMap(result.resultSetMetaData.rowType);
    const columns: Column[] = result.data.map((row) => ({
      name: row[columnMap.name ?? 0] || '',
      type: row[columnMap.type ?? 1] || '',
      kind: row[columnMap.kind ?? -1],
      nullable: row[columnMap.null ?? -1]?.toLowerCase() === 'y',
      default: row[columnMap.default ?? -1],
      primaryKey: row[columnMap.primary_key ?? -1]?.toLowerCase() === 'y',
      uniqueKey: row[columnMap.unique_key ?? -1]?.toLowerCase() === 'y',
      check: row[columnMap.check ?? -1],
      expression: row[columnMap.expression ?? -1],
      comment: row[columnMap.comment ?? -1],
      policyName: row[columnMap.policy_name ?? -1],
    }));

    return {
      items: columns,
      count: columns.length,
      total: result.resultSetMetaData.numRows,
      hasMore: false,
    };
  }

  private mapShowResultToWarehouses(result: StatementResult): PaginatedResponse<Warehouse> {
    if (!result.data || !result.resultSetMetaData) {
      return { items: [], count: 0, hasMore: false };
    }

    const columnMap = this.buildColumnMap(result.resultSetMetaData.rowType);
    const warehouses: Warehouse[] = result.data.map((row) => ({
      name: row[columnMap.name ?? 0] || '',
      state: (row[columnMap.state ?? 1] || 'SUSPENDED') as Warehouse['state'],
      type: row[columnMap.type ?? -1],
      size: row[columnMap.size ?? -1],
      minClusterCount: row[columnMap.min_cluster_count ?? -1]
        ? parseInt(row[columnMap.min_cluster_count ?? -1], 10)
        : undefined,
      maxClusterCount: row[columnMap.max_cluster_count ?? -1]
        ? parseInt(row[columnMap.max_cluster_count ?? -1], 10)
        : undefined,
      startedClusters: row[columnMap.started_clusters ?? -1]
        ? parseInt(row[columnMap.started_clusters ?? -1], 10)
        : undefined,
      running: row[columnMap.running ?? -1]
        ? parseInt(row[columnMap.running ?? -1], 10)
        : undefined,
      queued: row[columnMap.queued ?? -1] ? parseInt(row[columnMap.queued ?? -1], 10) : undefined,
      isDefault: row[columnMap.is_default ?? -1]?.toLowerCase() === 'y',
      isCurrent: row[columnMap.is_current ?? -1]?.toLowerCase() === 'y',
      autoSuspend: row[columnMap.auto_suspend ?? -1]
        ? parseInt(row[columnMap.auto_suspend ?? -1], 10)
        : undefined,
      autoResume: row[columnMap.auto_resume ?? -1]?.toLowerCase() === 'true',
      available: row[columnMap.available ?? -1],
      provisioning: row[columnMap.provisioning ?? -1],
      quiescing: row[columnMap.quiescing ?? -1],
      other: row[columnMap.other ?? -1],
      createdOn: row[columnMap.created_on ?? -1],
      resumedOn: row[columnMap.resumed_on ?? -1],
      updatedOn: row[columnMap.updated_on ?? -1],
      owner: row[columnMap.owner ?? -1],
      comment: row[columnMap.comment ?? -1],
      resourceMonitor: row[columnMap.resource_monitor ?? -1],
      scalingPolicy: row[columnMap.scaling_policy ?? -1],
    }));

    return {
      items: warehouses,
      count: warehouses.length,
      total: result.resultSetMetaData.numRows,
      hasMore: false,
    };
  }

  private buildColumnMap(
    rowType: Array<{ name: string }>
  ): Record<string, number> {
    const map: Record<string, number> = {};
    rowType.forEach((col, index) => {
      map[col.name.toLowerCase()] = index;
    });
    return map;
  }
}

// =============================================================================
// Snowflake API Response Types (internal)
// =============================================================================

interface SnowflakeStatementResponse {
  statementHandle: string;
  statementStatusUrl?: string;
  message?: string;
  sqlState?: string;
  code?: string;
  resultSetMetaData?: {
    numRows: number;
    format?: string;
    partitionInfo?: Array<{
      rowCount: number;
      uncompressedSize?: number;
      compressedSize?: number;
    }>;
    rowType: Array<{
      name: string;
      type: string;
      database?: string;
      schema?: string;
      table?: string;
      nullable?: boolean;
      byteLength?: number;
      precision?: number;
      scale?: number;
      collation?: string;
    }>;
  };
  data?: string[][];
  stats?: {
    numRowsScanned?: number;
    numRowsInserted?: number;
    numRowsUpdated?: number;
    numRowsDeleted?: number;
    numDuplicateRowsUpdated?: number;
    elapsedTimeMs?: number;
  };
}

// =============================================================================
// Factory Function
// =============================================================================

/**
 * Create a Snowflake client instance with tenant-specific credentials.
 *
 * MULTI-TENANT: Each request provides its own credentials via headers,
 * allowing a single server deployment to serve multiple tenants.
 *
 * @param credentials - Tenant credentials parsed from request headers
 */
export function createSnowflakeClient(credentials: TenantCredentials): SnowflakeClient {
  return new SnowflakeClientImpl(credentials);
}
