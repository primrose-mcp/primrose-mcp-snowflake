/**
 * Snowflake Entity Types
 *
 * Data structures for Snowflake SQL API entities.
 */

// =============================================================================
// Pagination
// =============================================================================

export interface PaginationParams {
  /** Number of items to return */
  limit?: number;
  /** Partition number for result fetching */
  partition?: number;
}

export interface PaginatedResponse<T> {
  /** Array of items */
  items: T[];
  /** Number of items in this response */
  count: number;
  /** Total count (if available) */
  total?: number;
  /** Whether more items are available */
  hasMore: boolean;
  /** Next partition number */
  nextPartition?: number;
}

// =============================================================================
// SQL Statement Execution
// =============================================================================

/**
 * Request body for executing SQL statements
 */
export interface StatementRequest {
  /** SQL statement to execute */
  statement: string;
  /** Query timeout in seconds */
  timeout?: number;
  /** Database context */
  database?: string;
  /** Schema context */
  schema?: string;
  /** Warehouse to use for execution */
  warehouse?: string;
  /** Role to use for the session */
  role?: string;
  /** Bind variables */
  bindings?: Record<string, BindVariable>;
  /** Session parameters */
  parameters?: Record<string, string>;
}

/**
 * Bind variable for parameterized queries
 */
export interface BindVariable {
  /** Data type of the variable */
  type: BindVariableType;
  /** Value as string */
  value: string;
}

export type BindVariableType =
  | 'FIXED'
  | 'REAL'
  | 'TEXT'
  | 'BINARY'
  | 'BOOLEAN'
  | 'DATE'
  | 'TIME'
  | 'TIMESTAMP_LTZ'
  | 'TIMESTAMP_NTZ'
  | 'TIMESTAMP_TZ';

/**
 * Statement execution result
 */
export interface StatementResult {
  /** Unique statement handle for async operations */
  statementHandle: string;
  /** Execution status */
  status: StatementStatus;
  /** Result set metadata */
  resultSetMetaData?: ResultSetMetaData;
  /** Result data (array of row arrays) */
  data?: string[][];
  /** Query statistics */
  stats?: QueryStats;
  /** Status URL for polling async queries */
  statementStatusUrl?: string;
  /** Error message if failed */
  message?: string;
  /** SQL state code */
  sqlState?: string;
  /** Snowflake error code */
  code?: string;
}

export type StatementStatus =
  | 'running'
  | 'resuming_warehouse'
  | 'queued'
  | 'blocked'
  | 'success'
  | 'failed_with_error'
  | 'failed_with_incident'
  | 'aborted';

/**
 * Metadata about the result set
 */
export interface ResultSetMetaData {
  /** Number of rows in the result */
  numRows: number;
  /** Column format (e.g., 'jsonv2') */
  format?: string;
  /** Information about result partitions */
  partitionInfo?: PartitionInfo[];
  /** Column definitions */
  rowType: ColumnMetaData[];
}

/**
 * Information about result partitions
 */
export interface PartitionInfo {
  /** Partition number */
  rowCount: number;
  /** Uncompressed size in bytes */
  uncompressedSize?: number;
  /** Compressed size in bytes */
  compressedSize?: number;
}

/**
 * Column metadata
 */
export interface ColumnMetaData {
  /** Column name */
  name: string;
  /** Snowflake data type */
  type: string;
  /** Database the column belongs to */
  database?: string;
  /** Schema the column belongs to */
  schema?: string;
  /** Table the column belongs to */
  table?: string;
  /** Whether the column is nullable */
  nullable: boolean;
  /** Byte length for string types */
  byteLength?: number;
  /** Precision for numeric types */
  precision?: number;
  /** Scale for numeric types */
  scale?: number;
  /** Collation for string types */
  collation?: string;
}

/**
 * Query execution statistics
 */
export interface QueryStats {
  /** Number of rows scanned */
  numRowsScanned?: number;
  /** Number of rows returned */
  numRowsInserted?: number;
  /** Number of rows updated */
  numRowsUpdated?: number;
  /** Number of rows deleted */
  numRowsDeleted?: number;
  /** Number of duplicate rows skipped */
  numDuplicateRowsUpdated?: number;
  /** Elapsed time in seconds */
  elapsedTimeMs?: number;
}

// =============================================================================
// Schema Objects
// =============================================================================

/**
 * Database information
 */
export interface Database {
  name: string;
  createdOn?: string;
  origin?: string;
  owner?: string;
  comment?: string;
  options?: string;
  retentionTime?: string;
  resourceGroup?: string;
  droppedOn?: string;
  kind?: string;
}

/**
 * Schema information
 */
export interface Schema {
  name: string;
  databaseName?: string;
  createdOn?: string;
  owner?: string;
  comment?: string;
  options?: string;
  retentionTime?: string;
  droppedOn?: string;
}

/**
 * Table information
 */
export interface Table {
  name: string;
  databaseName?: string;
  schemaName?: string;
  kind?: string;
  createdOn?: string;
  owner?: string;
  comment?: string;
  clusterBy?: string;
  rows?: number;
  bytes?: number;
  retentionTime?: string;
  automaticClustering?: string;
  changeTracking?: string;
  isExternal?: string;
  searchOptimization?: string;
  searchOptimizationProgress?: string;
  searchOptimizationBytes?: string;
  droppedOn?: string;
}

/**
 * View information
 */
export interface View {
  name: string;
  databaseName?: string;
  schemaName?: string;
  createdOn?: string;
  owner?: string;
  comment?: string;
  text?: string;
  isSecure?: string;
  isMaterialized?: string;
  droppedOn?: string;
}

/**
 * Column information from DESCRIBE TABLE
 */
export interface Column {
  name: string;
  type: string;
  kind?: string;
  nullable: boolean;
  default?: string;
  primaryKey?: boolean;
  uniqueKey?: boolean;
  check?: string;
  expression?: string;
  comment?: string;
  policyName?: string;
  privacyDomain?: string;
}

// =============================================================================
// Warehouse
// =============================================================================

/**
 * Warehouse information
 */
export interface Warehouse {
  name: string;
  state: WarehouseState;
  type?: string;
  size?: string;
  minClusterCount?: number;
  maxClusterCount?: number;
  startedClusters?: number;
  running?: number;
  queued?: number;
  isDefault?: boolean;
  isCurrent?: boolean;
  autoSuspend?: number;
  autoResume?: boolean;
  available?: string;
  provisioning?: string;
  quiescing?: string;
  other?: string;
  createdOn?: string;
  resumedOn?: string;
  updatedOn?: string;
  owner?: string;
  comment?: string;
  enableQueryAcceleration?: boolean;
  queryAccelerationMaxScaleFactor?: number;
  resourceMonitor?: string;
  scalingPolicy?: string;
}

export type WarehouseState =
  | 'STARTED'
  | 'SUSPENDED'
  | 'RESIZING'
  | 'SUSPENDING'
  | 'RESUMING';

// =============================================================================
// User and Role
// =============================================================================

/**
 * User information
 */
export interface User {
  name: string;
  createdOn?: string;
  loginName?: string;
  displayName?: string;
  firstName?: string;
  lastName?: string;
  email?: string;
  comment?: string;
  disabled?: boolean;
  mustChangePassword?: boolean;
  snowflakeLock?: boolean;
  defaultWarehouse?: string;
  defaultNamespace?: string;
  defaultRole?: string;
  defaultSecondaryRoles?: string;
  owner?: string;
  lastSuccessLogin?: string;
  expiresAtTime?: string;
  lockedUntilTime?: string;
  hasPassword?: boolean;
  hasMfa?: boolean;
}

/**
 * Role information
 */
export interface Role {
  name: string;
  createdOn?: string;
  assignedToUsers?: number;
  grantedToRoles?: number;
  grantedRoles?: number;
  owner?: string;
  comment?: string;
  isCurrent?: boolean;
  isDefault?: boolean;
  isInherited?: boolean;
}

// =============================================================================
// Response Format
// =============================================================================

export type ResponseFormat = 'json' | 'markdown';
