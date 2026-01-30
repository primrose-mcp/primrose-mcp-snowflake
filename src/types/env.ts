/**
 * Environment Bindings
 *
 * Type definitions for Cloudflare Worker environment variables and bindings.
 *
 * MULTI-TENANT ARCHITECTURE:
 * This server supports multiple tenants. Tenant-specific credentials (JWT tokens,
 * account identifiers, etc.) are passed via request headers, NOT stored in wrangler
 * secrets. This allows a single server instance to serve multiple customers.
 *
 * Request Headers:
 * - X-Snowflake-Account: Account identifier (required)
 * - X-Snowflake-Token: JWT token for authentication (required)
 * - X-Snowflake-Warehouse: Default warehouse
 * - X-Snowflake-Database: Default database
 * - X-Snowflake-Schema: Default schema
 * - X-Snowflake-Role: Role to use
 */

// =============================================================================
// Tenant Credentials (parsed from request headers)
// =============================================================================

export interface TenantCredentials {
  /** Snowflake account identifier (from X-Snowflake-Account header) */
  account: string;

  /** JWT token for authentication (from X-Snowflake-Token header) */
  token: string;

  /** Default warehouse (from X-Snowflake-Warehouse header) */
  warehouse?: string;

  /** Default database (from X-Snowflake-Database header) */
  database?: string;

  /** Default schema (from X-Snowflake-Schema header) */
  schema?: string;

  /** Role to use (from X-Snowflake-Role header) */
  role?: string;
}

/**
 * Parse tenant credentials from request headers
 */
export function parseTenantCredentials(request: Request): TenantCredentials {
  const headers = request.headers;

  return {
    account: headers.get('X-Snowflake-Account') || '',
    token: headers.get('X-Snowflake-Token') || '',
    warehouse: headers.get('X-Snowflake-Warehouse') || undefined,
    database: headers.get('X-Snowflake-Database') || undefined,
    schema: headers.get('X-Snowflake-Schema') || undefined,
    role: headers.get('X-Snowflake-Role') || undefined,
  };
}

/**
 * Validate that required credentials are present
 */
export function validateCredentials(credentials: TenantCredentials): void {
  if (!credentials.account) {
    throw new Error('Missing X-Snowflake-Account header. Provide your Snowflake account identifier.');
  }
  if (!credentials.token) {
    throw new Error('Missing X-Snowflake-Token header. Provide your JWT token for authentication.');
  }
}

// =============================================================================
// Environment Configuration (from wrangler.jsonc vars and bindings)
// =============================================================================

export interface Env {
  // ===========================================================================
  // Environment Variables (from wrangler.jsonc vars)
  // ===========================================================================

  /** Maximum character limit for responses */
  CHARACTER_LIMIT: string;

  /** Default page size for list operations */
  DEFAULT_PAGE_SIZE: string;

  /** Maximum page size allowed */
  MAX_PAGE_SIZE: string;

  /** Default query timeout in seconds */
  DEFAULT_TIMEOUT: string;

  // ===========================================================================
  // Bindings
  // ===========================================================================

  /** KV namespace for caching (optional) */
  CACHE_KV?: KVNamespace;

  /** Durable Object namespace for MCP sessions */
  MCP_SESSIONS?: DurableObjectNamespace;

  /** Cloudflare AI binding (optional) */
  AI?: Ai;
}

// ===========================================================================
// Helper Functions
// ===========================================================================

/**
 * Get a numeric environment value with a default
 */
export function getEnvNumber(env: Env, key: keyof Env, defaultValue: number): number {
  const value = env[key];
  if (typeof value === 'string') {
    const parsed = parseInt(value, 10);
    return Number.isNaN(parsed) ? defaultValue : parsed;
  }
  return defaultValue;
}

/**
 * Get the character limit from environment
 */
export function getCharacterLimit(env: Env): number {
  return getEnvNumber(env, 'CHARACTER_LIMIT', 50000);
}

/**
 * Get the default page size from environment
 */
export function getDefaultPageSize(env: Env): number {
  return getEnvNumber(env, 'DEFAULT_PAGE_SIZE', 20);
}

/**
 * Get the maximum page size from environment
 */
export function getMaxPageSize(env: Env): number {
  return getEnvNumber(env, 'MAX_PAGE_SIZE', 100);
}

/**
 * Get the default query timeout from environment
 */
export function getDefaultTimeout(env: Env): number {
  return getEnvNumber(env, 'DEFAULT_TIMEOUT', 60);
}
