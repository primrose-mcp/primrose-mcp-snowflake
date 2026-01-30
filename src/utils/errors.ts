/**
 * Error Handling Utilities
 *
 * Custom error classes and error handling helpers for Snowflake API.
 */

/**
 * Base Snowflake API error
 */
export class SnowflakeApiError extends Error {
  public statusCode?: number;
  public code: string;
  public retryable: boolean;
  public sqlState?: string;

  constructor(
    message: string,
    statusCode?: number,
    code?: string,
    retryable = false,
    sqlState?: string
  ) {
    super(message);
    this.name = 'SnowflakeApiError';
    this.statusCode = statusCode;
    this.code = code || 'SNOWFLAKE_ERROR';
    this.retryable = retryable;
    this.sqlState = sqlState;
  }
}

/**
 * Rate limit exceeded error
 */
export class RateLimitError extends SnowflakeApiError {
  public retryAfterSeconds: number;

  constructor(message: string, retryAfterSeconds: number) {
    super(message, 429, 'RATE_LIMIT_EXCEEDED', true);
    this.name = 'RateLimitError';
    this.retryAfterSeconds = retryAfterSeconds;
  }
}

/**
 * Authentication error
 */
export class AuthenticationError extends SnowflakeApiError {
  constructor(message: string) {
    super(message, 401, 'AUTHENTICATION_FAILED', false);
    this.name = 'AuthenticationError';
  }
}

/**
 * Statement execution error
 */
export class StatementError extends SnowflakeApiError {
  public statementHandle?: string;

  constructor(message: string, statementHandle?: string, sqlState?: string, code?: string) {
    super(message, 422, code || 'STATEMENT_ERROR', false, sqlState);
    this.name = 'StatementError';
    this.statementHandle = statementHandle;
  }
}

/**
 * Timeout error
 */
export class TimeoutError extends SnowflakeApiError {
  public statementHandle?: string;

  constructor(message: string, statementHandle?: string) {
    super(message, 408, 'EXECUTION_TIMEOUT', true);
    this.name = 'TimeoutError';
    this.statementHandle = statementHandle;
  }
}

/**
 * Not found error
 */
export class NotFoundError extends SnowflakeApiError {
  constructor(entityType: string, identifier: string) {
    super(`${entityType} '${identifier}' not found`, 404, 'NOT_FOUND', false);
    this.name = 'NotFoundError';
  }
}

/**
 * Validation error
 */
export class ValidationError extends SnowflakeApiError {
  public details: Record<string, string[]>;

  constructor(message: string, details: Record<string, string[]> = {}) {
    super(message, 400, 'VALIDATION_ERROR', false);
    this.name = 'ValidationError';
    this.details = details;
  }
}

/**
 * Check if an error is retryable
 */
export function isRetryableError(error: unknown): boolean {
  if (error instanceof SnowflakeApiError) {
    return error.retryable;
  }
  if (error instanceof Error) {
    // Network errors are typically retryable
    return (
      error.message.includes('network') ||
      error.message.includes('timeout') ||
      error.message.includes('ECONNRESET')
    );
  }
  return false;
}

/**
 * Format an error for logging
 */
export function formatErrorForLogging(error: unknown): Record<string, unknown> {
  if (error instanceof SnowflakeApiError) {
    return {
      name: error.name,
      message: error.message,
      code: error.code,
      statusCode: error.statusCode,
      retryable: error.retryable,
      sqlState: error.sqlState,
      ...(error instanceof RateLimitError && { retryAfterSeconds: error.retryAfterSeconds }),
      ...(error instanceof StatementError && { statementHandle: error.statementHandle }),
      ...(error instanceof TimeoutError && { statementHandle: error.statementHandle }),
      ...(error instanceof ValidationError && { details: error.details }),
    };
  }
  if (error instanceof Error) {
    return {
      name: error.name,
      message: error.message,
      stack: error.stack,
    };
  }
  return { error: String(error) };
}
