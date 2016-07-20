package org.apache.drill.jig.api;

/**
 * Base exception for all unexpected errors which indicates problems
 * that applications cannot recover from. Such errors might be protocol
 * errors, internal state errors and other problems that indicate faults
 * in configuration or code.
 */

public class JigError extends RuntimeException
{
  private static final long serialVersionUID = 1L;

}
