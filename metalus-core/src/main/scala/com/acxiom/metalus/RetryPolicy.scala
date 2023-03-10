package com.acxiom.metalus

/**
 * Represents a configuration for retrying operations.
 *
 * @param maximumRetries                Maximum number of times a retry will be attempted. Default is 10.
 * @param waitTimeMultipliesMS          Number of milliseconds the process should wait to handle the retry. Default is 1000.
 * @param useRetryCountAsTimeMultiplier Flag indicating whether the current attempt number should multiply against the
 *                                      waitTimeMultipliesMS to perform a back-off retry. Default is true.
 */
case class RetryPolicy(maximumRetries: Option[Int] = Some(Constants.TEN),
                       waitTimeMultipliesMS: Option[Int] = Some(Constants.ONE_THOUSAND),
                       useRetryCountAsTimeMultiplier: Option[Boolean] = Some(true))
