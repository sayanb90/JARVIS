@bdd
Feature: Fundamentals refresh rotates through the universe within the free API budget
  As the owner of the platform
  I want every stock refreshed regularly without exceeding 250 FMP calls a day
  So that fundamentals stay fresh at zero cost

  Background:
    Given an isolated JARVIS workspace
    And a universe of 10 stocks in each market of IN
    And FMP allows 12 calls a day, which is 3 stocks

  Scenario: A full cycle visits every stock once and never exceeds the daily budget
    When the fundamentals job runs on 4 consecutive days
    Then all 10 stocks were fetched at least once
    And only 12 fetches happened in total
    And no day used more than 12 FMP calls
    And the lake holds fundamentals for all 10 stocks

  Scenario: The stalest stocks go first
    Given 3 stocks were refreshed 30 days ago and the rest 2 days ago
    When the fundamentals job runs on 1 day
    Then the stocks fetched are exactly the 3 that were refreshed 30 days ago

  Scenario: A failed fetch is retried first on the next day and never marked as fresh
    Given FMP fails for IN1.NS
    When the fundamentals job runs on 1 day
    Then IN1.NS has no refresh date
    When FMP recovers
    And the fundamentals job runs on 1 day
    Then IN1.NS was fetched on the second day
    And IN1.NS has a refresh date

  Scenario: Only India is refreshed even if another market is requested
    Given a universe of 2 stocks in each market of IN US UK EU
    When the fundamentals job is asked for market US
    Then every stock fetched is an Indian stock
