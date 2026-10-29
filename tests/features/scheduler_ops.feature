@bdd
Feature: Scheduler operations
  As the operator running cron on the VPS
  I want safe, predictable job behaviour
  So that overlapping runs, dry runs and wrong-market requests cannot damage the data

  Background:
    Given an isolated JARVIS workspace
    And a universe of 2 stocks in each market of IN US UK EU

  Scenario: Daily OHLC covers all four markets and loads the lake
    Given Yahoo serves 30 business days of prices for every ticker
    When cron runs the daily OHLC job
    Then the job succeeds
    And the lake holds daily prices for markets IN US UK EU

  Scenario: A dry run changes nothing
    Given Yahoo would fail the test if it were called
    When cron runs all jobs as a dry run
    Then the job succeeds
    And no data files were written

  Scenario: Two overlapping runs cannot both proceed
    Given another scheduler run is in progress
    When cron runs the daily OHLC job
    Then the second run refuses to start
    And the first run's lock is left in place

  Scenario: Intraday bars are refused for markets other than India
    When cron runs the bars job for market US
    Then the job fails
    And no bars are stored

  Scenario: The lock is released after a failed job
    Given Yahoo is down
    When cron runs the daily OHLC job
    Then the job fails
    And the scheduler lock is released
