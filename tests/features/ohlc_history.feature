@bdd
Feature: Five years of daily prices for India, US, UK and EU
  As the owner of the platform
  I want a trustworthy 5-year daily price history for every market in the universe
  So that analytics and the backtester can rely on it

  Background:
    Given an isolated JARVIS workspace
    And a universe of 3 stocks in each market of IN US UK EU
    And Yahoo serves 1300 business days of prices for every ticker

  Scenario: Full backfill covers all four markets
    When I run the full OHLC backfill for all markets and load the lake
    Then the lake holds daily prices for markets IN US UK EU
    And every symbol has at least 1200 daily rows
    And the OHLC verification reports no failures

  Scenario: Repeating the backfill and the daily update never duplicates rows
    Given I have run the full OHLC backfill for all markets and loaded the lake
    When I run the daily OHLC update and load the lake
    And I run the full OHLC backfill for all markets and load the lake
    Then the number of OHLC rows is unchanged
    And the OHLC verification reports no failures

  Scenario: A corrected price from Yahoo replaces the stored one
    Given I have run the full OHLC backfill for all markets and loaded the lake
    And Yahoo now reports a close of 777.5 for UK0.L on its latest day
    When I run the daily OHLC update and load the lake
    Then the latest stored close for UK0.L is 777.5
    And the number of OHLC rows is unchanged

  Scenario: One failed Yahoo batch is detected by verification instead of passing silently
    Given Yahoo fails for the batch containing US1
    When I run the full OHLC backfill for all markets and load the lake
    Then the OHLC verification fails because US1 has no data
    And the other symbols still have their full history

  Scenario: A symbol Yahoo does not know is reported
    Given Yahoo has no data for EU2.PA
    When I run the full OHLC backfill for all markets and load the lake
    Then the OHLC verification fails because EU2.PA has no data
