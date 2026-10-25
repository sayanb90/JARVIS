@bdd
Feature: Pull a whole trading day from Upstox after the close
  As the owner of the platform
  I want to fetch a full day of 1-minute bars at once, even hours after the market closed
  So that I can test and repair data without being online during the session

  Background:
    Given an isolated JARVIS workspace
    And the India universe is RELIANCE.NS and INFY.NS
    And a valid Upstox token

  Scenario: A full session is fetched in one go and passes verification
    Given the Upstox candle service holds a full session for 2026-10-26
    When I pull 2026-10-26 from the candle service
    Then each instrument has 375 one-minute bars from 03:45 to 09:59 UTC
    And the bars are marked as coming from the candle service
    And the stored day is one compacted file
    And the day's candle bars pass verification

  Scenario: Live bars already collected are kept and only the gaps are filled
    Given the Upstox candle service holds a full session for 2026-10-26
    And live bars exist for the first 100 minutes of RELIANCE.NS with a close of 999
    When I pull 2026-10-26 from the candle service
    Then RELIANCE.NS has 375 bars with no duplicates
    And the 100 live bars still have a close of 999
    And 275 bars for RELIANCE.NS came from the candle service

  Scenario: Overwrite mode replaces live bars with the service's data
    Given the Upstox candle service holds a full session for 2026-10-26
    And live bars exist for the first 100 minutes of RELIANCE.NS with a close of 999
    When I pull 2026-10-26 from the candle service in overwrite mode
    Then no stored bar has a close of 999

  Scenario: An instrument the service does not know is reported, not fatal
    Given the Upstox candle service holds a full session for 2026-10-26 but not for INFY.NS
    When I pull 2026-10-26 from the candle service
    Then the pull reports no data for INFY.NS
    And RELIANCE.NS has 375 bars with no duplicates

  Scenario: An expired token stops the pull with a clear message
    Given the Upstox candle service rejects the token
    When I pull 2026-10-26 from the candle service
    Then the pull stops and tells the operator the token expires daily
    And no bars are stored

  Scenario: Today's session uses the intraday endpoint and earlier days use the historical one
    Given the Upstox candle service holds a full session for 2026-10-26
    When I pull 2026-10-26 from the candle service while it is still 2026-10-26 in India
    Then the intraday endpoint was asked first
    When I pull 2026-10-26 from the candle service while it is 2026-10-27 in India
    Then only the historical endpoint was used
