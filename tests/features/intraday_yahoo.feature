@bdd
Feature: Yahoo intraday bars (US, UK and EU by default, India on request)
  As the owner of the platform
  I want intraday bars from Yahoo for the markets the live feed does not cover
  So that intraday history starts accumulating immediately

  Background:
    Given an isolated JARVIS workspace
    And a universe of 2 stocks in each market of IN US UK EU

  Scenario: Bars are stored in UTC and bars still forming are left out
    Given Yahoo serves 5-minute bars for the last 2 sessions with the latest bar still forming
    When I collect the Yahoo intraday bars for India with the deepest window and load the lake
    Then the first stored bar of each day starts at 03:45 UTC
    And the bar that was still forming is not stored

  Scenario: Collecting twice never duplicates bars
    Given Yahoo serves 5-minute bars for the last 2 sessions with the latest bar still forming
    When I collect the Yahoo intraday bars for India with the deepest window and load the lake
    And I collect the Yahoo intraday bars for India with the deepest window and load the lake
    Then each symbol has no duplicate bar timestamps

  Scenario Outline: A market outside the four is refused
    When I try to collect Yahoo intraday bars for <market>
    Then the request is refused because the market is not one of the four

    Examples:
      | market |
      | JP     |
      | SG     |

  Scenario: Asking for the deepest window uses what Yahoo allows
    When I plan a deepest-window collection for each bar size
    Then the windows are 5m 59 days, 1m 6 days and 60m 729 days
