@bdd
Feature: Explore a trading day in the intraday viewer
  As a strategy developer
  I want to pick a date and an instrument and see volume, the order book at the best price, the spread and volatility
  So that I can judge whether the collected data is sensible and spot what is worth modelling

  Background:
    Given synthetic 1-second data for 3 trading days and 10 instruments
    And the viewer is running on that data

  Scenario: The dropdowns offer the newest date first and every instrument with its bar count
    When I open the viewer
    Then the dates offered are 2026-10-23, 2026-10-22 and 2026-10-21
    And I am warned that the data is synthetic
    And the instrument list for 2026-10-23 has 10 instruments with their bar counts

  Scenario: Choosing an instrument and a date shows the day's picture
    When I choose RELIANCE.NS on 2026-10-23
    Then I get price, volume, cumulative volume, best bid and ask, book sizes, imbalance, spread and volatility series of the same length
    And the times are shown in Indian Standard Time starting at 09:15
    And the summary reports volume, VWAP, average spread in basis points and realised volatility
    And the high is at least the open and the close and the low is at most both

  Scenario Outline: Coarser bar widths keep the day's totals
    When I choose RELIANCE.NS on 2026-10-23 with bars of <width> seconds
    Then the total volume equals the 1-second total
    And there are at most <max_bars> bars

    Examples:
      | width | max_bars |
      | 5     | 4500     |
      | 60    | 375      |
      | 300   | 75       |

  Scenario: The market overview ranks instruments by volatility, spread and volume
    When I open the market overview for 2026-10-23
    Then there is one row per instrument
    And every row has volume, average spread, realised volatility and return
    And the instrument with the most volume is ITC.NS
    And a wider-spread instrument is shown to have a higher average spread than a tighter one

  Scenario: A day pulled from the candle service has no book, and the viewer says so instead of failing
    Given a day with bars that carry no bid or ask
    When I view R.NS on the day that has no book data
    Then the chart data is valid JSON with no missing-number markers
    And the viewer reports that there is no book data

  Scenario Outline: Bad requests are refused without touching the data
    When I ask for <what>
    Then the viewer answers with status <status>

    Examples:
      | what                                   | status |
      | a date that is not a calendar date     | 422    |
      | an instrument name containing SQL      | 422    |
      | an instrument name that climbs folders | 422    |
      | a bar width that is not offered        | 422    |
      | an instrument with no data that day    | 404    |
      | a day with no data                     | 404    |

  Scenario: Access can be limited to the owner
    Given the viewer requires a username and password
    When someone opens it without credentials
    Then they are asked to log in and see no data
    When the owner opens it with the right credentials
    Then they see the data

  Scenario: Viewing never changes the stored data
    When I browse every date, instrument and the overview
    Then no file in the lake has changed
