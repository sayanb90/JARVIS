@bdd
Feature: Live Upstox ticks become trustworthy 1-minute bars
  As the owner of the platform
  I want the collector to turn the live feed into clean, complete bars and survive real-world trouble
  So that intraday signals can be built on the stored data

  Background:
    Given an isolated JARVIS workspace
    And the India universe is RELIANCE.NS and INFY.NS
    And the exchange clock reads 10:06 UTC on 2026-10-23

  Scenario: A full NSE session is stored as complete 1-minute bars
    Given an Upstox server streaming a full session for the universe
    When the collector runs the session with a valid token
    Then the server saw the bearer token and a full-mode subscription for both instruments
    And RELIANCE.NS has 375 one-minute bars from 03:45 to 09:59 UTC
    And the bar volumes add up to the volume traded after the first tick
    And every bar carries the last bid and ask
    And the day's bars pass verification

  Scenario: The stored bars agree with Yahoo's daily bar
    Given an Upstox server streaming a full session for the universe
    And Yahoo's daily bar for that day agrees with the session
    When the collector runs the session with a valid token
    Then the day's bars pass verification
    And the bars agree with Yahoo's daily close within 0.5 percent

  Scenario: A connection drop in mid-session loses nothing
    Given an Upstox server streaming a full session for the universe
    And the server drops the first connection after 300 frames
    When the collector runs the session with a valid token
    Then the collector connected more than once and subscribed again
    And RELIANCE.NS has 375 one-minute bars from 03:45 to 09:59 UTC

  Scenario Outline: A rejected token stops the collector with a clear message
    Given an Upstox server that answers the handshake with HTTP <status>
    When the collector runs the session with an expired token
    Then the collector stops and tells the operator the token expires daily
    And no bars are stored

    Examples:
      | status |
      | 401    |
      | 403    |

  Scenario: Last trades from a previous day are not turned into bars
    Given an Upstox server whose only ticks were last traded on the previous day
    When the collector runs the session with a valid token
    Then no bars are stored

  Scenario: Ticks stamped in the future are rejected as a clock or unit problem
    Given an Upstox server whose ticks are stamped hours ahead of the exchange clock
    When the collector runs the session with a valid token
    Then no bars are stored

  Scenario: Ctrl+C saves the bars received so far and compacts the day
    Given an Upstox server streaming a full session for the universe
    And the server then stays quiet
    When the operator presses Ctrl+C while the session is running
    Then the stored bars are compacted into one file for the day
    And RELIANCE.NS has 375 one-minute bars from 03:45 to 09:59 UTC
