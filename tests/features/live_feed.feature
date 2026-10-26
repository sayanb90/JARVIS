@bdd
Feature: Live Upstox ticks become trustworthy 1-second bars
  As a strategy developer
  I want the collector to turn the live feed into clean 1-second bars and survive real-world trouble
  So that intraday signals can be built on the stored data at the finest resolution I keep

  Background:
    Given an isolated JARVIS workspace
    And the India universe is RELIANCE.NS and INFY.NS
    And the exchange clock reads 10:06 UTC on 2026-10-23

  Scenario: A full NSE session is stored as 1-second bars
    Given an Upstox server streaming a full session with one update every 5 seconds
    When the collector runs the session with a valid token
    Then the server saw the bearer token and a full-mode subscription for both instruments
    And RELIANCE.NS has 4500 one-second bars from 03:45:00 to 09:59:55 UTC
    And the bar volumes add up to the volume traded after the first update
    And every bar carries the last bid and ask
    And the day's bars pass verification

  Scenario: The default bar width is one second
    Given an Upstox server streaming a full session with one update every 5 seconds
    When the collector runs the session with a valid token
    Then the bars were stored at one-second resolution

  Scenario: Several updates inside one second become a single bar
    Given an Upstox server that sends 3 updates for RELIANCE.NS within the second starting 03:45:10
    When the collector runs the session with a valid token
    Then RELIANCE.NS has exactly 1 bar at 03:45:10 built from 3 updates with the first as open and the last as close

  Scenario: Seconds without any update have no bar and nothing is invented
    Given an Upstox server that sends updates for RELIANCE.NS at 03:45:00, 03:45:01 and 03:45:05 only
    When the collector runs the session with a valid token
    Then RELIANCE.NS has bars at exactly those 3 seconds

  Scenario: One-second bars roll up to the same minutes Upstox reports
    Given an Upstox server streaming a full session with one update every 5 seconds
    And Upstox's own 1-minute candles for that day match the session
    When the collector runs the session with a valid token
    And I pull that day's candles from the candle service
    Then the live bars rolled up to one minute agree with the candles

  Scenario: The stored bars agree with Yahoo's daily bar
    Given an Upstox server streaming a full session with one update every 5 seconds
    And Yahoo's daily bar for that day agrees with the session
    When the collector runs the session with a valid token
    Then the day's bars pass verification
    And the bars agree with Yahoo's daily close within 0.5 percent

  Scenario: A connection drop in mid-session loses nothing
    Given an Upstox server streaming a full session with one update every 5 seconds
    And the server drops the first connection after 3000 frames
    When the collector runs the session with a valid token
    Then the collector connected more than once and subscribed again
    And RELIANCE.NS has 4500 one-second bars from 03:45:00 to 09:59:55 UTC

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
    Given an Upstox server whose only updates were last traded on the previous day
    When the collector runs the session with a valid token
    Then no bars are stored

  Scenario: Updates stamped in the future are rejected as a clock or unit problem
    Given an Upstox server whose updates are stamped hours ahead of the exchange clock
    When the collector runs the session with a valid token
    Then no bars are stored

  Scenario: Ctrl+C saves the bars received so far and compacts the day
    Given an Upstox server streaming a full session with one update every 5 seconds
    And the server then stays quiet
    When the operator presses Ctrl+C while the session is running
    Then the stored bars are compacted into one file for the day
    And RELIANCE.NS has 4500 one-second bars from 03:45:00 to 09:59:55 UTC
