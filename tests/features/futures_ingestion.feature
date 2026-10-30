@bdd
Feature: NSE futures and options flow through the same pipeline as equities
  As a strategy developer
  I want contracts chosen from the exchange's instrument file and collected as 1-second bars with open interest
  So that derivatives signals can be built on the same lake as the equity data

  Background:
    Given an isolated JARVIS workspace
    And an NSE instrument file with NIFTY and RELIANCE futures and options

  Scenario: The instrument file becomes a contract table that outlives expiry
    When I import the instrument file as of 2026-10-19
    Then the contract table lists futures, calls and puts for both underlyings
    And the expired NIFTY future is still resolvable by its symbol

  Scenario: Contracts are chosen around the money and written to the collector's list
    Given the instrument file has been imported
    When I select NIFTY with 2 futures expiries, 1 option expiry and 2 strikes either side of 24510
    Then the collector list holds 2 futures and 10 options for NIFTY
    And the collector list excludes the expired future

  Scenario: A live futures session is stored with open interest
    Given the instrument file has been imported
    And the collector list holds the nearest NIFTY future
    And an Upstox server streaming that future for 60 seconds with open interest rising from 9800000
    When the collector runs the session with a valid token
    Then the server saw a full-mode subscription for the future's key only
    And the future has 60 one-second bars stored under the contract's symbol
    And every bar carries open interest and the last one shows 9800059
    And the futures bars pass verification
    And the viewer reports open interest for that contract

  Scenario: The after-close pull stores open interest from the candle service
    Given the instrument file has been imported
    And the collector list holds the nearest NIFTY future
    And Upstox's candles for that future carry open interest
    When I pull that day's candles for the collector list
    Then the future has 3 one-minute bars with open interest 12000, 12010 and 12025

  Scenario: Equities and futures live side by side in the lake
    Given the instrument file has been imported
    And the collector list holds the nearest NIFTY future
    And an Upstox server streaming that future for 60 seconds with open interest rising from 9800000
    And equity bars for RELIANCE.NS exist for the same day
    When the collector runs the session with a valid token
    Then both instruments are listed for that day
    And the equity bars have no open interest
