@bdd
Feature: Upstox token setup
  As the owner of the platform
  I want a safe, simple way to get a token and learn whether a long-lived one works
  So that I know how often I must log in

  Background:
    Given an isolated JARVIS workspace
    And the Upstox app credentials are in the environment file

  Scenario: The login link carries the app's client id and redirect
    When I ask for the login URL
    Then the link points at the Upstox login dialog with my client id and redirect URL

  Scenario: Exchanging the redirected URL stores both tokens without printing them
    Given Upstox will exchange a code for an access token and an extended token
    When I paste the URL I was redirected to
    Then the environment file holds both tokens with owner-only permissions
    And the tokens never appear on screen
    And my other settings in the environment file are untouched

  Scenario: A feed that accepts the extended token lets me stop logging in daily
    Given an Upstox feed that accepts only the extended token
    When I check which token the feed accepts and adopt the extended one
    Then the extended token is saved as the feed token
    And the collector will prefer the feed token

  Scenario: A feed that rejects the extended token means a daily login
    Given an Upstox feed that accepts only the access token
    When I check which token the feed accepts and adopt the extended one
    Then nothing is adopted
    And I am told the access token must be refreshed daily
