@bdd
Feature: The 8-pillar quality score
  As an analyst
  I want each pillar to score independently and strictly
  So that a stock's score can be explained pillar by pillar

  Background:
    Given an isolated JARVIS workspace

  Scenario Outline: One weakness costs exactly one point
    Given a stock that passes all eight pillars at a price of <price>
    And its weakness is "<weakness>"
    When I score the stock
    Then its score is <score>

    Examples:
      | weakness                              | price | score |
      | none                                  | 9.0   | 8     |
      | five-year average P/E is too high     | 9.0   | 7     |
      | return on capital is below 9 percent  | 9.0   | 7     |
      | revenue shrank over the period        | 9.0   | 7     |
      | net income shrank over the period     | 9.0   | 7     |
      | the share count increased             | 9.0   | 7     |
      | long-term debt is over 5 times FCF    | 9.0   | 7     |
      | free cash flow shrank over the period | 9.0   | 7     |
      | price is just above 20 times FCF      | 10.5  | 7     |
      | none                                  | 20.0  | 6     |

  Scenario: Boundaries are strict
    Given a stock that passes all eight pillars at a price of 10.0
    When I score the stock
    Then its score is 7
    Given a stock that passes all eight pillars at a price of 9.99
    When I score the stock
    Then its score is 8

  Scenario: Unknown or unpriced stocks score zero instead of failing
    Given a stock that passes all eight pillars at a price of 9.0
    When I score a stock that is not in the lake
    Then its score is 0

  Scenario: The screen ranks stocks and gives unpriced ones zero
    Given a stock that passes all eight pillars at a price of 9.0
    And a second stock with fundamentals but no price
    When I screen the India universe
    Then the ranking is GOOD.NS with 8 then UNPRICED.NS with 0
