"""
Strategy and execution on the live signal engine (SCRUM-154): signals and ticks from the in-memory bus, a strategy that turns them into
order intents, risk limits and a kill switch in front of every order, and an execution adapter (paper by default).

  trading.costs      Indian F&O transaction costs (brokerage, STT, exchange and SEBI fees, stamp duty, GST), dated
  trading.risk       limits per order, per position and per day; the kill switch
  trading.execution  Order, Fill, PaperExecution (fills at the opposite quote after a delay) and UpstoxExecution (real orders: refused
                     unless JARVIS_LIVE_ORDERS=1, which only the owner sets)
  trading.strategy   the Strategy interface and one plumbing example (a VRP and skew gate), not a tuned strategy
  trading.latency    one record per decision: exchange time, Upstox time, receive, bus, engine emit, decision, order sent, order acknowledged
  trading.paper      the process: python -m trading paper --underlying NIFTY
"""
