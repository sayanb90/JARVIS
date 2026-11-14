"""
The live signal engine (SCRUM-201, SCRUM-159).

  signals.engine.runner     the first engine: one pandas LiveRunner per cadence group (the batch reference; JARVIS_SIGNAL_ENGINE=runner)
  signals.engine.state      per-tick state: option contracts in numpy arrays, index / VIX / futures bars, the chain snapshot per bucket
  signals.engine.surface    smile and ATM summaries of one snapshot, in numpy
  signals.engine.operators  one operator per live signal (native, or a windowed adapter over the batch function)
  signals.engine.core       engine v2: ticks -> state -> operators, provisional values on a timer, final rows at each bucket close
"""

from signals.engine.runner import (IN_FEED, LAYER, WRITE_EVERY_S, BusSource, Engine, FileSource, run_engine, signal_rows,  # noqa: F401
                                   start)
