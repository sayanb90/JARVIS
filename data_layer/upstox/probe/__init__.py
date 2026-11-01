"""
Upstox capability and cost probe (SCRUM-99, phase 0 of the signal catalogue plan).

  python -m data_layer.upstox.probe check     one pass over every dataset: what works, error codes, history depth
  python -m data_layer.upstox.probe run       a full session: live stream with the extended fields, scheduled REST polls,
                                              resource log, and a report with measured size and cost
  python -m data_layer.upstox.probe report    rebuild the report from what is on disk

Everything is written to its own directory (default probe_data/), never to the production lake.
"""
