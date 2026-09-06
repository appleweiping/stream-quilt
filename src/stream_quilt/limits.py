"""Resource and plausibility ceilings for untrusted documents and in-memory alignment work."""

MAX_CONFIG_BYTES = 1 * 1024 * 1024
MAX_EVENT_FILE_BYTES = 64 * 1024 * 1024
MAX_EVENTS = 100_000
MAX_TEXT_LENGTH = 1_024
MAX_STREAMS = 4_096
MAX_MAPPING_ENTRIES = 4_096
MAX_EVENTS_PER_WINDOW = 1_000_000
MAX_OUTPUT_WINDOWS = 100_000
MAX_RESULT_GAPS = 1_000_000
MAX_EVENT_DATA_BYTES = 24 * 1024 * 1024
MAX_JSON_DEPTH = 64
MAX_JSON_NODES = 100_000
MAX_BASE64_DECODED_BYTES = 16 * 1024 * 1024
MAX_BENCHMARK_EVENT_VISITS = 50_000_000
# Theil-Sen compares every anchor pair, so drift anchor cost is quadratic; 1,024 anchors
# is 523,776 slopes, which stays well inside a second of pure-Python work.
MAX_DRIFT_ANCHORS = 1_024
# 10,000 ppm is 1%: one second of divergence every 100 seconds. Commodity crystal
# oscillators are specified in the tens of ppm and stay within a few hundred ppm across
# their whole temperature range, so a fit beyond this bound is far more likely to be
# mismatched anchors, a units error, or a clock stepped mid-capture than a real rate.
MAX_DRIFT_RATE_PPM = 10_000.0
