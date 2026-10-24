"""Compatible module command and legacy API; canonical rules live in engine."""

from quantlab.cli import main as main
from quantlab.legacy import (
    REQUIRED_COLUMNS as REQUIRED_COLUMNS,
)
from quantlab.legacy import (
    ValidationError as ValidationError,
)
from quantlab.legacy import (
    iter_csv_ticks as iter_csv_ticks,
)
from quantlab.legacy import (
    load_ticks as load_ticks,
)
from quantlab.legacy import (
    parse_tick as parse_tick,
)
from quantlab.legacy import (
    validate_records as validate_records,
)

if __name__ == "__main__":
    raise SystemExit(main())
