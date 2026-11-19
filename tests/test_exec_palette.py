"""Executive KPI tile colours: direction, wide ranges and explicit ``better``."""
from elastic_config.dashboards.generate_exec_dashboard import (
    kpi_band,
    kpi_band_edges,
    kpi_band_usable,
)


def test_abandonment_is_lower_is_better():
    spec = {"uniform": [15.0, 35.0]}
    assert kpi_band("business.checkout_abandonment_rate", spec, 25.0) == "green"
    assert kpi_band("business.checkout_abandonment_rate", spec, 55.0) == "red"


def test_wide_higher_is_better_range_can_go_red():
    spec = {"uniform": [50000.0, 250000.0]}
    assert kpi_band("business.revenue_per_hour", spec, 65000.0) == "green"
    assert kpi_band("business.revenue_per_hour", spec, 20000.0) == "red"


def test_mission_is_not_a_miss():
    # "mission_success_probability" must stay higher-is-better
    assert kpi_band_edges("business.mission_success_probability_pct", {"uniform": [92.0, 99.8]})[0] is False


def test_explicit_better_overrides_keywords():
    spec = {"uniform": [0.01, 0.38]}
    assert kpi_band_edges("business.crash_rate_pct", spec)[0] is False  # keyword guess
    assert kpi_band_edges("business.crash_rate_pct", spec, "lower")[0] is True


def test_negative_range_higher_is_better_is_unusable():
    assert not kpi_band_usable("business.net_satisfaction_proxy_nps", {"uniform": [-8.0, 68.0]})
