from __future__ import annotations

from pathlib import Path

import yaml

from app.services.config_editor_service import ConfigEditorService


def test_config_editor_service_builds_sections_from_yaml(tmp_path: Path):
    (tmp_path / "strategy_scoring.yaml").write_text("selection:\n  min_final_score_for_target: 55.0\n", encoding="utf-8")
    (tmp_path / "execution_overlay.yaml").write_text("pullback_low_pct: -6.0\n", encoding="utf-8")
    (tmp_path / "category_profiles.yaml").write_text("category_heads:\n  stock_etf:\n    entry:\n      momentum_5d: 0.22\n", encoding="utf-8")
    (tmp_path / "portfolio_constraints.yaml").write_text("selection:\n  max_selected_total: 3\n", encoding="utf-8")
    (tmp_path / "execution_costs.yaml").write_text("execution_cost_bps: 5.0\n", encoding="utf-8")

    sections = ConfigEditorService(config_dir=tmp_path).build_sections()

    assert any(section["file_name"] == "portfolio_constraints.yaml" for section in sections)
    portfolio_section = next(section for section in sections if section["file_name"] == "portfolio_constraints.yaml")
    assert any(field["path"] == "selection.max_selected_total" for field in portfolio_section["fields"])


def test_config_editor_service_updates_scalar_and_list_values(tmp_path: Path):
    (tmp_path / "strategy_scoring.yaml").write_text("selection:\n  min_final_score_for_target: 55.0\n", encoding="utf-8")
    (tmp_path / "execution_overlay.yaml").write_text("pullback_low_pct: -6.0\n", encoding="utf-8")
    (tmp_path / "category_profiles.yaml").write_text("selection:\n  offensive_categories:\n    - stock_etf\n    - gold_etf\n", encoding="utf-8")
    (tmp_path / "portfolio_constraints.yaml").write_text("selection:\n  max_selected_total: 3\n", encoding="utf-8")
    (tmp_path / "execution_costs.yaml").write_text("execution_cost_bps: 5.0\nmin_trade_amount: 100.0\n", encoding="utf-8")

    service = ConfigEditorService(config_dir=tmp_path)
    service.update_file(
        "portfolio_constraints.yaml",
        {
            "selection__max_selected_total": "5",
        },
    )
    service.update_file(
        "category_profiles.yaml",
        {
            "selection__offensive_categories": "stock_etf, bond_etf, gold_etf",
        },
    )

    portfolio_payload = yaml.safe_load((tmp_path / "portfolio_constraints.yaml").read_text(encoding="utf-8"))
    category_payload = yaml.safe_load((tmp_path / "category_profiles.yaml").read_text(encoding="utf-8"))

    assert portfolio_payload["selection"]["max_selected_total"] == 5
    assert category_payload["selection"]["offensive_categories"] == ["stock_etf", "bond_etf", "gold_etf"]
