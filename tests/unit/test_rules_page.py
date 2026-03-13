from __future__ import annotations

from app.services.config_editor_service import ConfigEditorService
from app.services.rulebook_service import RulebookService
from app.web.pages import templates
from app.web.presenters import page_context


def _render_rules(rulebook: dict) -> str:
    templates.env.cache.clear()
    template = templates.env.get_template("rules.html")
    original_url_for = templates.env.globals.get("url_for")
    templates.env.globals["url_for"] = lambda *args, **kwargs: "/static/mock"
    try:
        context = page_context("规则页", "closed")
        context.update(
            {
                "request": object(),
                "data_status": None,
                "preferences": None,
                "rulebook": rulebook,
                "config_sections": ConfigEditorService().build_sections(),
            }
        )
        return template.render(**context)
    finally:
        if original_url_for is not None:
            templates.env.globals["url_for"] = original_url_for


def test_rules_page_renders_key_formulas():
    html = _render_rules(RulebookService().build())

    assert "单票分 intra_score" in html
    assert "最终分 final_score" in html
    assert "执行决策分 decision_score" in html
    assert "breakout_entry_threshold" in html
    assert "交易摩擦" in html
    assert "直接修改系统参数" in html
