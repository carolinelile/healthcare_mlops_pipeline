import pytest
from scripts.clean_fhir_json import clean_display, clean_div, should_remove_identifier

def test_clean_display_removes_digits_and_titles():
    assert clean_display("Dr. Synthia123") == "Synthia"
    assert clean_display("Mr.456 John") == "John"

def test_clean_div_returns_fixed_html():
    expected = '<div xmlns="http://www.w3.org/1999/xhtml">Generated text</div>'
    assert clean_div() == expected

def test_should_remove_identifier_true():
    identifier = {"type": {"coding": [{"code": "SS"}]}}
    assert should_remove_identifier(identifier) == True

def test_should_remove_identifier_false():
    identifier = {"type": {"coding": [{"code": "MR"}]}}
    assert should_remove_identifier(identifier) == False
