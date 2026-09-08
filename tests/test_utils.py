"""First tests for the lost-tokens utilities.

These feed https://dexaran.github.io/erc20-losses/, so a parsing mistake here becomes published data.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "lost_tokens"))

from utils.parsing import parse_addresses_from_text  # noqa: E402
from utils.iohelpers import uniq, number_with_commas, read_text  # noqa: E402

ADDR = "0x" + "c" * 40
ADDR2 = "0x" + "d" * 40


class TestParseAddresses:
    def test_extracts_a_plain_address(self):
        out = parse_addresses_from_text(ADDR)
        assert len(out) == 1
        assert out[0].lower() == ADDR

    def test_extracts_addresses_from_surrounding_text(self):
        text = f"token {ADDR} and also {ADDR2}\nanother line"
        assert len(parse_addresses_from_text(text)) == 2

    def test_returns_checksummed_addresses(self):
        out = parse_addresses_from_text("0x" + "ab" * 20)
        assert out[0].startswith("0x") and len(out[0]) == 42
        assert out[0] != out[0].lower(), "expected EIP-55 checksum casing"

    def test_ignores_a_transaction_hash(self):
        """A 32-byte value must not yield an address built from its first 40 hex chars."""
        assert parse_addresses_from_text("0x" + "a" * 64) == []

    def test_ignores_an_over_long_hex_string(self):
        assert parse_addresses_from_text("0x" + "b" * 41) == []

    def test_ignores_a_too_short_hex_string(self):
        assert parse_addresses_from_text("0x" + "b" * 39) == []

    def test_handles_empty_and_none(self):
        assert parse_addresses_from_text("") == []
        assert parse_addresses_from_text(None) == []

    def test_finds_an_address_inside_a_csv_row(self):
        assert len(parse_addresses_from_text(f"name,{ADDR},123")) == 1


class TestUniq:
    def test_preserves_order_and_drops_duplicates(self):
        assert uniq(["a", "b", "a", "c", "b"]) == ["a", "b", "c"]

    def test_drops_falsy_entries(self):
        assert uniq(["a", "", None, "b"]) == ["a", "b"]

    def test_empty(self):
        assert uniq([]) == []


class TestNumberWithCommas:
    def test_groups_thousands(self):
        assert number_with_commas(1234567.891) == "1,234,567.89"

    def test_handles_small_numbers(self):
        assert number_with_commas(1.5) == "1.50"

    def test_handles_negatives(self):
        assert number_with_commas(-1234.5).startswith("-1,234")

    def test_does_not_raise_on_non_numeric(self):
        # previously int() raised ValueError and took the report down
        assert number_with_commas("n/a") == "n/a"


class TestReadText:
    def test_missing_file_returns_empty(self, tmp_path):
        assert read_text(tmp_path / "nope.txt") == ""

    def test_reads_existing_file(self, tmp_path):
        f = tmp_path / "a.txt"
        f.write_text("hello", encoding="utf-8")
        assert read_text(f) == "hello"
