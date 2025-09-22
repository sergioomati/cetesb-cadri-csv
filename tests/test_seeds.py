import pytest
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from seeds import SeedManager
from utils_text import extract_trigrams, normalize_text


class TestSeedManager:
    """Test seed generation and management"""

    def test_valid_seed_check(self):
        """Test seed validation"""
        manager = SeedManager()

        # Valid seeds
        assert manager.is_valid_seed("CEM")
        assert manager.is_valid_seed("AGR")
        assert manager.is_valid_seed("BIO")
        assert manager.is_valid_seed("TEC1")

        # Invalid seeds (contain stopwords)
        assert not manager.is_valid_seed("LTDA")
        assert not manager.is_valid_seed("EPP")
        assert not manager.is_valid_seed("MEI")
        assert not manager.is_valid_seed("CIA")

        # Invalid seeds (too short)
        assert not manager.is_valid_seed("AB")
        assert not manager.is_valid_seed("X")

        # Invalid seeds (no letters)
        assert not manager.is_valid_seed("123")
        assert not manager.is_valid_seed("000")

    def test_seed_refinement(self):
        """Test 3->4 character refinement"""
        manager = SeedManager()

        refined = manager.refine_seed("CEM", level=4)

        # Should have letters and digits
        assert len(refined) > 0
        assert all(len(s) == 4 for s in refined)

        # Should include CEMA, CEMB, ..., CEM0, CEM1, etc.
        assert "CEMA" in refined
        assert "CEM1" in refined

        # Should not include invalid seeds
        refined_valid = [s for s in refined if manager.is_valid_seed(s)]
        assert len(refined_valid) == len(refined)

    def test_should_refine_logic(self):
        """Test refinement decision logic"""
        manager = SeedManager()

        # Should refine if too many results
        assert manager.should_refine("CEM", result_count=25)
        assert manager.should_refine("AGR", result_count=15)

        # Should not refine if reasonable results
        assert not manager.should_refine("CEMA", result_count=5)
        assert not manager.should_refine("BIO1", result_count=8)

    def test_discovered_text_processing(self):
        """Test extracting trigrams from discovered company names"""
        manager = SeedManager()

        company_names = [
            "CEMITÉRIO MUNICIPAL",
            "AGROPECUÁRIA BRASIL",
            "BIOMÉDICA LTDA",
            "TECNOLOGIA AVANÇADA"
        ]

        initial_queue_size = len(manager.seed_queue)
        manager.add_discovered_text(company_names)

        # Should have added new trigrams
        assert len(manager.seed_queue) > initial_queue_size

        # Should not add stopwords
        assert "LTD" not in manager.seed_queue

    def test_batch_seeds(self):
        """Test getting batch of seeds"""
        manager = SeedManager()
        manager.reset_queue()  # Start fresh

        batch = manager.get_batch_seeds(5)

        # Should return requested amount
        assert len(batch) <= 5

        # All should be valid
        assert all(manager.is_valid_seed(s) for s in batch)

        # All should be marked as used
        assert all(s in manager.used_seeds for s in batch)


class TestTextUtils:
    """Test text processing utilities"""

    def test_normalize_text(self):
        """Test text normalization"""
        assert normalize_text("São Paulo") == "SAO PAULO"
        assert normalize_text("Ação Ambiental") == "ACAO AMBIENTAL"
        assert normalize_text("  multiple   spaces  ") == "MULTIPLE SPACES"
        assert normalize_text("café") == "CAFE"

    def test_extract_trigrams(self):
        """Test trigram extraction"""
        trigrams = extract_trigrams("CEMITÉRIO MUNICIPAL")

        assert "CEM" in trigrams
        assert "MIT" in trigrams
        assert "RIO" in trigrams
        assert "MUN" in trigrams

        # Should not include non-letter sequences
        trigrams_num = extract_trigrams("123ABC456")
        assert "ABC" in trigrams_num
        assert "123" not in trigrams_num  # No pure numbers

    def test_trigram_extraction_edge_cases(self):
        """Test trigram extraction edge cases"""
        # Short text
        assert extract_trigrams("AB") == []

        # Special characters
        trigrams = extract_trigrams("A-B-C-D-E")
        assert "ABC" in trigrams
        assert "BCD" in trigrams

        # Mixed case
        trigrams = extract_trigrams("AbCdEf")
        assert "ABC" in trigrams
        assert "CDE" in trigrams


if __name__ == "__main__":
    pytest.main([__file__, "-v"])