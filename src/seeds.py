import string
from typing import List, Set, Optional
from collections import deque
import pandas as pd
from pathlib import Path

from config import CSV_DIR
from utils_text import extract_trigrams, normalize_text
from logging_conf import logger


class SeedManager:
    """Manage seed generation, refinement and bootstrapping"""

    # Corporate stopwords to avoid
    STOPWORDS = {
        'LTDA', 'LTD', 'MEI', 'EPP', 'EIRELI', 'S/A', 'SA',
        'CIA', 'IND', 'COM', 'EMP', 'CORP', 'GROUP', 'HOLD'
    }

    # Initial neutral seeds
    INITIAL_SEEDS = [
        'CEM', 'ACE', 'AGR', 'LOG', 'MEC', 'TEC', 'TRAN',
        'BIO', 'QUIM', 'ENER', 'PORT', 'AMB', 'RES', 'REC',
        'COL', 'TRA', 'DES', 'PRO', 'SER', 'IND', 'FAB',
        'META', 'PLAS', 'PAPEL', 'FARM', 'ALI', 'BEB', 'TEXT'
    ]

    def __init__(self):
        self.used_seeds: Set[str] = set()
        self.seed_queue: deque = deque()
        self.discovered_trigrams: Set[str] = set()
        self.load_state()

    def load_state(self):
        """Load previous state from CSV"""
        state_file = CSV_DIR / "seed_state.csv"
        if state_file.exists():
            try:
                df = pd.read_csv(state_file)
                self.used_seeds = set(df['used_seeds'].dropna())
                self.seed_queue = deque(df['queued_seeds'].dropna())
                self.discovered_trigrams = set(df['discovered'].dropna())
                logger.info(f"Loaded seed state: {len(self.used_seeds)} used, "
                           f"{len(self.seed_queue)} queued")
            except Exception as e:
                logger.warning(f"Could not load seed state: {e}")

    def save_state(self):
        """Save current state to CSV"""
        state_file = CSV_DIR / "seed_state.csv"

        # Create DataFrame with max length
        max_len = max(
            len(self.used_seeds),
            len(self.seed_queue),
            len(self.discovered_trigrams)
        )

        data = {
            'used_seeds': list(self.used_seeds) + [None] * (max_len - len(self.used_seeds)),
            'queued_seeds': list(self.seed_queue) + [None] * (max_len - len(self.seed_queue)),
            'discovered': list(self.discovered_trigrams) + [None] * (max_len - len(self.discovered_trigrams))
        }

        df = pd.DataFrame(data)
        df.to_csv(state_file, index=False)
        logger.debug("Saved seed state")

    def is_valid_seed(self, seed: str) -> bool:
        """Check if seed is valid (no stopwords)"""
        seed_upper = seed.upper()

        # Check length
        if len(seed) < 3:
            return False

        # Check for stopwords
        for stopword in self.STOPWORDS:
            if stopword in seed_upper:
                return False

        # Must contain at least one letter
        if not any(c.isalpha() for c in seed):
            return False

        return True

    def get_next_seed(self) -> Optional[str]:
        """Get next seed to use"""
        # First, try queue
        while self.seed_queue:
            seed = self.seed_queue.popleft()
            if seed not in self.used_seeds and self.is_valid_seed(seed):
                self.used_seeds.add(seed)
                self.save_state()
                return seed

        # If queue empty, use initial seeds
        for seed in self.INITIAL_SEEDS:
            if seed not in self.used_seeds:
                self.used_seeds.add(seed)
                self.save_state()
                return seed

        # If all initial seeds used, generate from discovered
        for trigram in self.discovered_trigrams:
            if trigram not in self.used_seeds and self.is_valid_seed(trigram):
                self.used_seeds.add(trigram)
                self.save_state()
                return trigram

        logger.warning("No more seeds available")
        return None

    def add_discovered_text(self, texts: List[str]):
        """Extract trigrams from discovered company names"""
        for text in texts:
            trigrams = extract_trigrams(text)
            for trigram in trigrams:
                if trigram not in self.discovered_trigrams and self.is_valid_seed(trigram):
                    self.discovered_trigrams.add(trigram)
                    if trigram not in self.used_seeds:
                        self.seed_queue.append(trigram)

        self.save_state()
        logger.debug(f"Discovered {len(self.seed_queue)} new potential seeds")

    def refine_seed(self, seed: str, level: int = 4) -> List[str]:
        """
        Refine a 3-letter seed to 4+ letters when too broad

        Args:
            seed: Base 3-letter seed
            level: Target length (default 4)

        Returns:
            List of refined seeds
        """
        if len(seed) >= level:
            return [seed]

        refined = []

        # Add letters
        for char in string.ascii_uppercase:
            refined.append(seed + char)

        # Add digits
        for digit in string.digits:
            refined.append(seed + digit)

        # Filter out invalid ones
        refined = [s for s in refined if self.is_valid_seed(s)]

        logger.info(f"Refined seed '{seed}' into {len(refined)} variations")
        return refined

    def should_refine(self, seed: str, result_count: int) -> bool:
        """
        Determine if a seed should be refined based on results

        Args:
            seed: The seed used
            result_count: Number of results/pages returned

        Returns:
            True if seed should be refined
        """
        # Refine if too many results
        if result_count > 20:  # More than 20 pages
            return True

        # Refine if seed is very generic (3 letters)
        if len(seed) == 3 and result_count > 10:
            return True

        return False

    def get_batch_seeds(self, count: int = 10) -> List[str]:
        """Get a batch of seeds for parallel processing"""
        seeds = []
        for _ in range(count):
            seed = self.get_next_seed()
            if seed:
                seeds.append(seed)
            else:
                break
        return seeds

    def reset_queue(self):
        """Reset to initial state (for testing)"""
        self.used_seeds.clear()
        self.seed_queue.clear()
        self.discovered_trigrams.clear()
        self.save_state()
        logger.info("Reset seed state")


class AdaptiveSearchStrategy:
    """Adaptive search strategy based on results"""

    def __init__(self, seed_manager: SeedManager):
        self.seed_manager = seed_manager
        self.performance_log: List[dict] = []

    def log_performance(self, seed: str, results: int, time_taken: float):
        """Log search performance for analysis"""
        self.performance_log.append({
            'seed': seed,
            'results': results,
            'time_taken': time_taken,
            'efficiency': results / time_taken if time_taken > 0 else 0
        })

    def suggest_strategy(self) -> str:
        """Suggest search strategy based on performance"""
        if not self.performance_log:
            return "standard"

        # Calculate average efficiency
        avg_efficiency = sum(p['efficiency'] for p in self.performance_log[-10:]) / min(10, len(self.performance_log))

        if avg_efficiency < 1:  # Less than 1 result per second
            return "refine"  # Need more specific searches
        elif avg_efficiency > 10:  # Too many results
            return "broaden"  # Can use broader searches
        else:
            return "standard"

    def get_adaptive_seeds(self, count: int = 5) -> List[str]:
        """Get seeds based on adaptive strategy"""
        strategy = self.suggest_strategy()

        if strategy == "refine":
            # Use longer, more specific seeds
            seeds = []
            base_seeds = self.seed_manager.get_batch_seeds(count // 2)
            for base in base_seeds:
                refined = self.seed_manager.refine_seed(base)[:2]
                seeds.extend(refined)
            return seeds[:count]

        elif strategy == "broaden":
            # Use shorter, broader seeds
            return [s for s in self.seed_manager.get_batch_seeds(count) if len(s) == 3]

        else:
            # Standard mix
            return self.seed_manager.get_batch_seeds(count)