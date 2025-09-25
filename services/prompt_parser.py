"""Data structures for database operations"""
from typing import Dict, List
from dataclasses import dataclass

@dataclass
class ParsedOperation:
    """Represents a parsed operation from user prompt"""
    action: str  # SELECT, ARCHIVE, DELETE
    table: str   # dsiactivities, dsitransactionlog, archivedsiactivities, archivedsitransactionlog
    filters: Dict[str, str]  # date_start, date_end, agent_name, server_name, etc.
    is_archive_target: bool  # True if operation targets archive table
    original_prompt: str
    confidence: float  # 0.0 to 1.0
    validation_errors: List[str]