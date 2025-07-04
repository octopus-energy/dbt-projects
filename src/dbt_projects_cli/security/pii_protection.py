"""
PII detection and sanitization for protecting sensitive data from being sent to LLMs.
"""

import hashlib
import re
from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, List, Optional, Set

from rich.console import Console

console = Console()


class PIIType(Enum):
    """Types of PII that can be detected."""

    EMAIL = "email"
    PHONE = "phone"
    SSN = "ssn"
    CREDIT_CARD = "credit_card"
    IP_ADDRESS = "ip_address"
    NAME = "name"
    ADDRESS = "address"
    DATE_OF_BIRTH = "date_of_birth"
    ID_NUMBER = "id_number"
    FINANCIAL = "financial"


@dataclass
class PIIDetectionResult:
    """Result of PII detection on a value."""

    has_pii: bool
    pii_types: Set[PIIType]
    sanitized_value: Any
    risk_level: str  # "LOW", "MEDIUM", "HIGH"


class PIIDetector:
    """Detects and sanitizes PII in data samples."""

    # Common PII column name patterns (case-insensitive)
    PII_COLUMN_PATTERNS = {
        PIIType.EMAIL: [r"email", r"e_mail", r"mail", r"email_address"],
        PIIType.PHONE: [r"phone", r"telephone", r"mobile", r"cell", r"contact"],
        PIIType.NAME: [
            r"first_name",
            r"last_name",
            r"full_name",
            r"name",
            r"customer_name",
            r"user_name",
            r"owner",
        ],
        PIIType.ADDRESS: [
            r"address",
            r"street",
            r"city",
            r"state",
            r"zip",
            r"postal",
            r"country",
            r"location",
            r"residence",
        ],
        PIIType.ID_NUMBER: [
            r"ssn",
            r"social_security",
            r"passport",
            r"license",
            r"id_number",
            r"national_id",
            r"tax_id",
            r"customer_id",
            r"user_id",
        ],
        PIIType.FINANCIAL: [
            r"account_number",
            r"routing",
            r"credit_card",
            r"card_number",
            r"bank_account",
            r"iban",
            r"swift",
        ],
        PIIType.DATE_OF_BIRTH: [r"birth", r"dob", r"born", r"age"],
    }

    # Value pattern matching (for content detection)
    VALUE_PATTERNS = {
        PIIType.EMAIL: re.compile(
            r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b"
        ),
        PIIType.PHONE: re.compile(
            r"(\+?\d{1,3}[-.\s]?)?\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}"
        ),
        PIIType.SSN: re.compile(r"\b\d{3}-?\d{2}-?\d{4}\b"),
        PIIType.CREDIT_CARD: re.compile(r"\b(?:\d{4}[-\s]?){3}\d{4}\b"),
        PIIType.IP_ADDRESS: re.compile(r"\b(?:\d{1,3}\.){3}\d{1,3}\b"),
    }

    def __init__(
        self,
        anonymization_method: str = "hash",
        include_schema_only: bool = False,
        max_rows: int = 3,
    ):
        """
        Initialize PII detector.

        Args:
            anonymization_method: "hash", "mask", or "exclude"
            include_schema_only: If True, only include column names/types, not values
            max_rows: Maximum number of rows to include
        """
        self.anonymization_method = anonymization_method
        self.include_schema_only = include_schema_only
        self.max_rows = max_rows

    def sanitize_sample_data(self, sample_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Sanitize sample data to remove PII before sending to LLM.

        Args:
            sample_data: Raw sample data from database

        Returns:
            Sanitized sample data safe for LLM processing
        """
        if not sample_data:
            return sample_data

        sanitized = sample_data.copy()

        # Get column information
        columns = sample_data.get("columns", [])
        rows = sample_data.get("sample_rows", [])

        if not columns or not rows:
            return sanitized

        # Analyze columns for PII risk
        column_analysis = self._analyze_columns(columns)

        # If schema-only mode, just return column info
        if self.include_schema_only:
            sanitized["sample_rows"] = []
            sanitized["pii_protection"] = {
                "method": "schema_only",
                "columns_analyzed": len(columns),
                "high_risk_columns": len(
                    [c for c in column_analysis if c.risk_level == "HIGH"]
                ),
            }
            return sanitized

        # Sanitize actual data rows
        sanitized_rows = []
        for i, row in enumerate(rows[: self.max_rows]):
            sanitized_row: Any
            if isinstance(row, dict):
                sanitized_row = self._sanitize_row_dict(row, column_analysis)
            elif isinstance(row, list):
                sanitized_row = self._sanitize_row_list(row, column_analysis)
            else:
                sanitized_row = self._sanitize_value(
                    row, PIIDetectionResult(False, set(), row, "LOW")
                )

            sanitized_rows.append(sanitized_row)

        sanitized["sample_rows"] = sanitized_rows
        sanitized["pii_protection"] = self._generate_protection_summary(
            column_analysis, columns
        )

        return sanitized

    def _analyze_columns(
        self, columns: List[Dict[str, Any]]
    ) -> List[PIIDetectionResult]:
        """Analyze columns for PII risk based on names and metadata."""
        results = []

        for column in columns:
            column_name = column.get("name", "").lower()
            column_type = column.get("type", "").lower()

            pii_types = set()
            risk_level = "LOW"

            # Check column name patterns
            for pii_type, patterns in self.PII_COLUMN_PATTERNS.items():
                for pattern in patterns:
                    if re.search(pattern, column_name, re.IGNORECASE):
                        pii_types.add(pii_type)
                        if pii_type in [
                            PIIType.EMAIL,
                            PIIType.PHONE,
                            PIIType.SSN,
                            PIIType.CREDIT_CARD,
                            PIIType.NAME,
                        ]:
                            risk_level = "HIGH"
                        elif pii_type in [PIIType.ADDRESS, PIIType.ID_NUMBER]:
                            risk_level = "MEDIUM" if risk_level != "HIGH" else "HIGH"

            # Special handling for certain data types
            if any(t in column_type for t in ["varchar", "string", "text"]):
                if any(
                    keyword in column_name for keyword in ["name", "email", "phone"]
                ):
                    risk_level = "HIGH"

            results.append(
                PIIDetectionResult(
                    has_pii=len(pii_types) > 0,
                    pii_types=pii_types,
                    sanitized_value=column_name,
                    risk_level=risk_level,
                )
            )

        return results

    def _sanitize_row_dict(
        self, row: Dict[str, Any], column_analysis: List[PIIDetectionResult]
    ) -> Dict[str, Any]:
        """Sanitize a row represented as a dictionary."""
        sanitized = {}

        for i, (key, value) in enumerate(row.items()):
            analysis = (
                column_analysis[i]
                if i < len(column_analysis)
                else PIIDetectionResult(False, set(), value, "LOW")
            )

            # Check the actual value for PII patterns
            value_analysis = self._detect_pii_in_value(value)

            # Combine column and value analysis
            combined_analysis = PIIDetectionResult(
                has_pii=analysis.has_pii or value_analysis.has_pii,
                pii_types=analysis.pii_types.union(value_analysis.pii_types),
                sanitized_value=value,
                risk_level=max(
                    analysis.risk_level,
                    value_analysis.risk_level,
                    key=lambda x: {"LOW": 0, "MEDIUM": 1, "HIGH": 2}[x],
                ),
            )

            sanitized[key] = self._sanitize_value(value, combined_analysis)

        return sanitized

    def _sanitize_row_list(
        self, row: List[Any], column_analysis: List[PIIDetectionResult]
    ) -> List[Any]:
        """Sanitize a row represented as a list."""
        sanitized = []

        for i, value in enumerate(row):
            analysis = (
                column_analysis[i]
                if i < len(column_analysis)
                else PIIDetectionResult(False, set(), value, "LOW")
            )

            # Check the actual value for PII patterns
            value_analysis = self._detect_pii_in_value(value)

            # Combine column and value analysis
            combined_analysis = PIIDetectionResult(
                has_pii=analysis.has_pii or value_analysis.has_pii,
                pii_types=analysis.pii_types.union(value_analysis.pii_types),
                sanitized_value=value,
                risk_level=max(
                    analysis.risk_level,
                    value_analysis.risk_level,
                    key=lambda x: {"LOW": 0, "MEDIUM": 1, "HIGH": 2}[x],
                ),
            )

            sanitized.append(self._sanitize_value(value, combined_analysis))

        return sanitized

    def _detect_pii_in_value(self, value: Any) -> PIIDetectionResult:
        """Detect PII patterns in actual values."""
        if not isinstance(value, str):
            return PIIDetectionResult(False, set(), value, "LOW")

        pii_types = set()
        risk_level = "LOW"

        # Check value patterns
        for pii_type, pattern in self.VALUE_PATTERNS.items():
            if pattern.search(value):
                pii_types.add(pii_type)
                if pii_type in [PIIType.EMAIL, PIIType.SSN, PIIType.CREDIT_CARD]:
                    risk_level = "HIGH"
                elif pii_type == PIIType.PHONE:
                    risk_level = "MEDIUM" if risk_level != "HIGH" else "HIGH"

        return PIIDetectionResult(
            has_pii=len(pii_types) > 0,
            pii_types=pii_types,
            sanitized_value=value,
            risk_level=risk_level,
        )

    def _sanitize_value(self, value: Any, analysis: PIIDetectionResult) -> Any:
        """Sanitize a single value based on PII analysis."""
        if not analysis.has_pii:
            return value

        if analysis.risk_level == "HIGH":
            if self.anonymization_method == "hash":
                return self._hash_value(value)
            elif self.anonymization_method == "mask":
                return self._mask_value(value, analysis.pii_types)
            elif self.anonymization_method == "exclude":
                return "[REDACTED]"
        elif analysis.risk_level == "MEDIUM":
            if self.anonymization_method in ["hash", "mask"]:
                return self._mask_value(value, analysis.pii_types)
            else:
                return "[REDACTED]"

        return value

    def _hash_value(self, value: Any) -> Optional[str]:
        """Create a consistent hash of the value."""
        if value is None:
            return None

        value_str = str(value)
        hash_obj = hashlib.sha256(value_str.encode())
        return f"[HASH:{hash_obj.hexdigest()[:8]}]"

    def _mask_value(self, value: Any, pii_types: Set[PIIType]) -> Optional[str]:
        """Mask the value while preserving some structure."""
        if value is None:
            return None

        value_str = str(value)

        if PIIType.EMAIL in pii_types:
            # email@domain.com -> e****@d*****.com
            if "@" in value_str:
                local, domain = value_str.split("@", 1)
                masked_local = (
                    local[0] + "*" * (len(local) - 1) if len(local) > 1 else "*"
                )
                if "." in domain:
                    domain_parts = domain.split(".")
                    masked_domain = domain_parts[0][0] + "*" * (
                        len(domain_parts[0]) - 1
                    )
                    masked_domain += "." + domain_parts[-1]
                else:
                    masked_domain = (
                        domain[0] + "*" * (len(domain) - 1) if len(domain) > 1 else "*"
                    )
                return f"{masked_local}@{masked_domain}"

        if PIIType.PHONE in pii_types:
            # Preserve format but mask digits: (555) 123-4567 -> (***) ***-**67
            if len(value_str) >= 4:
                return "*" * (len(value_str) - 2) + value_str[-2:]

        # Default masking: preserve first and last char if length > 2
        if len(value_str) <= 2:
            return "*" * len(value_str)
        elif len(value_str) <= 4:
            return value_str[0] + "*" * (len(value_str) - 1)
        else:
            return value_str[0] + "*" * (len(value_str) - 2) + value_str[-1]

    def _generate_protection_summary(
        self,
        column_analysis: List[PIIDetectionResult],
        columns: Optional[List[Dict[str, Any]]] = None,
    ) -> Dict[str, Any]:
        """Generate a summary of PII protection applied."""
        high_risk = len([c for c in column_analysis if c.risk_level == "HIGH"])
        medium_risk = len([c for c in column_analysis if c.risk_level == "MEDIUM"])
        low_risk = len([c for c in column_analysis if c.risk_level == "LOW"])

        # Detailed logging of protection results
        protected_columns = []
        high_risk_columns = []
        medium_risk_columns = []
        low_risk_columns = []

        for i, analysis in enumerate(column_analysis):
            column_name = (
                columns[i].get("name", f"column_{i}")
                if columns and i < len(columns)
                else f"column_{i}"
            )

            if analysis.risk_level == "HIGH":
                high_risk_columns.append(column_name)
                if analysis.has_pii:
                    protected_columns.append(
                        f"{column_name} (HIGH risk - "
                        f"{', '.join([t.value for t in analysis.pii_types])})"
                    )
            elif analysis.risk_level == "MEDIUM":
                medium_risk_columns.append(column_name)
                if analysis.has_pii:
                    protected_columns.append(
                        f"{column_name} (MEDIUM risk - "
                        f"{', '.join([t.value for t in analysis.pii_types])})"
                    )
            else:
                low_risk_columns.append(column_name)

        # Log detailed protection information
        if protected_columns:
            console.print("[yellow]🔒 PII Protection Details:[/yellow]")
            console.print(f"[yellow]   Method: {self.anonymization_method}[/yellow]")
            console.print(
                f"[yellow]   Protected columns: "
                f"{', '.join(protected_columns)}[/yellow]"
            )

        if high_risk_columns:
            console.print(
                f"[red]⚠️  High-risk columns: {', '.join(high_risk_columns)}[/red]"
            )

        if medium_risk_columns:
            console.print(
                f"[yellow]⚠️  Medium-risk columns: "
                f"{', '.join(medium_risk_columns)}[/yellow]"
            )

        if low_risk_columns:
            console.print(
                f"[green]✅ Low-risk columns: {', '.join(low_risk_columns)}[/green]"
            )

        return {
            "method": self.anonymization_method,
            "columns_analyzed": len(column_analysis),
            "high_risk_columns": high_risk,
            "medium_risk_columns": medium_risk,
            "low_risk_columns": low_risk,
            "protection_applied": high_risk > 0 or medium_risk > 0,
            "protected_column_details": protected_columns,
            "high_risk_column_names": high_risk_columns,
            "medium_risk_column_names": medium_risk_columns,
            "low_risk_column_names": low_risk_columns,
        }


def create_pii_detector(protection_level: str = "high") -> PIIDetector:
    """
    Create a PII detector with predefined protection levels.

    Args:
        protection_level: "strict", "high", "medium", or "schema_only"
    """
    if protection_level == "strict":
        return PIIDetector(
            anonymization_method="exclude", include_schema_only=False, max_rows=2
        )
    elif protection_level == "high":
        return PIIDetector(
            anonymization_method="hash", include_schema_only=False, max_rows=3
        )
    elif protection_level == "medium":
        return PIIDetector(
            anonymization_method="mask", include_schema_only=False, max_rows=3
        )
    elif protection_level == "schema_only":
        return PIIDetector(
            anonymization_method="exclude", include_schema_only=True, max_rows=0
        )
    else:
        raise ValueError(f"Unknown protection level: {protection_level}")
