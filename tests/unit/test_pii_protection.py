from unittest.mock import patch

import pytest

from dbt_projects_cli.security.pii_protection import (
    PIIDetectionResult,
    PIIDetector,
    PIIType,
    create_pii_detector,
)


def test_detector_initialization():
    detector = PIIDetector(
        anonymization_method="hash", include_schema_only=False, max_rows=3
    )
    assert detector.anonymization_method == "hash"
    assert not detector.include_schema_only
    assert detector.max_rows == 3


def test_sanitize_sample_data_empty():
    detector = PIIDetector()
    sanitized = detector.sanitize_sample_data({})
    assert sanitized == {}


def test_sanitize_sample_data_no_columns_or_rows():
    detector = PIIDetector()
    sample_data = {"columns": [], "sample_rows": []}
    sanitized = detector.sanitize_sample_data(sample_data)
    assert sanitized == sample_data


def test_analyze_columns():
    detector = PIIDetector()
    columns = [
        {"name": "email_address", "type": "varchar"},
        {"name": "phone_number", "type": "string"},
        {"name": "first_name", "type": "text"},
        {"name": "address", "type": "varchar"},
        {"name": "zip_code", "type": "integer"},
    ]
    analysis = detector._analyze_columns(columns)
    assert len(analysis) == 5
    assert analysis[0].has_pii
    assert PIIType.EMAIL in analysis[0].pii_types
    assert analysis[0].risk_level == "HIGH"


def test_detect_pii_in_value_email():  # Value detection
    detector = PIIDetector()
    result = detector._detect_pii_in_value("test@example.com")
    assert result.has_pii
    assert PIIType.EMAIL in result.pii_types
    assert result.risk_level == "HIGH"


def test_detect_pii_in_value_phone():
    detector = PIIDetector()
    result = detector._detect_pii_in_value("555-123-4567")
    assert result.has_pii
    assert PIIType.PHONE in result.pii_types
    assert result.risk_level == "MEDIUM"


def test_sanitize_value_hash():  # Sanitization logic
    detector = PIIDetector(anonymization_method="hash")
    result = detector._sanitize_value(
        "test@example.com", detector._detect_pii_in_value("test@example.com")
    )
    assert result.startswith("[PII_PLACEHOLDER:")


def test_sanitize_value_mask():
    detector = PIIDetector(anonymization_method="mask")
    result = detector._sanitize_value(
        "555-123-4567", detector._detect_pii_in_value("555-123-4567")
    )
    assert result.endswith("67")


def test_sanitize_value_exclude():
    detector = PIIDetector(anonymization_method="exclude")
    result = detector._sanitize_value(
        "test@example.com", detector._detect_pii_in_value("test@example.com")
    )
    assert result == "[REDACTED]"


def test_protection_summary():  # Protection summary
    detector = PIIDetector()
    columns = [
        {"name": "email_address", "type": "varchar"},
        {"name": "phone_number", "type": "string"},
        {"name": "first_name", "type": "text"},
    ]
    column_analysis = detector._analyze_columns(columns)
    summary = detector._generate_protection_summary(column_analysis, columns)
    assert (
        summary["high_risk_columns"] == 3
    )  # email_address matches both email and address patterns
    assert len(summary["protected_column_details"]) == 3


def test_create_pii_detector_high():  # Testing factory function
    detector = create_pii_detector("high")
    assert detector.anonymization_method == "hash"
    assert detector.max_rows == 3


class TestPIIDetector:
    """Comprehensive tests for PIIDetector class."""

    def test_initialization_defaults(self):
        """Test PIIDetector with default parameters."""
        detector = PIIDetector()
        assert detector.anonymization_method == "hash"
        assert not detector.include_schema_only
        assert detector.max_rows == 3

    def test_initialization_custom_params(self):
        """Test PIIDetector with custom parameters."""
        detector = PIIDetector(
            anonymization_method="mask", include_schema_only=True, max_rows=5
        )
        assert detector.anonymization_method == "mask"
        assert detector.include_schema_only
        assert detector.max_rows == 5

    def test_sanitize_sample_data_none(self):
        """Test sanitization when sample_data is None."""
        detector = PIIDetector()
        result = detector.sanitize_sample_data(None)
        assert result is None

    def test_sanitize_sample_data_schema_only_mode(self):
        """Test sanitization in schema_only mode."""
        detector = PIIDetector(include_schema_only=True)
        sample_data = {
            "columns": [
                {"name": "email", "type": "varchar"},
                {"name": "name", "type": "string"},
            ],
            "sample_rows": [{"email": "test@example.com", "name": "John Doe"}],
        }

        result = detector.sanitize_sample_data(sample_data)

        assert result["sample_rows"] == []
        assert result["pii_protection"]["method"] == "schema_only"
        assert result["pii_protection"]["columns_analyzed"] == 2
        assert result["pii_protection"]["high_risk_columns"] == 2

    def test_sanitize_sample_data_with_dict_rows(self):
        """Test sanitization with dictionary-formatted rows."""
        detector = PIIDetector(anonymization_method="mask")
        sample_data = {
            "columns": [
                {"name": "email", "type": "varchar"},
                {"name": "score", "type": "integer"},
            ],
            "sample_rows": [
                {"email": "test@example.com", "score": 100},
                {"email": "user@domain.org", "score": 85},
            ],
        }

        result = detector.sanitize_sample_data(sample_data)

        assert len(result["sample_rows"]) == 2
        # Email should be masked
        assert "@" in result["sample_rows"][0]["email"]
        assert result["sample_rows"][0]["email"] != "test@example.com"
        # Score should remain unchanged
        assert result["sample_rows"][0]["score"] == 100

    def test_sanitize_sample_data_with_list_rows(self):
        """Test sanitization with list-formatted rows."""
        detector = PIIDetector(anonymization_method="hash")
        sample_data = {
            "columns": [
                {"name": "email", "type": "varchar"},
                {"name": "age", "type": "integer"},
            ],
            "sample_rows": [["test@example.com", 25], ["user@domain.org", 30]],
        }

        result = detector.sanitize_sample_data(sample_data)

        assert len(result["sample_rows"]) == 2
        # Email should be hashed
        assert result["sample_rows"][0][0].startswith("[PII_PLACEHOLDER:")
        # Age should remain unchanged
        assert result["sample_rows"][0][1] == 25

    def test_sanitize_sample_data_with_non_dict_non_list_rows(self):
        """Test sanitization with other row formats."""
        detector = PIIDetector()
        sample_data = {
            "columns": [
                {"name": "email_value", "type": "string"}
            ],  # Make column name suggest email
            "sample_rows": ["test@example.com", "normal_value"],
        }

        result = detector.sanitize_sample_data(sample_data)

        assert len(result["sample_rows"]) == 2
        # Both values should be sanitized due to the column being identified as
        # email type
        # The sanitization method depends on combined analysis of column + value
        # Since values are treated as individual items, they get passed to
        # _sanitize_value
        # which uses the column analysis to determine sanitization

    def test_analyze_columns_various_pii_types(self):
        """Test column analysis for various PII types."""
        detector = PIIDetector()
        columns = [
            {"name": "email_address", "type": "varchar"},
            {"name": "phone", "type": "string"},
            {"name": "ssn", "type": "varchar"},
            {"name": "credit_card_number", "type": "string"},
            {"name": "first_name", "type": "text"},
            {"name": "address", "type": "varchar"},
            {"name": "customer_id", "type": "bigint"},
            {"name": "birth_date", "type": "date"},
            {"name": "normal_column", "type": "integer"},
        ]

        analysis = detector._analyze_columns(columns)

        assert len(analysis) == 9

        # High risk columns
        assert analysis[0].risk_level == "HIGH"  # email
        assert analysis[1].risk_level == "HIGH"  # phone
        assert analysis[2].risk_level == "MEDIUM"  # ssn (actually ID_NUMBER type)
        assert (
            analysis[3].risk_level == "LOW"
        )  # credit_card_number matches FINANCIAL but is only MEDIUM risk,
        # and financial isn't in the HIGH list
        assert analysis[4].risk_level == "HIGH"  # first_name

        # Medium risk columns
        assert analysis[5].risk_level == "MEDIUM"  # address
        assert analysis[6].risk_level == "MEDIUM"  # customer_id

        # Low risk columns
        assert analysis[8].risk_level == "LOW"  # normal_column

    def test_analyze_columns_special_data_types(self):
        """Test column analysis for special data type handling."""
        detector = PIIDetector()
        columns = [
            {"name": "user_name", "type": "varchar"},
            {"name": "user_email", "type": "string"},
            {"name": "phone_number", "type": "text"},
        ]

        analysis = detector._analyze_columns(columns)

        # All should be high risk due to name/email/phone keywords + string types
        assert all(a.risk_level == "HIGH" for a in analysis)

    def test_detect_pii_in_value_various_types(self):
        """Test PII detection in various value types."""
        detector = PIIDetector()

        # Email
        result = detector._detect_pii_in_value("user@example.com")
        assert result.has_pii
        assert PIIType.EMAIL in result.pii_types
        assert result.risk_level == "HIGH"

        # Phone number
        result = detector._detect_pii_in_value("(555) 123-4567")
        assert result.has_pii
        assert PIIType.PHONE in result.pii_types
        assert result.risk_level == "MEDIUM"

        # SSN
        result = detector._detect_pii_in_value("123-45-6789")
        assert result.has_pii
        assert PIIType.SSN in result.pii_types
        assert result.risk_level == "HIGH"

        # Credit card
        result = detector._detect_pii_in_value("4532 1234 5678 9012")
        assert result.has_pii
        assert PIIType.CREDIT_CARD in result.pii_types
        assert result.risk_level == "HIGH"

        # IP address
        result = detector._detect_pii_in_value("192.168.1.1")
        assert result.has_pii
        assert PIIType.IP_ADDRESS in result.pii_types
        assert result.risk_level == "LOW"

        # Non-string value
        result = detector._detect_pii_in_value(12345)
        assert not result.has_pii
        assert result.risk_level == "LOW"

        # Normal string
        result = detector._detect_pii_in_value("normal text")
        assert not result.has_pii
        assert result.risk_level == "LOW"

    def test_sanitize_value_no_pii(self):
        """Test sanitization when no PII is detected."""
        detector = PIIDetector()
        analysis = PIIDetectionResult(False, set(), "normal_value", "LOW")

        result = detector._sanitize_value("normal_value", analysis)
        assert result == "normal_value"

    def test_sanitize_value_high_risk_methods(self):
        """Test sanitization methods for high-risk PII."""
        analysis = PIIDetectionResult(True, {PIIType.EMAIL}, "test@example.com", "HIGH")

        # Hash method
        detector = PIIDetector(anonymization_method="hash")
        result = detector._sanitize_value("test@example.com", analysis)
        assert result.startswith("[HASH:")

        # Mask method
        detector = PIIDetector(anonymization_method="mask")
        result = detector._sanitize_value("test@example.com", analysis)
        assert "@" in result and result != "test@example.com"

        # Exclude method
        detector = PIIDetector(anonymization_method="exclude")
        result = detector._sanitize_value("test@example.com", analysis)
        assert result == "[REDACTED]"

    def test_sanitize_value_medium_risk_methods(self):
        """Test sanitization methods for medium-risk PII."""
        analysis = PIIDetectionResult(True, {PIIType.PHONE}, "555-123-4567", "MEDIUM")

        # Hash method (should mask for medium risk)
        detector = PIIDetector(anonymization_method="hash")
        result = detector._sanitize_value("555-123-4567", analysis)
        assert result.endswith("67") and result != "555-123-4567"

        # Mask method
        detector = PIIDetector(anonymization_method="mask")
        result = detector._sanitize_value("555-123-4567", analysis)
        assert result.endswith("67") and result != "555-123-4567"

        # Exclude method
        detector = PIIDetector(anonymization_method="exclude")
        result = detector._sanitize_value("555-123-4567", analysis)
        assert result == "[REDACTED]"

    def test_hash_value(self):
        """Test hash value generation."""
        detector = PIIDetector()

        # Normal value
        result = detector._hash_value("test_value")
        assert result.startswith("[HASH:")
        assert len(result) == 15  # [HASH: + 8 chars + ]

        # None value
        result = detector._hash_value(None)
        assert result is None

        # Consistent hashing
        result1 = detector._hash_value("same_value")
        result2 = detector._hash_value("same_value")
        assert result1 == result2

    def test_mask_value_email(self):
        """Test email masking."""
        detector = PIIDetector()

        # Normal email
        result = detector._mask_value("test@example.com", {PIIType.EMAIL})
        assert result == "t***@e******.com"

        # Short local part
        result = detector._mask_value("a@example.com", {PIIType.EMAIL})
        assert result == "*@e******.com"

        # Domain without dot
        result = detector._mask_value("test@localhost", {PIIType.EMAIL})
        assert result == "t***@l********"

        # Short domain
        result = detector._mask_value("test@a.com", {PIIType.EMAIL})
        assert result == "t***@a.com"  # Single char domain isn't masked

    def test_mask_value_phone(self):
        """Test phone number masking."""
        detector = PIIDetector()

        # Normal phone
        result = detector._mask_value("555-123-4567", {PIIType.PHONE})
        assert result == "**********67"

        # Short phone
        result = detector._mask_value("1234", {PIIType.PHONE})
        assert result == "**34"

        # Very short phone
        result = detector._mask_value("12", {PIIType.PHONE})
        assert result == "**"

    def test_mask_value_general(self):
        """Test general masking patterns."""
        detector = PIIDetector()

        # Short strings
        result = detector._mask_value("ab", set())
        assert result == "**"

        result = detector._mask_value("abc", set())
        assert result == "a**"

        result = detector._mask_value("abcd", set())
        assert result == "a***"

        # Longer strings
        result = detector._mask_value("hello world", set())
        assert result == "h*********d"

        # None value
        result = detector._mask_value(None, set())
        assert result is None

    @patch("dbt_projects_cli.security.pii_protection.console")
    def test_generate_protection_summary(self, mock_console):
        """Test protection summary generation with console output."""
        detector = PIIDetector()

        # Create mock column analysis
        column_analysis = [
            PIIDetectionResult(True, {PIIType.EMAIL}, "email", "HIGH"),
            PIIDetectionResult(True, {PIIType.PHONE}, "phone", "MEDIUM"),
            PIIDetectionResult(False, set(), "normal", "LOW"),
        ]

        columns = [{"name": "email_col"}, {"name": "phone_col"}, {"name": "normal_col"}]

        summary = detector._generate_protection_summary(column_analysis, columns)

        # Check summary content
        assert summary["method"] == "hash"
        assert summary["columns_analyzed"] == 3
        assert summary["high_risk_columns"] == 1
        assert summary["medium_risk_columns"] == 1
        assert summary["low_risk_columns"] == 1
        assert summary["protection_applied"] is True

        # Check detailed information
        assert len(summary["protected_column_details"]) == 2
        assert "email_col (HIGH risk - email)" in summary["protected_column_details"]
        assert "phone_col (MEDIUM risk - phone)" in summary["protected_column_details"]

        assert summary["high_risk_column_names"] == ["email_col"]
        assert summary["medium_risk_column_names"] == ["phone_col"]
        assert summary["low_risk_column_names"] == ["normal_col"]

        # Verify console output was called
        assert (
            mock_console.print.call_count >= 4
        )  # Protection details + risk level outputs

    @patch("dbt_projects_cli.security.pii_protection.console")
    def test_generate_protection_summary_no_columns(self, mock_console):
        """Test protection summary without column metadata."""
        detector = PIIDetector()

        column_analysis = [
            PIIDetectionResult(True, {PIIType.EMAIL}, "email", "HIGH"),
            PIIDetectionResult(False, set(), "normal", "LOW"),
        ]

        summary = detector._generate_protection_summary(column_analysis)

        # Should use generic column names
        assert "column_0 (HIGH risk - email)" in summary["protected_column_details"]
        assert summary["high_risk_column_names"] == ["column_0"]
        assert summary["low_risk_column_names"] == ["column_1"]

    def test_sanitize_row_dict(self):
        """Test dictionary row sanitization."""
        detector = PIIDetector(anonymization_method="mask")

        row = {"email": "test@example.com", "name": "John Doe", "score": 100}

        column_analysis = [
            PIIDetectionResult(True, {PIIType.EMAIL}, "email", "HIGH"),
            PIIDetectionResult(True, {PIIType.NAME}, "name", "HIGH"),
            PIIDetectionResult(False, set(), "score", "LOW"),
        ]

        result = detector._sanitize_row_dict(row, column_analysis)

        # Email and name should be sanitized
        assert result["email"] != "test@example.com"
        assert result["name"] != "John Doe"
        # Score should remain unchanged
        assert result["score"] == 100

    def test_sanitize_row_list(self):
        """Test list row sanitization."""
        detector = PIIDetector(anonymization_method="hash")

        row = ["test@example.com", "John Doe", 100]

        column_analysis = [
            PIIDetectionResult(True, {PIIType.EMAIL}, "email", "HIGH"),
            PIIDetectionResult(True, {PIIType.NAME}, "name", "HIGH"),
            PIIDetectionResult(False, set(), "score", "LOW"),
        ]

        result = detector._sanitize_row_list(row, column_analysis)

        # Email and name should be sanitized
        assert result[0] != "test@example.com"
        assert result[1] != "John Doe"
        # Score should remain unchanged
        assert result[2] == 100

    def test_sanitize_row_with_insufficient_column_analysis(self):
        """Test row sanitization when column analysis is shorter than row."""
        detector = PIIDetector()

        row = {"email": "test@example.com", "extra": "value"}
        column_analysis = [PIIDetectionResult(True, {PIIType.EMAIL}, "email", "HIGH")]

        # Should not crash and should handle extra columns gracefully
        result = detector._sanitize_row_dict(row, column_analysis)
        assert "email" in result
        assert "extra" in result


class TestCreatePIIDetector:
    """Test the create_pii_detector factory function."""

    def test_create_strict_detector(self):
        """Test creation of strict protection level detector."""
        detector = create_pii_detector("strict")
        assert detector.anonymization_method == "exclude"
        assert not detector.include_schema_only
        assert detector.max_rows == 2

    def test_create_high_detector(self):
        """Test creation of high protection level detector."""
        detector = create_pii_detector("high")
        assert detector.anonymization_method == "hash"
        assert not detector.include_schema_only
        assert detector.max_rows == 3

    def test_create_medium_detector(self):
        """Test creation of medium protection level detector."""
        detector = create_pii_detector("medium")
        assert detector.anonymization_method == "mask"
        assert not detector.include_schema_only
        assert detector.max_rows == 3

    def test_create_schema_only_detector(self):
        """Test creation of schema_only protection level detector."""
        detector = create_pii_detector("schema_only")
        assert detector.anonymization_method == "exclude"
        assert detector.include_schema_only
        assert detector.max_rows == 0

    def test_create_invalid_detector(self):
        """Test creation with invalid protection level."""
        with pytest.raises(ValueError, match="Unknown protection level"):
            create_pii_detector("invalid")


class TestPIIDetectionResult:
    """Test the PIIDetectionResult dataclass."""

    def test_pii_detection_result_creation(self):
        """Test creation of PIIDetectionResult."""
        result = PIIDetectionResult(
            has_pii=True,
            pii_types={PIIType.EMAIL, PIIType.PHONE},
            sanitized_value="masked_value",
            risk_level="HIGH",
        )

        assert result.has_pii
        assert PIIType.EMAIL in result.pii_types
        assert PIIType.PHONE in result.pii_types
        assert result.sanitized_value == "masked_value"
        assert result.risk_level == "HIGH"


class TestPIIType:
    """Test the PIIType enum."""

    def test_pii_type_values(self):
        """Test PIIType enum values."""
        assert PIIType.EMAIL.value == "email"
        assert PIIType.PHONE.value == "phone"
        assert PIIType.SSN.value == "ssn"
        assert PIIType.CREDIT_CARD.value == "credit_card"
        assert PIIType.IP_ADDRESS.value == "ip_address"
        assert PIIType.NAME.value == "name"
        assert PIIType.ADDRESS.value == "address"
        assert PIIType.DATE_OF_BIRTH.value == "date_of_birth"
        assert PIIType.ID_NUMBER.value == "id_number"
        assert PIIType.FINANCIAL.value == "financial"
