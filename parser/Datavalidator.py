from pyspark.sql import functions as F


class DataValidator:

    def __init__(self, spark):
        self.spark = spark

    # ============================================================
    # PUBLIC VALIDATION METHOD
    # ============================================================

    def validate(
        self,
        expected_df,
        actual_df,
        key_columns=None
    ):
        """
        Validate expected vs actual DataFrames.

        Validation levels:
            1. Schema validation
            2. Row count validation
            3. Key-based data reconciliation
            4. Column-level value comparison

        key_columns:
            Business key columns used to identify records.
            Example:
                ["CUSTOMER_ID"]
        """

        report = {
            "schema_validation": "PASS",
            "row_count_validation": "PASS",
            "data_validation": "PASS",
            "overall_status": "PASS",
            "errors": [],
            "warnings": []
        }

        # --------------------------------------------------------
        # Validate input DataFrames
        # --------------------------------------------------------

        if expected_df is None:
            report["overall_status"] = "FAIL"
            report["data_validation"] = "FAIL"
            report["errors"].append(
                "Expected DataFrame is None."
            )
            return report

        if actual_df is None:
            report["overall_status"] = "FAIL"
            report["data_validation"] = "FAIL"
            report["errors"].append(
                "Actual DataFrame is None."
            )
            return report

        # --------------------------------------------------------
        # Normalize key columns
        # --------------------------------------------------------

        if key_columns is None:
            key_columns = []

        if isinstance(key_columns, str):
            key_columns = [key_columns]

        key_columns = [
            column.strip()
            for column in key_columns
            if column and column.strip()
        ]

        # --------------------------------------------------------
        # 1. SCHEMA VALIDATION
        # --------------------------------------------------------

        expected_columns = expected_df.columns
        actual_columns = actual_df.columns

        report["expected_columns"] = expected_columns
        report["actual_columns"] = actual_columns

        if expected_columns != actual_columns:

            report["schema_validation"] = "FAIL"
            report["overall_status"] = "FAIL"

            report["errors"].append(
                "Schema mismatch. "
                f"Expected: {expected_columns}, "
                f"Actual: {actual_columns}"
            )

        # --------------------------------------------------------
        # Check whether key columns exist
        # --------------------------------------------------------

        missing_expected_keys = [
            column
            for column in key_columns
            if column not in expected_columns
        ]

        missing_actual_keys = [
            column
            for column in key_columns
            if column not in actual_columns
        ]

        if missing_expected_keys:

            report["overall_status"] = "FAIL"
            report["data_validation"] = "FAIL"

            report["errors"].append(
                "Key columns missing from expected DataFrame: "
                f"{missing_expected_keys}"
            )

        if missing_actual_keys:

            report["overall_status"] = "FAIL"
            report["data_validation"] = "FAIL"

            report["errors"].append(
                "Key columns missing from actual DataFrame: "
                f"{missing_actual_keys}"
            )

        # --------------------------------------------------------
        # 2. ROW COUNT VALIDATION
        # --------------------------------------------------------

        expected_count = expected_df.count()
        actual_count = actual_df.count()

        report["expected_row_count"] = expected_count
        report["actual_row_count"] = actual_count

        if expected_count != actual_count:

            report["row_count_validation"] = "FAIL"
            report["overall_status"] = "FAIL"

            report["errors"].append(
                "Row count mismatch. "
                f"Expected: {expected_count}, "
                f"Actual: {actual_count}"
            )

        # --------------------------------------------------------
        # 3. DATA VALIDATION
        # --------------------------------------------------------

        if (
            report["schema_validation"] == "PASS"
            and key_columns
            and not missing_expected_keys
            and not missing_actual_keys
        ):

            data_result = self._compare_data(
                expected_df=expected_df,
                actual_df=actual_df,
                key_columns=key_columns
            )

            report["mismatched_rows"] = (
                data_result["mismatched_rows"]
            )

            report["missing_rows"] = (
                data_result["missing_rows"]
            )

            report["extra_rows"] = (
                data_result["extra_rows"]
            )

            report["duplicate_expected_keys"] = (
                data_result["duplicate_expected_keys"]
            )

            report["duplicate_actual_keys"] = (
                data_result["duplicate_actual_keys"]
            )

            # ----------------------------------------------------
            # Determine final data validation status
            # ----------------------------------------------------

            if (
                data_result["mismatched_rows"] > 0
                or data_result["missing_rows"] > 0
                or data_result["extra_rows"] > 0
            ):

                report["data_validation"] = "FAIL"
                report["overall_status"] = "FAIL"

            # ----------------------------------------------------
            # Duplicate keys are warnings by default
            # ----------------------------------------------------

            if (
                data_result["duplicate_expected_keys"] > 0
                or data_result["duplicate_actual_keys"] > 0
            ):

                report["warnings"].append(
                    "Duplicate business keys detected."
                )

        elif not key_columns:

            report["data_validation"] = "FAIL"
            report["overall_status"] = "FAIL"

            report["errors"].append(
                "No key columns were supplied for data reconciliation."
            )

        # --------------------------------------------------------
        # Return final report
        # --------------------------------------------------------

        return report

    # ============================================================
    # DATA COMPARISON
    # ============================================================

    def _compare_data(
        self,
        expected_df,
        actual_df,
        key_columns
    ):
        """
        Compare expected and actual data using business keys.

        This method intentionally does NOT use DataFrame.subtract().

        Instead:

            Expected keys
                    |
                    | LEFT ANTI
                    v
              Missing rows

            Actual keys
                    |
                    | LEFT ANTI
                    v
               Extra rows

            Expected + Actual
                    |
                    | FULL OUTER JOIN
                    v
              Value comparison
        """

        all_columns = expected_df.columns

        compare_columns = [
            column
            for column in all_columns
            if column not in key_columns
        ]

        # --------------------------------------------------------
        # Select only required columns
        # --------------------------------------------------------

        expected_selected = expected_df.select(
            *all_columns
        )

        actual_selected = actual_df.select(
            *all_columns
        )

        # --------------------------------------------------------
        # Remove duplicate key combinations for key existence
        # checks.
        # --------------------------------------------------------

        expected_keys = (
            expected_selected
            .select(*key_columns)
            .dropDuplicates()
            .alias("expected")
        )

        actual_keys = (
            actual_selected
            .select(*key_columns)
            .dropDuplicates()
            .alias("actual")
        )

        # --------------------------------------------------------
        # MISSING ROWS
        #
        # Present in expected but absent in actual.
        #
        # LEFT ANTI JOIN:
        #
        # expected - actual
        # --------------------------------------------------------

        missing_rows_df = (
            expected_keys
            .join(
                actual_keys,
                on=self._build_null_safe_join_condition(
                    expected_keys,
                    actual_keys,
                    key_columns,
                    "expected",
                    "actual"
                ),
                how="left_anti"
            )
        )

        missing_rows = missing_rows_df.count()

        # --------------------------------------------------------
        # EXTRA ROWS
        #
        # Present in actual but absent in expected.
        #
        # LEFT ANTI JOIN:
        #
        # actual - expected
        # --------------------------------------------------------

        extra_rows_df = (
            actual_keys
            .join(
                expected_keys,
                on=self._build_null_safe_join_condition(
                    actual_keys,
                    expected_keys,
                    key_columns,
                    "actual",
                    "expected"
                ),
                how="left_anti"
            )
        )

        extra_rows = extra_rows_df.count()

        # --------------------------------------------------------
        # DUPLICATE KEY VALIDATION
        # --------------------------------------------------------

        expected_duplicate_keys = (
            expected_selected
            .groupBy(*key_columns)
            .count()
            .filter(F.col("count") > 1)
            .count()
        )

        actual_duplicate_keys = (
            actual_selected
            .groupBy(*key_columns)
            .count()
            .filter(F.col("count") > 1)
            .count()
        )

        # --------------------------------------------------------
        # KEY-BASED FULL OUTER JOIN
        #
        # Used to compare values for records whose keys exist
        # in both DataFrames.
        # --------------------------------------------------------

        expected_alias = expected_selected.alias("expected")
        actual_alias = actual_selected.alias("actual")

        join_condition = self._build_null_safe_join_condition(
            expected_alias,
            actual_alias,
            key_columns,
            "expected",
            "actual"
        )

        joined_df = (
            expected_alias
            .join(
                actual_alias,
                join_condition,
                "full_outer"
            )
        )

        # --------------------------------------------------------
        # Build mismatch condition
        # --------------------------------------------------------

        mismatch_condition = None

        for column in compare_columns:
            expected_value = F.col(f"expected.{column}")
            actual_value = F.col(f"actual.{column}")

            # Compare canonical string representations while preserving
            # null-safe equality. This avoids false mismatches caused by
            # compatible Spark datatype/representation differences.
            expected_normalized = F.when(
                expected_value.isNull(),
                F.lit(None)
            ).otherwise(
                F.trim(expected_value.cast("string"))
            )

            actual_normalized = F.when(
                actual_value.isNull(),
                F.lit(None)
            ).otherwise(
                F.trim(actual_value.cast("string"))
            )

            condition = ~expected_normalized.eqNullSafe(
                actual_normalized
            )

            if mismatch_condition is None:
                mismatch_condition = condition
            else:
                mismatch_condition = (
                    mismatch_condition | condition
                )

        # --------------------------------------------------------
        # Count mismatched rows
        # --------------------------------------------------------

        if mismatch_condition is None:

            mismatched_rows = 0

        else:

            # Only compare records where the key exists in both
            # DataFrames.
            common_key_condition = (
                self._build_common_key_condition(
                    key_columns
                )
            )

            mismatched_rows = (
                joined_df
                .filter(
                    common_key_condition
                    & mismatch_condition
                )
                .count()
            )

        return {
            "mismatched_rows": mismatched_rows,
            "missing_rows": missing_rows,
            "extra_rows": extra_rows,
            "duplicate_expected_keys": (
                expected_duplicate_keys
            ),
            "duplicate_actual_keys": (
                actual_duplicate_keys
            )
        }

    # ============================================================
    # NULL-SAFE JOIN CONDITION
    # ============================================================

    def _build_null_safe_join_condition(
        self,
        left_df,
        right_df,
        key_columns,
        left_alias,
        right_alias
    ):
        """
        Build a null-safe join condition for business keys.

        Example:

            expected.CUSTOMER_ID
                <=> actual.CUSTOMER_ID
        """

        conditions = []

        for column in key_columns:

            conditions.append(
                F.col(
                    f"{left_alias}.{column}"
                ).eqNullSafe(
                    F.col(
                        f"{right_alias}.{column}"
                    )
                )
            )

        if not conditions:
            return F.lit(True)

        condition = conditions[0]

        for additional_condition in conditions[1:]:

            condition = (
                condition
                & additional_condition
            )

        return condition

    # ============================================================
    # COMMON KEY CONDITION
    # ============================================================

    def _build_common_key_condition(
        self,
        key_columns
    ):
        """
        Build a condition that confirms a record exists
        on both sides of the full outer join.

        Null-safe equality is used so that NULL business keys
        are handled consistently.
        """

        conditions = []

        for column in key_columns:

            condition = (
                F.col(f"expected.{column}")
                .eqNullSafe(
                    F.col(f"actual.{column}")
                )
            )

            conditions.append(condition)

        if not conditions:
            return F.lit(True)

        condition = conditions[0]

        for additional_condition in conditions[1:]:

            condition = (
                condition
                & additional_condition
            )

        return condition

    # ============================================================
    # PRINT REPORT
    # ============================================================

    def print_report(self, report):

        print("\n")
        print("=" * 70)
        print("DATA RECONCILIATION REPORT")
        print("=" * 70)

        print(
            f"Schema Validation      : "
            f"{report.get('schema_validation', 'N/A')}"
        )

        print(
            f"Expected Row Count     : "
            f"{report.get('expected_row_count', 'N/A')}"
        )

        print(
            f"Actual Row Count       : "
            f"{report.get('actual_row_count', 'N/A')}"
        )

        print(
            f"Row Count Validation   : "
            f"{report.get('row_count_validation', 'N/A')}"
        )

        print(
            f"Data Validation        : "
            f"{report.get('data_validation', 'N/A')}"
        )

        if "mismatched_rows" in report:

            print(
                f"Mismatched Rows        : "
                f"{report.get('mismatched_rows', 0)}"
            )

            print(
                f"Missing Rows           : "
                f"{report.get('missing_rows', 0)}"
            )

            print(
                f"Extra Rows             : "
                f"{report.get('extra_rows', 0)}"
            )

            print(
                f"Duplicate Expected Keys: "
                f"{report.get('duplicate_expected_keys', 0)}"
            )

            print(
                f"Duplicate Actual Keys  : "
                f"{report.get('duplicate_actual_keys', 0)}"
            )

        print("-" * 70)

        print(
            f"OVERALL STATUS         : "
            f"{report.get('overall_status', 'N/A')}"
        )

        warnings = report.get("warnings", [])

        if warnings:

            print("\nWARNINGS:")

            for warning in warnings:
                print(f"- {warning}")

        errors = report.get("errors", [])

        if errors:

            print("\nERRORS:")

            for error in errors:
                print(f"- {error}")

        print("=" * 70)