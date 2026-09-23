import ast


class PySparkValidator:

    def __init__(
        self,
        ir,
        execution_plan,
        pyspark_code
    ):

        self.ir = ir
        self.execution_plan = execution_plan
        self.code = pyspark_code

        self.errors = []
        self.warnings = []
        self.results = []

    # =========================================================
    # MAIN VALIDATION
    # =========================================================

    def validate(self):

        self._validate_python_syntax()

        # Do not continue semantic validation if Python
        # syntax itself is invalid.
        if self.errors:

            return {
                "valid": False,
                "errors": self.errors,
                "warnings": self.warnings,
                "results": self.results
            }

        self._validate_sources()

        self._validate_transformations()

        self._validate_targets()

        self._validate_transformation_order()

        self._validate_target_usage()

        self._validate_unsupported_transformations()

        return {
            "valid": len(self.errors) == 0,
            "errors": self.errors,
            "warnings": self.warnings,
            "results": self.results
        }

    # =========================================================
    # PYTHON SYNTAX
    # =========================================================

    def _validate_python_syntax(self):

        try:

            ast.parse(
                self.code
            )

            self.results.append({
                "check": "Python syntax",
                "status": "PASS"
            })

        except SyntaxError as exc:

            self.errors.append(
                f"Python syntax error: {exc}"
            )

            self.results.append({
                "check": "Python syntax",
                "status": "FAIL"
            })

    # =========================================================
    # SOURCES
    # =========================================================

    def _validate_sources(self):

        sources = self.ir.get(
            "sources",
            []
        )

        source_instances = self.ir.get(
            "source_instances",
            []
        )

        for source in sources:

            name = source.get(
                "name"
            )

            if not name:
                continue

            # -------------------------------------------------
            # Resolve expected source table using the same
            # logic as the generator.
            # -------------------------------------------------

            expected_table = (
                self._resolve_source_table(
                    source
                )
            )

            expected_pattern = (
                f'spark.table("{expected_table}")'
            )

            found = (
                expected_pattern in self.code
            )

            if found:

                self.results.append({
                    "check":
                        f"Source: {name}",
                    "status":
                        "PASS"
                })

            else:

                self.errors.append(
                    f"Source missing from PySpark: "
                    f"{name} "
                    f"(expected "
                    f"{expected_table})"
                )

                self.results.append({
                    "check":
                        f"Source: {name}",
                    "status":
                        "FAIL"
                })

        # -----------------------------------------------------
        # Validate source-instance resolution.
        # -----------------------------------------------------

        for source_instance in source_instances:

            instance_name = (
                source_instance.get(
                    "name"
                )
            )

            source_name = (
                source_instance.get(
                    "source"
                )
            )

            if not instance_name:
                continue

            if not source_name:

                self.errors.append(
                    f"Source instance "
                    f"'{instance_name}' "
                    f"has no source"
                )

                self.results.append({
                    "check":
                        f"Source instance: "
                        f"{instance_name}",
                    "status":
                        "FAIL"
                })

                continue

            dataframe_name = (
                self._safe_name(
                    source_name
                )
            )

            # The source dataframe should exist
            # in generated code.

            if dataframe_name not in self.code:

                self.errors.append(
                    f"Source instance "
                    f"'{instance_name}' "
                    f"does not resolve to "
                    f"'{source_name}'"
                )

                self.results.append({
                    "check":
                        f"Source instance: "
                        f"{instance_name}",
                    "status":
                        "FAIL"
                })

            else:

                self.results.append({
                    "check":
                        f"Source instance: "
                        f"{instance_name}",
                    "status":
                        "PASS"
                })

    # =========================================================
    # TRANSFORMATIONS
    # =========================================================

    def _validate_transformations(self):

        transformations = self.ir.get(
            "transformations",
            []
        )

        for transformation in transformations:

            name = transformation.get(
                "name"
            )

            transformation_type = (
                transformation.get(
                    "type",
                    ""
                ).upper()
            )

            if not name:
                continue

            dataframe_name = (
                self._safe_name(
                    name
                )
            )

            # -------------------------------------------------
            # A generated transformation normally creates a
            # dataframe with the transformation name.
            #
            # The name can also appear in a generated comment.
            # -------------------------------------------------

            found = (
                dataframe_name in self.code
            )

            if found:

                self.results.append({
                    "check":
                        f"{transformation_type}: "
                        f"{name}",
                    "status":
                        "PASS"
                })

            else:

                self.errors.append(
                    "Transformation missing "
                    f"from PySpark: {name} "
                    f"({transformation_type})"
                )

                self.results.append({
                    "check":
                        f"{transformation_type}: "
                        f"{name}",
                    "status":
                        "FAIL"
                })

    # =========================================================
    # TARGETS
    # =========================================================

    def _validate_targets(self):

        targets = self._get_targets()

        if not targets:

            self.errors.append(
                "No targets found in IR"
            )

            return

        for target in targets:

            name = target.get(
                "name"
            )

            if not name:
                continue

            target_table = (
                self._resolve_target_table(
                    target
                )
            )

            # -------------------------------------------------
            # Target table should be present in saveAsTable.
            # -------------------------------------------------

            expected_write = (
                f'saveAsTable("{target_table}")'
            )

            found = (
                expected_write in self.code
            )

            if found:

                self.results.append({
                    "check":
                        f"Target: {name}",
                    "status":
                        "PASS"
                })

            else:

                self.errors.append(
                    f"Target missing from "
                    f"PySpark: {name} "
                    f"(expected "
                    f"{target_table})"
                )

                self.results.append({
                    "check":
                        f"Target: {name}",
                    "status":
                        "FAIL"
                })

            # -------------------------------------------------
            # Target instance
            # -------------------------------------------------

            instance_name = (
                target.get("instance_name")
                or target.get("target_instance")
                or target.get("name")
            )

            if not instance_name:

                self.errors.append(
                    f"Target '{name}' "
                    f"has no instance_name"
                )

                self.results.append({
                    "check":
                        f"Target instance: {name}",
                    "status":
                        "FAIL"
                })

            else:

                # Target instance should participate
                # in target mapping logic.
                #
                # It doesn't necessarily have to appear
                # literally in generated code, because the
                # generator uses it to resolve target links.

                target_links = [
                    link
                    for link in self.ir.get(
                        "links",
                        []
                    )
                    if link.get("to")
                    == instance_name
                ]

                if not target_links:

                    self.warnings.append(
                        f"Target instance "
                        f"'{instance_name}' "
                        f"has no incoming links"
                    )

                    self.results.append({
                        "check":
                            f"Target instance: "
                            f"{instance_name}",
                        "status":
                            "WARN"
                    })

                else:

                    self.results.append({
                        "check":
                            f"Target instance: "
                            f"{instance_name}",
                        "status":
                            "PASS"
                    })

    # =========================================================
    # EXECUTION ORDER
    # =========================================================

    def _validate_transformation_order(self):

        steps = self.execution_plan.get(
            "steps",
            []
        )

        # -----------------------------------------------------
        # If execution_plan is represented directly
        # as a list, support that.
        # -----------------------------------------------------

        if not steps:

            if isinstance(
                self.execution_plan,
                list
            ):

                steps = self.execution_plan

            elif self.ir.get(
                "execution_order"
            ):

                steps = [

                    {
                        "name": name
                    }

                    for name in self.ir.get(
                        "execution_order",
                        []
                    )
                ]

        if not steps:

            self.warnings.append(
                "No execution plan available "
                "for PySpark order validation"
            )

            self.results.append({
                "check":
                    "Execution order",
                "status":
                    "WARN"
            })

            return

        positions = {}

        # -----------------------------------------------------
        # Find where each generated dataframe is first created.
        # -----------------------------------------------------

        for step in steps:

            name = step.get(
                "name"
            )

            if not name:
                continue

            dataframe_name = (
                self._safe_name(
                    name
                )
            )

            position = self._find_dataframe_assignment(
                dataframe_name
            )

            if position >= 0:

                positions[name] = position

        # -----------------------------------------------------
        # Validate order.
        # -----------------------------------------------------

        order_valid = True

        previous_position = -1

        for step in steps:

            name = step.get(
                "name"
            )

            if name not in positions:
                continue

            current_position = positions[
                name
            ]

            if (
                current_position
                < previous_position
            ):

                order_valid = False

                self.errors.append(
                    f"Generated PySpark "
                    f"execution order is incorrect "
                    f"around: {name}"
                )

            previous_position = (
                current_position
            )

        self.results.append({
            "check":
                "Execution order",
            "status":
                "PASS"
                if order_valid
                else "FAIL"
        })

    # =========================================================
    # TARGET USAGE
    # =========================================================

    def _validate_target_usage(self):

        targets = self._get_targets()

        for target in targets:

            target_name = target.get(
                "name"
            )

            if not target_name:
                continue

            target_fields = []

            # -------------------------------------------------
            # Support all target field representations:
            #   columns: [...]
            #   fields: [...]
            #   ports: [...]
            # -------------------------------------------------

            for field_key in (
                "columns",
                "fields",
                "ports"
            ):

                for field in target.get(
                    field_key,
                    []
                ):

                    if isinstance(field, dict):

                        field_name = (
                            field.get("name")
                            or field.get("port_name")
                        )

                        if field_name:
                            target_fields.append(
                                field_name
                            )

                    elif isinstance(field, str):

                        target_fields.append(
                            field
                        )

            # -------------------------------------------------
            # Some IR versions represent target fields as ports.
            # Support that shape as well.
            # -------------------------------------------------

            for field in target.get(
                "ports",
                []
            ):

                if isinstance(field, dict):

                    field_name = field.get(
                        "name"
                    )

                    if field_name:
                        target_fields.append(
                            field_name
                        )

            target_fields = list(
                dict.fromkeys(
                    target_fields
                )
            )

            missing_fields = []

            # -------------------------------------------------
            # Check that target columns appear in the
            # generated final mapping.
            # -------------------------------------------------

            for field in target_fields:

                if field not in self.code:

                    missing_fields.append(
                        field
                    )

            if missing_fields:

                self.errors.append(
                    f"Target '{target_name}' "
                    f"has missing generated "
                    f"fields: "
                    f"{missing_fields}"
                )

                self.results.append({
                    "check":
                        f"Target fields: "
                        f"{target_name}",
                    "status":
                        "FAIL"
                })

            else:

                self.results.append({
                    "check":
                        f"Target fields: "
                        f"{target_name}",
                    "status":
                        "PASS"
                })

    # =========================================================
    # UNSUPPORTED TRANSFORMATIONS
    # =========================================================

    def _validate_unsupported_transformations(
        self
    ):

        supported_transformations = {
            "SOURCE_QUALIFIER",
            "EXPRESSION",
            "FILTER",
            "LOOKUP",
            "JOINER",
            "AGGREGATOR"
        }

        transformations = self.ir.get(
            "transformations",
            []
        )

        unsupported = []

        for transformation in transformations:

            transformation_type = (
                transformation.get(
                    "type",
                    ""
                ).upper()
            )

            if transformation_type not in (
                supported_transformations
            ):

                unsupported.append(
                    transformation_type
                )

        unsupported = list(
            dict.fromkeys(
                unsupported
            )
        )

        if unsupported:

            for transformation_type in unsupported:

                self.errors.append(
                    "Unsupported transformation "
                    f"type in generated mapping: "
                    f"{transformation_type}"
                )

                self.results.append({
                    "check":
                        f"Unsupported transformation: "
                        f"{transformation_type}",
                    "status":
                        "FAIL"
                })

    # =========================================================
    # TARGET NORMALIZATION / BACKWARD COMPATIBILITY
    # =========================================================

    def _get_targets(self):
        """
        Return targets consistently as a list of dictionaries.

        Supports:
          - target: { ... }
          - target: [ { ... } ]
          - targets: [ { ... } ]
          - targets: { ... }

        This keeps older and newer IR shapes compatible.
        """

        targets = self.ir.get("target")

        if targets is None:
            targets = self.ir.get("targets", [])

        if isinstance(targets, dict):
            return [targets]

        if isinstance(targets, list):
            return [
                target
                for target in targets
                if isinstance(target, dict)
            ]

        return []

    # =========================================================
    # SOURCE TABLE RESOLUTION
    # =========================================================

    def _resolve_source_table(
        self,
        source
    ):

        source_name = source.get(
            "name"
        )

        if not source_name:

            return None

        # -----------------------------------------------------
        # Resolve the Databricks landing/bronze location.
        #
        # IMPORTANT:
        # The IDMC source connection can contain the original
        # source-system schema, for example:
        #
        #   SRC_SALES_DB      -> public
        #   SRC_REFERENCE_DB  -> master
        #
        # Those values describe the original source system.
        # They are NOT the Databricks Bronze schema used by
        # this migration POC.
        #
        # The validator must use exactly the same resolution
        # rule as PySparkGenerator:
        #
        #   landing_catalog -> target_catalog -> idmc_poc
        #   landing_schema  -> bronze_schema -> bronze
        #
        # This keeps validation aligned with generated PySpark.
        # -----------------------------------------------------

        catalog = (
            source.get(
                "landing_catalog"
            )
            or source.get(
                "target_catalog"
            )
            or "idmc_poc"
        )

        schema = (
            source.get(
                "landing_schema"
            )
            or source.get(
                "bronze_schema"
            )
            or "bronze"
        )

        return (
            f"{catalog}."
            f"{schema}."
            f"{source_name}"
        )

    # =========================================================
    # TARGET TABLE RESOLUTION
    # =========================================================

    def _resolve_target_table(
        self,
        target
    ):

        if not isinstance(target, dict):
            return None

        target_name = (
            target.get("target")
            or target.get("table")
            or target.get("target_name")
            or target.get("name")
        )

        if not target_name:
            return None

        catalog = target.get("catalog")
        schema = target.get("schema")

        connection = target.get("connection")

        if isinstance(connection, dict):

            properties = connection.get(
                "properties",
                {}
            )

            if isinstance(properties, dict):

                if not catalog:
                    catalog = properties.get("catalog")

                if not schema:
                    schema = properties.get("schema")

        elif isinstance(connection, str):

            connection = connection.strip()

        # Keep this exactly aligned with the current
        # PySparkGenerator target-resolution defaults.
        if not catalog:
            catalog = "idmc_poc"

        if not schema:
            schema = "silver"

        return (
            f"{catalog}."
            f"{schema}."
            f"{target_name}"
        )

    # =========================================================
    # FIND DATAFRAME ASSIGNMENT
    # =========================================================

    def _find_dataframe_assignment(
        self,
        dataframe_name
    ):

        assignment_patterns = [

            f"{dataframe_name} =",

            f"{dataframe_name}="

        ]

        positions = []

        for pattern in assignment_patterns:

            position = self.code.find(
                pattern
            )

            if position >= 0:

                positions.append(
                    position
                )

        if not positions:

            return -1

        return min(
            positions
        )

    # =========================================================
    # SAFE PYTHON NAME
    # =========================================================

    def _safe_name(
        self,
        name
    ):

        if not name:

            return "df"

        return (
            str(name)
            .lower()
            .replace(
                "-",
                "_"
            )
            .replace(
                " ",
                "_"
            )
        )