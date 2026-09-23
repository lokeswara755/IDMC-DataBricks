from pyspark.sql import functions as F


class PySparkGenerator:

    def generate(self, ir):

        code = []

        # =====================================================
        # HEADER
        # =====================================================

        code.append(
            "from pyspark.sql import functions as F"
        )

        code.append("")

        code.append(
            f"# Generated PySpark for mapping: "
            f"{ir.get('name')}"
        )

        if ir.get("description"):

            code.append(
                f"# {ir.get('description')}"
            )

        code.append("")

        # =====================================================
        # SOURCE DATAFRAMES
        # =====================================================

        code.append(
            "# ====================================================="
        )

        code.append(
            "# SOURCE DATAFRAMES"
        )

        code.append(
            "# ====================================================="
        )

        code.append("")

        for source in ir.get(
            "sources",
            []
        ):

            source_name = source.get(
                "name"
            )

            if not source_name:
                continue

            dataframe_name = self._safe_name(
                source_name
            )

            source_table = (
                self._resolve_source_table(
                    source
                )
            )

            code.append(
                f'{dataframe_name} = '
                f'spark.table("{source_table}")'
            )

        code.append("")

        # =====================================================
        # TRANSFORMATIONS
        # =====================================================

        code.append(
            "# ====================================================="
        )

        code.append(
            "# TRANSFORMATIONS"
        )

        code.append(
            "# ====================================================="
        )

        code.append("")

        transformation_map = {
            transformation.get("name"): transformation
            for transformation in ir.get(
                "transformations",
                []
            )
            if transformation.get("name")
        }

        targets = self._get_targets(ir)

        target_instance_names = {
            target.get("instance_name") or target.get("name")
            for target in targets
            if isinstance(target, dict)
            and (target.get("instance_name") or target.get("name"))
        }

        for transformation_name in ir.get(
            "execution_order",
            []
        ):

            # -------------------------------------------------
            # Target instance is handled separately
            # -------------------------------------------------

            if transformation_name in (
                target_instance_names
            ):
                continue

            transformation = transformation_map.get(
                transformation_name
            )

            if transformation is None:
                continue

            transformation_type = (
                transformation.get("type")
            )

            # -------------------------------------------------
            # SOURCE QUALIFIER
            # -------------------------------------------------

            if transformation_type == (
                "SOURCE_QUALIFIER"
            ):

                self._generate_source_qualifier(
                    code,
                    transformation,
                    ir
                )

            # -------------------------------------------------
            # EXPRESSION
            # -------------------------------------------------

            elif transformation_type == (
                "EXPRESSION"
            ):

                self._generate_expression(
                    code,
                    transformation,
                    ir
                )

            # -------------------------------------------------
            # FILTER
            # -------------------------------------------------

            elif transformation_type == (
                "FILTER"
            ):

                self._generate_filter(
                    code,
                    transformation,
                    ir
                )

            # -------------------------------------------------
            # LOOKUP
            # -------------------------------------------------

            elif transformation_type == (
                "LOOKUP"
            ):

                self._generate_lookup(
                    code,
                    transformation,
                    ir
                )

            # -------------------------------------------------
            # JOINER
            # -------------------------------------------------

            elif transformation_type == (
                "JOINER"
            ):

                self._generate_joiner(
                    code,
                    transformation,
                    ir
                )

            # -------------------------------------------------
            # AGGREGATOR
            # -------------------------------------------------

            elif transformation_type == (
                "AGGREGATOR"
            ):

                self._generate_aggregator(
                    code,
                    transformation,
                    ir
                )

            # -------------------------------------------------
            # Unsupported transformation
            # -------------------------------------------------

            else:

                raise ValueError(
                    "Unsupported transformation type: "
                    f"{transformation_type} "
                    f"in transformation "
                    f"'{transformation_name}'"
                )

            code.append("")

        # =====================================================
        # TARGET MAPPING
        # =====================================================

        code.append(
            "# ====================================================="
        )

        code.append(
            "# TARGET MAPPING"
        )

        code.append(
            "# ====================================================="
        )

        code.append("")

        self._generate_target_mapping(
            code,
            ir
        )

        code.append("")

        # =====================================================
        # TARGET WRITE
        # =====================================================

        code.append(
            "# ====================================================="
        )

        code.append(
            "# TARGET WRITE"
        )

        code.append(
            "# ====================================================="
        )

        code.append("")

        targets = self._get_targets(ir)

        if not targets:

            raise ValueError(
                "No target found in IR"
            )

        for target in targets:

            target_table = (
                self._resolve_target_table(
                    target
                )
            )

            code.append(
                f'final_df.write.mode("overwrite")'
                f'.saveAsTable("{target_table}")'
            )

        code.append("")

        return "\n".join(code)

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

            raise ValueError(
                "Source name is missing"
            )

        connection = source.get(
            "connection"
        )

        catalog = None
        schema = None

        if isinstance(connection, dict):

            properties = connection.get(
                "properties",
                {}
            )

            if isinstance(properties, dict):
                catalog = properties.get(
                    "catalog"
                )

                schema = properties.get(
                    "schema"
                )

        elif isinstance(connection, str):
            # The normalized IR may store the connection as a
            # connection name such as SRC_SALES_DB. The POC
            # source data is already landed in the Bronze layer,
            # so the physical source connection is not required
            # to resolve the Databricks Bronze table.
            connection = connection.strip()

        # -----------------------------------------------------
        # POC default
        # -----------------------------------------------------

        if not catalog:
            catalog = "idmc_poc"

        if not schema:
            schema = "bronze"

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

        if not target:
            raise ValueError(
                "Target metadata is missing"
            )

        target_name = (
            target.get("target")
            or target.get("name")
        )

        if not target_name:
            raise ValueError(
                "Target name is missing"
            )

        # Prefer catalog/schema directly defined on target.
        catalog = target.get(
            "catalog"
        )

        schema = target.get(
            "schema"
        )

        # Fall back to target connection properties.
        connection = target.get(
            "connection"
        )

        if isinstance(connection, dict):

            properties = connection.get(
                "properties",
                {}
            )

            if isinstance(properties, dict):
                if not catalog:
                    catalog = properties.get(
                        "catalog"
                    )

                if not schema:
                    schema = properties.get(
                        "schema"
                    )

        elif isinstance(connection, str):
            # Target connection names are metadata only in this POC.
            # The physical target catalog/schema come from target
            # metadata or the POC defaults below.
            connection = connection.strip()

        # POC defaults.
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
    # SOURCE QUALIFIER
    # =========================================================

    def _generate_source_qualifier(
        self,
        code,
        transformation,
        ir
    ):

        name = transformation.get(
            "name"
        )

        if not name:
            return

        config = transformation.get(
            "config",
            {}
        )

        input_source = config.get(
            "input_source"
        )

        source_name = None

        # -----------------------------------------------------
        # First try input_source directly
        # -----------------------------------------------------

        if input_source:

            source_instance = (
                self._resolve_source_instance(
                    input_source,
                    ir
                )
            )

            if source_instance:

                source_name = source_instance.get(
                    "source"
                )

        # -----------------------------------------------------
        # If direct resolution fails, resolve from
        # SOURCE_QUALIFIER name.
        #
        # Example:
        #
        # SQ_CUSTOMER -> CUSTOMER
        # SQ_ORDERS   -> ORDERS
        # -----------------------------------------------------

        if not source_name:

            source_name = (
                self._resolve_source_for_qualifier(
                    name,
                    ir
                )
            )

        if not source_name:

            raise ValueError(
                f"Unable to resolve source for "
                f"SOURCE_QUALIFIER '{name}'"
            )

        source_dataframe = self._safe_name(
            source_name
        )

        output_dataframe = self._safe_name(
            name
        )

        code.append(
            f"# SOURCE_QUALIFIER: {name}"
        )

        code.append(
            f"{output_dataframe} = "
            f"{source_dataframe}"
        )

    # =========================================================
    # RESOLVE SOURCE FOR SOURCE QUALIFIER
    # =========================================================

    def _resolve_source_for_qualifier(
        self,
        qualifier_name,
        ir
    ):

        if not qualifier_name:
            return None

        # -----------------------------------------------------
        # Normalize qualifier
        #
        # SQ_CUSTOMER -> customer
        # -----------------------------------------------------

        qualifier_base = str(
            qualifier_name
        ).strip()

        if qualifier_base.upper().startswith(
            "SQ_"
        ):

            qualifier_base = (
                qualifier_base[3:]
            )

        qualifier_base = (
            qualifier_base
            .strip()
            .lower()
        )

        # =====================================================
        # 1. Try source_instances
        # =====================================================

        source_instances = ir.get(
            "source_instances",
            []
        )

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

            if not instance_name or not source_name:
                continue

            instance_base = str(
                instance_name
            ).strip()

            if instance_base.upper().startswith(
                "SRC_"
            ):

                instance_base = (
                    instance_base[4:]
                )

            instance_base = (
                instance_base
                .strip()
                .lower()
            )

            if instance_base == qualifier_base:

                return source_name

        # =====================================================
        # 2. Match SOURCE_QUALIFIER to actual source name
        #
        # SQ_CUSTOMER -> CUSTOMER
        # SQ_ORDERS   -> ORDERS
        # =====================================================

        sources = ir.get(
            "sources",
            []
        )

        for source in sources:

            source_name = source.get(
                "name"
            )

            if not source_name:
                continue

            normalized_source_name = (
                str(source_name)
                .strip()
                .lower()
            )

            if normalized_source_name == qualifier_base:

                return source_name

        # =====================================================
        # 3. Try source instance names
        #
        # SRC_CUSTOMER -> CUSTOMER
        # =====================================================

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

            if not instance_name or not source_name:
                continue

            normalized_instance = (
                str(instance_name)
                .strip()
                .lower()
            )

            if normalized_instance.startswith(
                "src_"
            ):

                normalized_instance = (
                    normalized_instance[4:]
                )

            if normalized_instance == qualifier_base:

                return source_name

        return None

    # =========================================================
    # RESOLVE SOURCE INSTANCE
    # =========================================================

    def _resolve_source_instance(
        self,
        source_instance_name,
        ir
    ):

        if not source_instance_name:
            return None

        # -----------------------------------------------------
        # Mapping-level source instances
        # -----------------------------------------------------

        for source_instance in ir.get(
            "source_instances",
            []
        ):

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

            if instance_name == (
                source_instance_name
            ):

                return {
                    "instance": instance_name,
                    "source": source_name
                }

        # -----------------------------------------------------
        # Source name directly
        # -----------------------------------------------------

        for source in ir.get(
            "sources",
            []
        ):

            source_name = source.get(
                "name"
            )

            if source_name == source_instance_name:

                return {
                    "instance": source_name,
                    "source": source_name
                }

        return None

    # =========================================================
    # EXPRESSION
    # =========================================================

    def _generate_expression(
        self,
        code,
        transformation,
        ir
    ):

        name = transformation.get(
            "name"
        )

        output_dataframe = self._safe_name(
            name
        )

        input_dataframe = (
            self._find_input_dataframe(
                name,
                ir
            )
        )

        code.append(
            f"# EXPRESSION: {name}"
        )

        code.append(
            f"{output_dataframe} = "
            f"{input_dataframe}"
        )

        for port in transformation.get(
            "ports",
            []
        ):

            expression = port.get(
                "expression"
            )

            if not expression:
                continue

            column_name = port.get(
                "name"
            )

            if not column_name:
                continue

            pyspark_expression = (
                self._convert_expression(
                    expression
                )
            )

            code.append(
                f"{output_dataframe} = "
                f"{output_dataframe}.withColumn("
            )

            code.append(
                f'    "{column_name}",'
            )

            code.append(
                f"    {pyspark_expression}"
            )

            code.append(
                ")"
            )

    # =========================================================
    # FILTER
    # =========================================================

    def _generate_filter(
        self,
        code,
        transformation,
        ir
    ):

        name = transformation.get(
            "name"
        )

        output_dataframe = self._safe_name(
            name
        )

        input_dataframe = (
            self._find_input_dataframe(
                name,
                ir
            )
        )

        config = transformation.get(
            "config",
            {}
        )

        condition = config.get(
            "condition"
        )

        if not condition:

            raise ValueError(
                f"FILTER '{name}' "
                f"has no condition"
            )

        pyspark_condition = (
            self._convert_condition(
                condition
            )
        )

        code.append(
            f"# FILTER: {name}"
        )

        code.append(
            f"{output_dataframe} = "
            f"{input_dataframe}.filter("
        )

        code.append(
            f"    {pyspark_condition}"
        )

        code.append(
            ")"
        )

    # =========================================================
    # LOOKUP
    # =========================================================

    def _generate_lookup(
        self,
        code,
        transformation,
        ir
    ):

        name = transformation.get(
            "name"
        )

        output_dataframe = self._safe_name(
            name
        )

        input_dataframe = (
            self._find_input_dataframe(
                name,
                ir
            )
        )

        config = transformation.get(
            "config",
            {}
        )

        lookup_source = config.get(
            "lookup_source"
        )

        lookup_condition = config.get(
            "lookup_condition"
        )

        if not lookup_source:

            raise ValueError(
                f"LOOKUP '{name}' "
                f"has no lookup_source"
            )

        lookup_dataframe = (
            self._resolve_lookup_dataframe(
                lookup_source,
                ir
            )
        )

        if not lookup_condition:

            raise ValueError(
                f"LOOKUP '{name}' "
                f"has no lookup_condition"
            )

        join_expression = (
            self._convert_join_condition(
                lookup_condition,
                left_alias="left",
                right_alias="right"
            )
        )

        code.append(
            f"# LOOKUP: {name}"
        )

        code.append(
            f"{output_dataframe} = "
            f"{input_dataframe}.alias(\"left\").join("
        )

        code.append(
            f"    {lookup_dataframe}.alias(\"right\"),"
        )

        code.append(
            f"    {join_expression},"
        )

        lookup_type = config.get(
            "lookup_type",
            "LEFT"
        )

        code.append(
            f'    "{self._normalize_join_type(lookup_type)}"'
        )

        code.append(
            ")"
        )

    # =========================================================
    # JOINER
    # =========================================================

    def _generate_joiner(
        self,
        code,
        transformation,
        ir
    ):

        name = transformation.get(
            "name"
        )

        output_dataframe = self._safe_name(
            name
        )

        config = transformation.get(
            "config",
            {}
        )

        join_type = self._normalize_join_type(
            config.get(
                "join_type",
                "INNER"
            )
        )

        join_condition = config.get(
            "join_condition"
        )

        inputs = (
            self._find_input_transformations(
                name,
                ir
            )
        )

        if len(inputs) < 2:

            raise ValueError(
                f"JOINER '{name}' "
                f"requires at least two "
                f"input transformations"
            )

        left_dataframe = self._safe_name(
            inputs[0]
        )

        right_dataframe = self._safe_name(
            inputs[1]
        )

        if not join_condition:

            raise ValueError(
                f"JOINER '{name}' "
                f"has no join condition"
            )

        join_expression = (
            self._convert_join_condition(
                join_condition,
                left_alias="left",
                right_alias="right"
            )
        )

        code.append(
            f"# JOINER: {name}"
        )

        code.append(
            f"{output_dataframe} = "
            f"{left_dataframe}.alias(\"left\").join("
        )

        code.append(
            f"    {right_dataframe}.alias(\"right\"),"
        )

        code.append(
            f"    {join_expression},"
        )

        code.append(
            f'    "{join_type}"'
        )

        code.append(
            ")"
        )

        # -----------------------------------------------------
        # Select output columns from actual graph links
        # -----------------------------------------------------

        output_links = [
            link
            for link in ir.get(
                "links",
                []
            )
            if link.get(
                "from"
            ) == name
        ]

        selected_columns = []

        for link in output_links:

            from_port = link.get(
                "from_port"
            )

            to_port = link.get(
                "to_port"
            )

            if not from_port:
                continue

            source_side = (
                self._resolve_join_output_side(
                    from_port,
                    inputs,
                    ir
                )
            )

            if source_side == "left":

                expression = (
                    f'F.col("left.{from_port}")'
                )

            elif source_side == "right":

                expression = (
                    f'F.col("right.{from_port}")'
                )

            else:

                expression = (
                    f'F.col("{from_port}")'
                )

            if (
                to_port
                and
                to_port != from_port
            ):

                expression += (
                    f'.alias("{to_port}")'
                )

            selected_columns.append(
                expression
            )

        if selected_columns:

            code.append(
                f"{output_dataframe} = "
                f"{output_dataframe}.select("
            )

            for index, expression in enumerate(
                selected_columns
            ):

                comma = (
                    ","
                    if index <
                    len(selected_columns) - 1
                    else ""
                )

                code.append(
                    f"    {expression}{comma}"
                )

            code.append(
                ")"
            )

    # =========================================================
    # AGGREGATOR
    # =========================================================

    def _generate_aggregator(
        self,
        code,
        transformation,
        ir
    ):

        name = transformation.get(
            "name"
        )

        output_dataframe = self._safe_name(
            name
        )

        input_transformations = (
            self._find_input_transformations(
                name,
                ir
            )
        )

        if not input_transformations:

            raise ValueError(
                f"AGGREGATOR '{name}' "
                f"has no input transformation"
            )

        config = transformation.get(
            "config",
            {}
        )

        group_by = config.get(
            "group_by"
        )

        group_columns = (
            self._parse_group_by(
                group_by
            )
        )

        if not group_columns:

            raise ValueError(
                f"AGGREGATOR '{name}' "
                f"has no group_by columns"
            )

        input_dataframe = self._safe_name(
            input_transformations[0]
        )

        code.append(
            f"# AGGREGATOR: {name}"
        )

        code.append(
            f"{output_dataframe} = "
            f"{input_dataframe}.groupBy("
        )

        for index, column in enumerate(
            group_columns
        ):

            comma = (
                ","
                if index <
                len(group_columns) - 1
                else ""
            )

            code.append(
                f'    "{column}"{comma}'
            )

        code.append(
            ").agg("
        )

        aggregation_ports = [
            port
            for port in transformation.get(
                "ports",
                []
            )
            if port.get(
                "expression"
            )
        ]

        if not aggregation_ports:

            raise ValueError(
                f"AGGREGATOR '{name}' "
                f"has no aggregate expressions"
            )

        for index, port in enumerate(
            aggregation_ports
        ):

            expression = port.get(
                "expression"
            )

            alias = port.get(
                "name"
            )

            aggregation = (
                self._convert_aggregation(
                    expression,
                    alias
                )
            )

            comma = (
                ","
                if index <
                len(aggregation_ports) - 1
                else ""
            )

            code.append(
                f"    {aggregation}{comma}"
            )

        code.append(
            ")"
        )

    # =========================================================
    # TARGET MAPPING
    # =========================================================

    def _generate_target_mapping(
        self,
        code,
        ir
    ):

        target_instance = (
            self._get_target_instance(
                ir
            )
        )

        if not target_instance:

            raise ValueError(
                "Unable to resolve target instance"
            )

        final_transformation = (
            self._find_final_transformation(
                ir
            )
        )

        if not final_transformation:

            raise ValueError(
                "Unable to determine final "
                "transformation"
            )

        code.append(
            "# Final target column mapping"
        )

        code.append(
            "final_df = "
            f"{final_transformation}.select("
        )

        links = ir.get(
            "links",
            []
        )

        target_links = [
            link
            for link in links
            if link.get(
                "to"
            ) == target_instance.get(
                "name"
            )
        ]

        if not target_links:

            raise ValueError(
                "No links found from final "
                "transformation to target"
            )

        for index, link in enumerate(
            target_links
        ):

            source_column = link.get(
                "from_port"
            )

            target_column = link.get(
                "to_port"
            )

            if not source_column:
                continue

            if not target_column:
                target_column = source_column

            comma = (
                ","
                if index <
                len(target_links) - 1
                else ""
            )

            code.append(
                f'    F.col("{source_column}")'
                f'.alias("{target_column}")'
                f"{comma}"
            )

        code.append(
            ")"
        )

    # =========================================================
    # FIND INPUT DATAFRAME
    # =========================================================

    def _find_input_dataframe(
        self,
        transformation_name,
        ir
    ):

        inputs = (
            self._find_input_transformations(
                transformation_name,
                ir
            )
        )

        if not inputs:
            return "df"

        return self._safe_name(
            inputs[0]
        )

    # =========================================================
    # FIND INPUT TRANSFORMATIONS
    # =========================================================

    def _find_input_transformations(
        self,
        transformation_name,
        ir
    ):

        inputs = []

        transformation_names = {
            transformation.get("name")
            for transformation in ir.get(
                "transformations",
                []
            )
            if transformation.get("name")
        }

        for link in ir.get(
            "links",
            []
        ):

            if link.get(
                "to"
            ) != transformation_name:

                continue

            source = link.get(
                "from"
            )

            if source not in transformation_names:
                continue

            if source not in inputs:

                inputs.append(
                    source
                )

        return inputs

    # =========================================================
    # RESOLVE LOOKUP DATAFRAME
    # =========================================================

    def _resolve_lookup_dataframe(
        self,
        lookup_source,
        ir
    ):

        # -----------------------------------------------------
        # Source instance
        # -----------------------------------------------------

        source_instance = (
            self._resolve_source_instance(
                lookup_source,
                ir
            )
        )

        if source_instance:

            source_name = source_instance.get(
                "source"
            )

            return self._safe_name(
                source_name
            )

        # -----------------------------------------------------
        # Direct source
        # -----------------------------------------------------

        for source in ir.get(
            "sources",
            []
        ):

            if source.get(
                "name"
            ) == lookup_source:

                return self._safe_name(
                    lookup_source
                )

        # -----------------------------------------------------
        # Transformation
        # -----------------------------------------------------

        for transformation in ir.get(
            "transformations",
            []
        ):

            if transformation.get(
                "name"
            ) == lookup_source:

                return self._safe_name(
                    lookup_source
                )

        raise ValueError(
            f"Unable to resolve lookup source: "
            f"{lookup_source}"
        )

    # =========================================================
    # TARGET INSTANCE
    # =========================================================

    def _get_targets(
        self,
        ir
    ):
        """
        Normalize the IR target representation.

        The current IR contains a single target dictionary.
        Older IR versions may contain a list of target dictionaries.
        """

        targets = ir.get(
            "target",
            []
        )

        if isinstance(targets, dict):
            return [targets]

        if targets is None:
            return []

        if isinstance(targets, list):
            return targets

        raise ValueError(
            "Invalid target representation in IR"
        )

    # =========================================================
    # TARGET INSTANCE
    # =========================================================

    def _get_target_instance(
        self,
        ir
    ):

        targets = self._get_targets(ir)

        if not targets:
            return None

        target = targets[0]

        instance_name = (
            target.get("instance_name")
            or target.get("name")
        )

        if not instance_name:

            raise ValueError(
                f"Target '{target.get('name')}' "
                f"has no instance_name"
            )

        return {
            "name": instance_name,
            "target": (
                target.get("target")
                or target.get("name")
            )
        }

    # =========================================================
    # FINAL TRANSFORMATION
    # =========================================================

    def _find_final_transformation(
        self,
        ir
    ):

        target_instance = (
            self._get_target_instance(
                ir
            )
        )

        if not target_instance:
            return None

        target_name = target_instance.get(
            "name"
        )

        transformation_names = {
            transformation.get("name")
            for transformation in ir.get(
                "transformations",
                []
            )
            if transformation.get("name")
        }

        # -----------------------------------------------------
        # Prefer transformation directly feeding target
        # -----------------------------------------------------

        for link in ir.get(
            "links",
            []
        ):

            if link.get(
                "to"
            ) == target_name:

                source = link.get(
                    "from"
                )

                if source in transformation_names:

                    return self._safe_name(
                        source
                    )

        # -----------------------------------------------------
        # Fallback to execution order
        # -----------------------------------------------------

        for name in reversed(
            ir.get(
                "execution_order",
                []
            )
        ):

            if name in transformation_names:

                return self._safe_name(
                    name
                )

        return None

    # =========================================================
    # EXPRESSION CONVERSION
    # =========================================================

    def _convert_expression(
        self,
        expression
    ):

        if not expression:
            return "F.lit(None)"

        expression = expression.strip()

        upper_expression = (
            expression.upper()
        )

        # -----------------------------------------------------
        # UPPER(TRIM(column))
        # -----------------------------------------------------

        if upper_expression.startswith(
            "UPPER(TRIM("
        ):

            column = self._extract_nested_column(
                expression
            )

            return (
                f'F.upper(F.trim('
                f'F.col("{column}")'
                f'))'
            )

        # -----------------------------------------------------
        # TRIM(column)
        # -----------------------------------------------------

        if upper_expression.startswith(
            "TRIM("
        ):

            column = self._extract_column(
                expression
            )

            return (
                f'F.trim(F.col("{column}"))'
            )

        # -----------------------------------------------------
        # UPPER(column)
        # -----------------------------------------------------

        if upper_expression.startswith(
            "UPPER("
        ):

            column = self._extract_column(
                expression
            )

            return (
                f'F.upper(F.col("{column}"))'
            )

        # -----------------------------------------------------
        # LOWER(column)
        # -----------------------------------------------------

        if upper_expression.startswith(
            "LOWER("
        ):

            column = self._extract_column(
                expression
            )

            return (
                f'F.lower(F.col("{column}"))'
            )

        # -----------------------------------------------------
        # Generic Spark SQL fallback
        # -----------------------------------------------------

        escaped_expression = (
            expression.replace(
                '"',
                '\\"'
            )
        )

        return (
            f'F.expr("{escaped_expression}")'
        )

    # =========================================================
    # FILTER CONDITION
    # =========================================================

    def _convert_condition(
        self,
        condition
    ):

        if not condition:
            return "F.lit(True)"

        condition = (
            condition
            .replace("\n", " ")
            .strip()
        )

        escaped_condition = (
            condition.replace(
                '"',
                '\\"'
            )
        )

        return (
            f'F.expr("{escaped_condition}")'
        )

    # =========================================================
    # JOIN CONDITION
    # =========================================================

    def _convert_join_condition(
        self,
        condition,
        left_alias="left",
        right_alias="right"
    ):

        if not condition:

            raise ValueError(
                "Join condition is missing"
            )

        condition = (
            condition
            .replace("\n", " ")
            .strip()
        )

        # -----------------------------------------------------
        # Equality condition
        # -----------------------------------------------------

        if "=" in condition:

            parts = condition.split(
                "=",
                1
            )

            left_expression = (
                parts[0].strip()
            )

            right_expression = (
                parts[1].strip()
            )

            left_column = (
                self._extract_qualified_column(
                    left_expression
                )
            )

            right_column = (
                self._extract_qualified_column(
                    right_expression
                )
            )

            if left_column and right_column:

                return (
                    f'F.col("{left_alias}.'
                    f'{left_column}") == '
                    f'F.col("{right_alias}.'
                    f'{right_column}")'
                )

        # -----------------------------------------------------
        # Generic fallback
        # -----------------------------------------------------

        escaped_condition = (
            condition.replace(
                '"',
                '\\"'
            )
        )

        return (
            f'F.expr("{escaped_condition}")'
        )

    # =========================================================
    # JOIN TYPE
    # =========================================================

    def _normalize_join_type(
        self,
        join_type
    ):

        if not join_type:
            return "inner"

        normalized = (
            str(join_type)
            .strip()
            .upper()
        )

        # IDMC join semantics are not always the same as Spark join
        # type names. Keep native Spark values supported, while mapping
        # IDMC-specific values to valid Spark join types.
        mapping = {
            # Standard Spark / SQL style values
            "INNER": "inner",
            "LEFT": "left",
            "LEFT OUTER": "left",
            "RIGHT": "right",
            "RIGHT OUTER": "right",
            "FULL": "full",
            "FULL OUTER": "full",
            "OUTER": "full",
            "CROSS": "cross",

            # IDMC joiner semantics
            # CONNECTED means both pipelines participate in the join.
            # For a normal IDMC connected join, INNER is the closest
            # equivalent Spark join semantics.
            "CONNECTED": "inner",

            # Keep these IDMC-style values usable if they occur in
            # another mapping. They represent the detail/master side
            # relationship, which is commonly materialized as a LEFT join
            # when translating to Spark.
            "MASTER": "left",
            "DETAIL": "left",

            # Common aliases
            "NORMAL": "inner",
            "NORMAL JOIN": "inner"
        }

        return mapping.get(
            normalized,
            normalized.lower()
        )

    # =========================================================
    # AGGREGATION CONVERSION
    # =========================================================

    def _convert_aggregation(
        self,
        expression,
        alias
    ):

        if not expression:

            return (
                f'F.lit(None).alias("{alias}")'
            )

        expression = expression.strip()

        upper_expression = (
            expression.upper()
        )

        # -----------------------------------------------------
        # COUNT
        # -----------------------------------------------------

        if upper_expression.startswith(
            "COUNT("
        ):

            column = self._extract_column(
                expression
            )

            if column == "*":

                return (
                    f'F.count("*")'
                    f'.alias("{alias}")'
                )

            return (
                f'F.count("{column}")'
                f'.alias("{alias}")'
            )

        # -----------------------------------------------------
        # SUM
        # -----------------------------------------------------

        if upper_expression.startswith(
            "SUM("
        ):

            column = self._extract_column(
                expression
            )

            return (
                f'F.sum("{column}")'
                f'.alias("{alias}")'
            )

        # -----------------------------------------------------
        # MAX
        # -----------------------------------------------------

        if upper_expression.startswith(
            "MAX("
        ):

            column = self._extract_column(
                expression
            )

            return (
                f'F.max("{column}")'
                f'.alias("{alias}")'
            )

        # -----------------------------------------------------
        # MIN
        # -----------------------------------------------------

        if upper_expression.startswith(
            "MIN("
        ):

            column = self._extract_column(
                expression
            )

            return (
                f'F.min("{column}")'
                f'.alias("{alias}")'
            )

        # -----------------------------------------------------
        # AVG
        # -----------------------------------------------------

        if upper_expression.startswith(
            "AVG("
        ):

            column = self._extract_column(
                expression
            )

            return (
                f'F.avg("{column}")'
                f'.alias("{alias}")'
            )

        # -----------------------------------------------------
        # Generic fallback
        # -----------------------------------------------------

        escaped_expression = (
            expression.replace(
                '"',
                '\\"'
            )
        )

        return (
            f'F.expr("{escaped_expression}")'
            f'.alias("{alias}")'
        )

    # =========================================================
    # GROUP BY
    # =========================================================

    def _parse_group_by(
        self,
        group_by
    ):

        if not group_by:
            return []

        if isinstance(
            group_by,
            list
        ):

            return [
                str(column).strip()
                for column in group_by
                if str(column).strip()
            ]

        return [
            column.strip()
            for column in str(
                group_by
            ).split(",")
            if column.strip()
        ]

    # =========================================================
    # EXTRACT COLUMN
    # =========================================================

    def _extract_column(
        self,
        expression
    ):

        start = expression.find(
            "("
        )

        end = expression.rfind(
            ")"
        )

        if start == -1 or end == -1:

            return expression.strip()

        return expression[
            start + 1:end
        ].strip()

    # =========================================================
    # EXTRACT NESTED COLUMN
    # =========================================================

    def _extract_nested_column(
        self,
        expression
    ):

        expression = expression.strip()

        start = expression.find(
            "("
        )

        end = expression.rfind(
            ")"
        )

        if start == -1 or end == -1:

            return expression

        inner = expression[
            start + 1:end
        ].strip()

        if "(" in inner:

            return self._extract_nested_column(
                inner
            )

        return inner

    # =========================================================
    # EXTRACT QUALIFIED COLUMN
    # =========================================================

    def _extract_qualified_column(
        self,
        expression
    ):

        expression = (
            expression
            .strip()
            .replace(
                "`",
                ""
            )
        )

        if "." in expression:

            return expression.split(
                ".",
                1
            )[1].strip()

        return expression

    # =========================================================
    # RESOLVE JOIN OUTPUT SIDE
    # =========================================================

    def _resolve_join_output_side(
        self,
        column_name,
        inputs,
        ir
    ):

        if not inputs:
            return None

        left_transformation = inputs[0]

        right_transformation = (
            inputs[1]
            if len(inputs) > 1
            else None
        )

        left_ports = (
            self._get_transformation_ports(
                left_transformation,
                ir
            )
        )

        right_ports = (
            self._get_transformation_ports(
                right_transformation,
                ir
            )
            if right_transformation
            else set()
        )

        if column_name in left_ports:
            return "left"

        if column_name in right_ports:
            return "right"

        return None

    # =========================================================
    # GET TRANSFORMATION PORTS
    # =========================================================

    def _get_transformation_ports(
        self,
        transformation_name,
        ir
    ):

        if not transformation_name:
            return set()

        for transformation in ir.get(
            "transformations",
            []
        ):

            if transformation.get(
                "name"
            ) != transformation_name:

                continue

            return {
                port.get("name")
                for port in transformation.get(
                    "ports",
                    []
                )
                if port.get("name")
            }

        return set()

    # =========================================================
    # SAFE PYTHON NAME
    # =========================================================

    def _safe_name(
        self,
        name
    ):

        if not name:
            return "df"

        safe_name = (
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

        return safe_name