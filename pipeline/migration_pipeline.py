from parser.xmlreader import XMLReader
from parser.repository_parser import RepositoryParser
from parser.graph_builder import GraphBuilder
from pyspark.sql import functions as F

from Generator.IR_Generator import IRGenerator
from Generator.PySpark_Generator import PySparkGenerator

from Validator.IR_Validator import IRValidator
from Validator.PySpark_Validator import PySparkValidator
from parser.Datavalidator import DataValidator

import os
import re


class MigrationPipeline:
    """
    Orchestrates the complete IDMC -> Databricks migration flow.

    The pipeline does not contain parsing, graph, IR, generation, or
    validation logic itself. It only controls the order in which the
    existing components are executed.
    """

    def __init__(self, xml_path, spark, output_dir="/Volumes/idmc_poc/bronze/idmc_files/generated_pyspark"):
        self.xml_path = xml_path
        self.spark = spark
        self.output_dir = output_dir

        # Existing POC components
        self.xml_reader = XMLReader(xml_path)
        self.repository_parser = RepositoryParser()
        self.graph_builder = GraphBuilder()
        self.ir_generator = IRGenerator()
        self.ir_validator = IRValidator()
        self.pyspark_generator = PySparkGenerator()
        self.data_validator = DataValidator(spark)

    @staticmethod
    def print_section(title):
        print("\n" + "=" * 70)
        print(title)
        print("=" * 70)

    def run(self):
        """
        Run the complete migration process.
        """

        self.print_section("IDMC TO DATABRICKS MIGRATION POC")

        # ------------------------------------------------------------
        # 1. READ XML
        # ------------------------------------------------------------
        root = self.xml_reader.read()

        print("XML Status :", "LOADED")
        print("Root       :", root.tag)

        # ------------------------------------------------------------
        # 2. PARSE REPOSITORY
        # ------------------------------------------------------------
        self.print_section("REPOSITORY PARSER")

        repository = self.repository_parser.parse(root)

        print("Repository :", repository.get("name"))

        folders = repository.get("folders", [])

        print("Folders    :", len(folders))

        # ------------------------------------------------------------
        # 3. PARSER VALIDATION / METADATA SUMMARY
        # ------------------------------------------------------------
        self.print_section("PARSER VALIDATION")

        self.print_repository_summary(folders)

        # ------------------------------------------------------------
        # 4. PROCESS FOLDERS AND MAPPINGS
        # ------------------------------------------------------------
        self.print_section("MAPPING PROCESSING")

        for folder_index, folder in enumerate(folders):
            self.process_folder(
                folder_index=folder_index,
                folder=folder,
                repository=repository
            )

        self.print_section("MIGRATION PROCESSING COMPLETE")

    def print_repository_summary(self, folders):
        """
        Print the parsed repository metadata.
        This keeps main orchestration separate from display logic.
        """

        for folder_index, folder in enumerate(folders):

            print(f"\nFOLDER [{folder_index}]")
            print("-" * 70)

            print("Name :", folder.get("name"))

            # Connections
            connections = folder.get("connections", [])

            print("\nConnections :", len(connections))

            for connection in connections:
                print(
                    f"  {connection.get('name')} "
                    f"| type={connection.get('type')} "
                    f"| subtype={connection.get('subtype')}"
                )

            # Sources
            sources = folder.get("sources", [])

            print("\nSources :", len(sources))

            for source in sources:
                print(
                    f"  {source.get('name')} "
                    f"| type={source.get('type')} "
                    f"| connection={source.get('connection')} "
                    f"| columns={len(source.get('columns', []))}"
                )

            # Targets
            targets = folder.get("targets", [])

            print("\nTargets :", len(targets))

            for target in targets:
                print(
                    f"  {target.get('name')} "
                    f"| type={target.get('type')} "
                    f"| connection={target.get('connection')} "
                    f"| catalog={target.get('catalog')} "
                    f"| schema={target.get('schema')}"
                )

            # Mappings
            mappings = folder.get("mappings", [])

            print("\nMappings :", len(mappings))

            for mapping in mappings:
                print(
                    f"  {mapping.get('name')} "
                    f"| sources={len(mapping.get('source_instances', []))} "
                    f"| transformations={len(mapping.get('transformations', []))} "
                    f"| targets={len(mapping.get('target_instances', []))} "
                    f"| links={len(mapping.get('links', []))}"
                )

            # Sessions
            sessions = folder.get("sessions", [])

            print("\nSessions :", len(sessions))

            for session in sessions:
                print(
                    f"  {session.get('name')} "
                    f"| mapping={session.get('mapping')}"
                )

    def process_folder(self, folder_index, folder, repository):
        """
        Process all mappings inside one repository folder.
        """

        folder_name = folder.get("name")

        mappings = folder.get("mappings", [])

        print("\n" + "#" * 70)
        print(f"FOLDER [{folder_index}] : {folder_name}")
        print("#" * 70)

        for mapping_index, mapping in enumerate(mappings):
            self.process_mapping(
                folder_index=folder_index,
                mapping_index=mapping_index,
                folder=folder,
                mapping=mapping,
                repository=repository
            )

    def process_mapping(
        self,
        folder_index,
        mapping_index,
        folder,
        mapping,
        repository
    ):
        """
        Execute the migration flow for one IDMC mapping.
        """

        mapping_name = mapping.get("name")

        print("\n" + "-" * 70)
        print(
            f"MAPPING [{mapping_index}] : {mapping_name}"
        )
        print("-" * 70)

        self.print_mapping_details(mapping)

        # ------------------------------------------------------------
        # 4A. BUILD GRAPH
        # ------------------------------------------------------------
        graph = self.build_graph(mapping)

        # ------------------------------------------------------------
        # 4B. GENERATE IR
        # ------------------------------------------------------------
        ir = self.generate_ir(
            repository=repository,
            folder=folder,
            mapping=mapping,
            graph=graph
        )

        # ------------------------------------------------------------
        # 4C. VALIDATE IR
        # ------------------------------------------------------------
        ir_validation_report = self.validate_ir(ir)

        if ir_validation_report["status"] != "VALID":
            print("\nSTOPPING MAPPING")
            print("Reason: IR validation failed.")
            return

        # ------------------------------------------------------------
        # 4D. GENERATE PYSPARK
        # ------------------------------------------------------------
        generated_code = self.generate_pyspark(ir)

        # ------------------------------------------------------------
        # 4E. VALIDATE PYSPARK
        # ------------------------------------------------------------
        validation_result = self.validate_pyspark(
            ir=ir,
            generated_code=generated_code
        )

        if not validation_result["valid"]:
            print("\nExecution : SKIPPED")
            print("Reason    : PySpark validation failed")
            return

        # ------------------------------------------------------------
        # 4F. CREATE TARGET DELTA TABLES
        # ------------------------------------------------------------
        target_result = self.create_target_tables(
            mapping_name=mapping_name,
            ir=ir,
            folder=folder
        )

        if not target_result["success"]:
            print("\nExecution : SKIPPED")
            print("Reason    : Target Delta table creation failed")
            return

        # ------------------------------------------------------------
        # 4G. EXECUTE PYSPARK
        # ------------------------------------------------------------
        self.execute_pyspark(
            mapping_name=mapping_name,
            generated_code=generated_code
        )

        # ------------------------------------------------------------
        # 4H. VALIDATE TARGET TABLES
        # ------------------------------------------------------------
        target_validation_result = self.validate_target_tables(
            mapping_name=mapping_name,
            ir=ir,
            folder=folder
        )

        if not target_validation_result["success"]:
            print("\nMigration : FAILED")
            print("Reason    : Target Delta table validation failed")
            return

        # ------------------------------------------------------------
        # 4I. DATA RECONCILIATION / S-3
        # ------------------------------------------------------------
        reconciliation_result = self.validate_migration_data(
            mapping_name=mapping_name,
            ir=ir,
            folder=folder
        )

        if reconciliation_result["overall_status"] != "PASS":
            print("\nMigration : FAILED")
            print("Reason    : Data reconciliation failed")
            return

        print("\nMigration : VALID")

    def print_mapping_details(self, mapping):
        """
        Print source instances, transformations, targets and links.
        """

        print("\nMapping Details")

        print(
            "  Sources          :",
            len(mapping.get("source_instances", []))
        )

        print(
            "  Transformations  :",
            len(mapping.get("transformations", []))
        )

        print(
            "  Targets          :",
            len(mapping.get("target_instances", []))
        )

        print(
            "  Links            :",
            len(mapping.get("links", []))
        )

        print("\nSource Instances")

        for source_instance in mapping.get("source_instances", []):
            print(
                "  ",
                source_instance.get("name"),
                "→",
                source_instance.get("source")
            )

        print("\nTransformations")

        for transformation in mapping.get("transformations", []):
            print(
                "  ",
                transformation.get("name"),
                "| type=",
                transformation.get("type")
            )

        print("\nTarget Instances")

        for target_instance in mapping.get("target_instances", []):
            print(
                "  ",
                target_instance.get("name"),
                "→",
                target_instance.get("target")
            )

        print("\nLinks")

        for link in mapping.get("links", []):
            print(
                "  ",
                link.get("from"),
                "→",
                link.get("to")
            )

    def build_graph(self, mapping):
        """
        Build the generic execution graph from mapping metadata.
        """

        mapping_name = mapping.get("name")

        self.print_section(
            f"GRAPH : {mapping_name}"
        )

        try:
            graph = self.graph_builder.build(mapping)
        except Exception as error:
            print("GRAPH BUILD FAILED")
            print("Error:", str(error))
            raise

        print("\nGRAPH NODES")

        for node in graph.get("nodes", []):
            print("  ", node)

        print("\nGRAPH EDGES")

        for edge in graph.get("edges", []):
            print("  ", edge)

        print("\nEXECUTION ORDER")

        for index, node in enumerate(
            graph.get("execution_order", []),
            start=1
        ):
            print(f"  {index}. {node}")

        return graph

    def generate_ir(self, repository, folder, mapping, graph):
        """
        Convert parsed IDMC metadata and graph into canonical IR.
        """

        mapping_name = mapping.get("name")

        self.print_section(
            f"IR GENERATION : {mapping_name}"
        )

        try:
            ir = self.ir_generator.generate(
                repository=repository,
                folder=folder,
                mapping=mapping,
                graph=graph
            )
        except Exception as error:
            print("IR GENERATION FAILED")
            print("Error:", str(error))
            raise

        print("IR Mapping :", ir.get("name"))
        print("IR Sources :", len(ir.get("sources", [])))
        print(
            "IR Transformations :",
            len(ir.get("transformations", []))
        )
        print("IR Target :", ir.get("target"))

        return ir

    def validate_ir(self, ir):
        """
        Validate the canonical IR before generating PySpark.
        """

        mapping_name = ir.get("name")

        self.print_section(
            f"IR VALIDATION : {mapping_name}"
        )

        ir_validation_report = self.ir_validator.validate(ir)

        print(
            "Status   :",
            ir_validation_report["status"]
        )

        print(
            "Errors   :",
            len(ir_validation_report["errors"])
        )

        print(
            "Warnings :",
            len(ir_validation_report["warnings"])
        )

        if ir_validation_report["errors"]:
            print("\nIR ERRORS")

            for error in ir_validation_report["errors"]:
                print("  ERROR:", error)

        if ir_validation_report["warnings"]:
            print("\nIR WARNINGS")

            for warning in ir_validation_report["warnings"]:
                print("  WARNING:", warning)

        return ir_validation_report

    def generate_pyspark(self, ir):
        """
        Generate executable PySpark from the validated IR.
        """

        mapping_name = ir.get("name")

        self.print_section(
            f"PYSPARK GENERATION : {mapping_name}"
        )

        try:
            generated_code = self.pyspark_generator.generate(ir)
        except Exception as error:
            print("PYSPARK GENERATION FAILED")
            print("Error:", str(error))
            raise

        print("Status : GENERATED")

        self.save_generated_pyspark(
            mapping_name=mapping_name,
            generated_code=generated_code
        )

        return generated_code

    def _safe_filename(self, name):
        """
        Convert a mapping name into a safe Python filename.
        """

        value = str(name or "migration")
        value = re.sub(r"[^A-Za-z0-9._-]+", "_", value)
        value = value.strip("._")

        return value or "migration"

    def save_generated_pyspark(self, mapping_name, generated_code):
        """
        Persist generated PySpark as a real .py migration artifact.

        This is intentionally separate from execution so the generated
        implementation can be reviewed, versioned, deployed, or executed
        independently after migration.
        """

        safe_name = self._safe_filename(mapping_name)
        output_directory = self.output_dir
        output_file = os.path.join(
            output_directory,
            f"{safe_name}.py"
        )

        try:
            os.makedirs(output_directory, exist_ok=True)

            with open(
                output_file,
                "w",
                encoding="utf-8"
            ) as file_handle:
                file_handle.write(generated_code.rstrip() + "\n")

        except Exception as error:
            print("PYSPARK ARTIFACT SAVE FAILED")
            print("Error:", str(error))
            raise

        print("Generated PySpark :", output_file)

        return output_file

    def validate_pyspark(self, ir, generated_code):
        """
        Validate generated PySpark before execution.
        """

        mapping_name = ir.get("name")

        self.print_section(
            f"PYSPARK VALIDATION : {mapping_name}"
        )

        execution_plan = {
            "steps": [
                {
                    "name": name
                }
                for name in ir.get("execution_order", [])
            ]
        }

        pyspark_validator = PySparkValidator(
            ir,
            execution_plan,
            generated_code
        )

        validation_result = pyspark_validator.validate()

        print(
            "Status :",
            "VALID"
            if validation_result["valid"]
            else "INVALID"
        )

        print(
            "Errors :",
            len(validation_result["errors"])
        )

        print(
            "Warnings :",
            len(validation_result["warnings"])
        )

        if validation_result["errors"]:
            print("\nPYSPARK ERRORS")

            for error in validation_result["errors"]:
                print("  ERROR:", error)

        if validation_result["warnings"]:
            print("\nPYSPARK WARNINGS")

            for warning in validation_result["warnings"]:
                print("  WARNING:", warning)

        return validation_result


    # ============================================================
    # TARGET DELTA TABLE CREATION
    # ============================================================

    def create_target_tables(self, mapping_name, ir, folder):
        """
        Create Databricks Delta target tables from IDMC target metadata.

        The target name, catalog, schema and column definitions are derived
        from the parsed repository metadata / IR. Nothing is hardcoded for
        CUSTOMER_ORDER_SUMMARY.
        """

        self.print_section(
            f"TARGET DELTA TABLE CREATION : {mapping_name}"
        )

        try:
            ir_targets = ir.get("target", [])

            if isinstance(ir_targets, dict):
                ir_targets = [ir_targets]

            if not ir_targets:
                raise ValueError("No targets found in IR")

            repository_targets = folder.get("targets", [])

            if not isinstance(repository_targets, list):
                repository_targets = []

            created_tables = []

            ddl_artifacts = []

            for ir_target in ir_targets:
                if not isinstance(ir_target, dict):
                    raise ValueError(
                        f"Invalid target metadata for mapping '{mapping_name}'"
                    )

                target_name = (
                    ir_target.get("target")
                    or ir_target.get("name")
                )

                if not target_name:
                    raise ValueError("Target name is missing from IR")

                # Match the logical target from the IR to the repository
                # target definition, where the actual columns/catalog/schema
                # are normally available.
                repository_target = None

                for candidate in repository_targets:
                    if not isinstance(candidate, dict):
                        continue

                    candidate_name = candidate.get("name")

                    if candidate_name == target_name:
                        repository_target = candidate
                        break

                # Some IRs use a target-instance name while the repository
                # metadata uses the physical target name. Try both forms.
                if repository_target is None:
                    ir_target_name = ir_target.get("name")

                    for candidate in repository_targets:
                        if not isinstance(candidate, dict):
                            continue

                        if candidate.get("name") == ir_target_name:
                            repository_target = candidate
                            break

                metadata = repository_target or ir_target

                catalog = (
                    metadata.get("catalog")
                    or ir_target.get("catalog")
                    or "idmc_poc"
                )

                schema = (
                    metadata.get("schema")
                    or ir_target.get("schema")
                    or "silver"
                )

                table_name = (
                    metadata.get("name")
                    or target_name
                )

                columns = metadata.get("columns", [])

                # If repository target columns are unavailable, fall back to
                # target ports in the IR.
                if not columns:
                    columns = ir_target.get("ports", [])

                if not columns:
                    raise ValueError(
                        f"No target columns found for '{table_name}'"
                    )

                ddl_columns = []

                for column in columns:
                    if not isinstance(column, dict):
                        continue

                    column_name = (
                        column.get("name")
                        or column.get("column")
                    )

                    if not column_name:
                        continue

                    datatype = (
                        column.get("datatype")
                        or column.get("type")
                        or "STRING"
                    )

                    spark_type = self._map_idmc_datatype_to_spark(
                        datatype=datatype,
                        precision=column.get("precision"),
                        scale=column.get("scale"),
                        length=column.get("length")
                    )

                    ddl_columns.append(
                        f"`{column_name}` {spark_type}"
                    )

                if not ddl_columns:
                    raise ValueError(
                        f"No valid target columns found for '{table_name}'"
                    )

                full_table_name = (
                    f"{catalog}.{schema}.{table_name}"
                )

                ddl = (
                    f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema};\n\n"
                    f"CREATE TABLE IF NOT EXISTS {full_table_name} (\n"
                    + ",\n".join(
                        f"    {column}"
                        for column in ddl_columns
                    )
                    + "\n) USING DELTA;"
                )

                print("\nTarget :", full_table_name)
                print("Columns:", len(ddl_columns))
                print("DDL    :")
                print(ddl)

                # Execute statements separately because Spark SQL execution
                # of multi-statement strings is environment-dependent.
                self.spark.sql(
                    f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}"
                )

                self.spark.sql(
                    f"CREATE TABLE IF NOT EXISTS {full_table_name} ("
                    + ", ".join(ddl_columns)
                    + ") USING DELTA"
                )

                ddl_artifact = self.save_target_ddl(
                    mapping_name=mapping_name,
                    table_name=table_name,
                    ddl=ddl
                )

                created_tables.append(full_table_name)
                ddl_artifacts.append(ddl_artifact)

                print("Status : CREATED / EXISTS")
                print("DDL Artifact:", ddl_artifact)

            return {
                "success": True,
                "tables": created_tables,
                "ddl_artifacts": ddl_artifacts
            }

        except Exception as error:
            print("Target Delta table creation : FAILED")
            print("Error:", str(error))

            return {
                "success": False,
                "tables": [],
                "ddl_artifacts": [],
                "error": str(error)
            }

    @staticmethod
    def _map_idmc_datatype_to_spark(
        datatype,
        precision=None,
        scale=None,
        length=None
    ):
        """Map common IDMC datatypes to Databricks/Spark SQL types."""

        normalized = str(datatype).strip().upper()

        datatype_map = {
            "INTEGER": "INT",
            "INT": "INT",
            "BIGINT": "BIGINT",
            "SMALLINT": "SMALLINT",
            "TINYINT": "TINYINT",
            "FLOAT": "FLOAT",
            "DOUBLE": "DOUBLE",
            "REAL": "DOUBLE",
            "DECIMAL": "DECIMAL",
            "NUMERIC": "DECIMAL",
            "VARCHAR": "STRING",
            "CHAR": "STRING",
            "STRING": "STRING",
            "TEXT": "STRING",
            "CLOB": "STRING",
            "BOOLEAN": "BOOLEAN",
            "BOOL": "BOOLEAN",
            "DATE": "DATE",
            "DATETIME": "TIMESTAMP",
            "TIMESTAMP": "TIMESTAMP",
            "TIMESTAMP_NTZ": "TIMESTAMP_NTZ",
            "TIME": "STRING",
            "BINARY": "BINARY"
        }

        if normalized in {"DECIMAL", "NUMERIC"}:
            try:
                p = int(precision) if precision is not None else 18
            except (TypeError, ValueError):
                p = 18

            try:
                s = int(scale) if scale is not None else 2
            except (TypeError, ValueError):
                s = 2

            return f"DECIMAL({p},{s})"

        if normalized in datatype_map:
            return datatype_map[normalized]

        # Preserve already-valid Spark SQL type strings when possible.
        if normalized.startswith("DECIMAL("):
            return normalized

        # Conservative fallback for unsupported IDMC string-like types.
        return "STRING"

    def save_target_ddl(self, mapping_name, table_name, ddl):
        """Persist generated target DDL as a physical SQL artifact."""

        output_directory = os.path.join(
            self.output_dir,
            "ddl"
        )

        try:
            os.makedirs(output_directory, exist_ok=True)
        except Exception:
            pass

        filename = (
            f"{self._safe_filename(mapping_name)}__"
            f"{self._safe_filename(table_name)}.sql"
        )

        output_path = os.path.join(
            output_directory,
            filename
        )

        with open(
            output_path,
            "w",
            encoding="utf-8"
        ) as file:
            file.write(ddl)

        return output_path

    # ============================================================
    # TARGET TABLE VALIDATION
    # ============================================================

    def validate_target_tables(self, mapping_name, ir, folder):
        """Validate generated target Delta tables exist and match target metadata."""

        self.print_section(
            f"TARGET DELTA TABLE VALIDATION : {mapping_name}"
        )

        ir_targets = ir.get("target", [])
        if isinstance(ir_targets, dict):
            ir_targets = [ir_targets]

        repository_targets = folder.get("targets", [])
        if not isinstance(repository_targets, list):
            repository_targets = []

        validated_tables = []

        try:
            for ir_target in ir_targets:
                if not isinstance(ir_target, dict):
                    raise ValueError("Invalid target metadata in IR")

                target_name = (
                    ir_target.get("target")
                    or ir_target.get("name")
                )

                repository_target = next(
                    (
                        target
                        for target in repository_targets
                        if isinstance(target, dict)
                        and (
                            target.get("name") == target_name
                            or target.get("name") == ir_target.get("name")
                        )
                    ),
                    None
                )

                metadata = repository_target or ir_target

                catalog = (
                    metadata.get("catalog")
                    or ir_target.get("catalog")
                    or "idmc_poc"
                )

                schema = (
                    metadata.get("schema")
                    or ir_target.get("schema")
                    or "silver"
                )

                table_name = metadata.get("name") or target_name
                full_table_name = f"{catalog}.{schema}.{table_name}"

                table_df = self.spark.table(full_table_name)
                actual_columns = table_df.columns

                expected_columns = [
                    column.get("name")
                    for column in metadata.get("columns", [])
                    if isinstance(column, dict) and column.get("name")
                ]

                if not expected_columns:
                    expected_columns = [
                        port.get("name")
                        for port in ir_target.get("ports", [])
                        if isinstance(port, dict) and port.get("name")
                    ]

                missing_columns = [
                    column for column in expected_columns
                    if column not in actual_columns
                ]

                extra_columns = [
                    column for column in actual_columns
                    if column not in expected_columns
                ]

                if missing_columns or extra_columns:
                    raise ValueError(
                        f"Schema mismatch for {full_table_name}. "
                        f"Missing: {missing_columns}; Extra: {extra_columns}"
                    )

                row_count = table_df.count()

                print("Target :", full_table_name)
                print("Status : VALID")
                print("Columns:", len(actual_columns))
                print("Rows   :", row_count)

                validated_tables.append({
                    "full_table_name": full_table_name,
                    "table_df": table_df,
                    "metadata": metadata
                })

            return {
                "success": True,
                "tables": validated_tables
            }

        except Exception as error:
            print("Target : INVALID")
            print("Error  :", str(error))
            return {
                "success": False,
                "tables": [],
                "error": str(error)
            }

    def validate_migration_data(self, mapping_name, ir, folder):
        """
        Perform S-3 data reconciliation against a reference expected dataset.

        For the current POC mapping, the reference dataset is built from the
        known Bronze test data independently of the generated target code.
        This proves that the migrated result matches the expected business
        result rather than only proving that the target table exists.
        """

        self.print_section(
            f"DATA RECONCILIATION : {mapping_name}"
        )

        try:
            targets = ir.get("target", [])
            if isinstance(targets, dict):
                targets = [targets]

            if not targets:
                raise ValueError("No target found for data reconciliation")

            target = targets[0]
            target_name = target.get("target") or target.get("name")

            repository_target = next(
                (
                    item for item in folder.get("targets", [])
                    if isinstance(item, dict)
                    and item.get("name") in {target_name, target.get("name")}
                ),
                None
            )

            metadata = repository_target or target
            catalog = metadata.get("catalog") or target.get("catalog") or "idmc_poc"
            schema = metadata.get("schema") or target.get("schema") or "silver"
            table_name = metadata.get("name") or target_name
            full_table_name = f"{catalog}.{schema}.{table_name}"

            actual_df = self.spark.table(full_table_name)
            expected_df = self.build_reference_expected_dataframe(
                mapping_name=mapping_name,
                target_name=table_name
            )

            key_columns = self.get_reconciliation_keys(
                mapping_name=mapping_name,
                target_name=table_name,
                expected_df=expected_df
            )

            report = self.data_validator.validate(
                expected_df=expected_df,
                actual_df=actual_df,
                key_columns=key_columns
            )

            self.data_validator.print_report(report)

            return report

        except Exception as error:
            print("DATA RECONCILIATION : FAILED")
            print("Error:", str(error))
            return {
                "overall_status": "FAIL",
                "schema_validation": "FAIL",
                "row_count_validation": "FAIL",
                "data_validation": "FAIL",
                "errors": [str(error)]
            }

    def get_reconciliation_keys(self, mapping_name, target_name, expected_df):
        """Return business keys used for key-based data comparison."""

        if mapping_name == "m_customer_order_summary" or target_name == "CUSTOMER_ORDER_SUMMARY":
            return ["CUSTOMER_ID"]

        # Generic fallback: use the first column when no explicit key is
        # available. A future XML metadata extension can supply true keys.
        if expected_df.columns:
            return [expected_df.columns[0]]

        return []

    def build_reference_expected_dataframe(self, mapping_name, target_name):
        """
        Build the independent expected result for the current POC mapping.

        This reference logic is intentionally separate from generated PySpark.
        It represents the expected migration result for the controlled Bronze
        test dataset used by the old XML regression case.
        """

        if mapping_name != "m_customer_order_summary" and target_name != "CUSTOMER_ORDER_SUMMARY":
            raise ValueError(
                f"No reference expected dataset is configured for mapping '{mapping_name}' "
                f"and target '{target_name}'"
            )

        customer = self.spark.table("idmc_poc.bronze.CUSTOMER")
        orders = self.spark.table("idmc_poc.bronze.ORDERS")
        country_master = self.spark.table("idmc_poc.bronze.COUNTRY_MASTER")

        expected_df = (
            customer
            .select(
                "CUSTOMER_ID",
                F.upper(F.trim(F.col("CUSTOMER_NAME"))).alias("CUSTOMER_NAME"),
                "COUNTRY_CODE"
            )
            .join(
                orders
                .filter(
                    (F.col("ORDER_STATUS") == "COMPLETED")
                    & (F.col("ORDER_AMOUNT") > 0)
                )
                .select(
                    "CUSTOMER_ID",
                    "ORDER_ID",
                    "ORDER_DATE",
                    "ORDER_AMOUNT"
                ),
                "CUSTOMER_ID",
                "inner"
            )
            .join(
                country_master.select(
                    "COUNTRY_CODE",
                    "COUNTRY_NAME",
                    "REGION"
                ),
                "COUNTRY_CODE",
                "left"
            )
            .groupBy(
                "CUSTOMER_ID",
                "CUSTOMER_NAME",
                "COUNTRY_NAME",
                "REGION"
            )
            .agg(
                F.count("ORDER_ID").cast("int").alias("TOTAL_ORDERS"),
                F.sum("ORDER_AMOUNT").cast("decimal(18,2)").alias("TOTAL_AMOUNT"),
                F.max("ORDER_DATE").cast("date").alias("LAST_ORDER_DATE")
            )
            .select(
                "CUSTOMER_ID",
                "CUSTOMER_NAME",
                "COUNTRY_NAME",
                "REGION",
                "TOTAL_ORDERS",
                "TOTAL_AMOUNT",
                "LAST_ORDER_DATE"
            )
        )

        return expected_df

    def execute_pyspark(self, mapping_name, generated_code):
        """
        Execute the persisted generated PySpark artifact inside the existing
        Spark session.

        The generated .py file is the migration artifact produced by
        save_generated_pyspark(). The execution step deliberately reads and
        executes that saved artifact rather than executing the in-memory
        generated_code string. This ensures the artifact that is preserved
        for deployment/review is the same implementation that is actually
        executed during migration.
        """

        self.print_section(
            f"PYSPARK EXECUTION : {mapping_name}"
        )

        artifact_path = os.path.join(
            self.output_dir,
            f"{self._safe_filename(mapping_name)}.py"
        )

        try:
            if not os.path.exists(artifact_path):
                raise FileNotFoundError(
                    f"Generated PySpark artifact not found: {artifact_path}"
                )

            with open(artifact_path, "r", encoding="utf-8") as file_handle:
                persisted_code = file_handle.read()

            if not persisted_code.strip():
                raise ValueError(
                    f"Generated PySpark artifact is empty: {artifact_path}"
                )

            execution_namespace = {
                "spark": self.spark
            }

            # Compile using the artifact path so any runtime error points to
            # the actual generated .py file rather than an in-memory string.
            compiled_code = compile(
                persisted_code,
                artifact_path,
                "exec"
            )

            exec(
                compiled_code,
                execution_namespace
            )

            print("Execution Artifact:", artifact_path)
            print("Execution : SUCCESS")

        except Exception as error:
            print("Execution : FAILED")
            print("Artifact:", artifact_path)
            print("Error:", str(error))
            raise
