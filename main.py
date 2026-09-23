from pipeline.migration_pipeline import MigrationPipeline


def main():

    # ------------------------------------------------------------
    # GET XML PATH FROM DATABRICKS PARAMETER
    # ------------------------------------------------------------

    dbutils.widgets.text(
        "xml_path",
        "",
        "IDMC XML Path"
    )

    xml_path = dbutils.widgets.get("xml_path").strip()

    if not xml_path:
        raise ValueError(
            "XML path is required. "
            "Provide the IDMC XML path using the 'xml_path' parameter."
        )

    print("=" * 70)
    print("IDMC TO DATABRICKS MIGRATION POC")
    print("=" * 70)

    print("\nInput XML:")
    print(xml_path)

    # ------------------------------------------------------------
    # START MIGRATION PIPELINE
    # ------------------------------------------------------------

    pipeline = MigrationPipeline(
        xml_path=xml_path,
        spark=spark
    )

    pipeline.run()


if __name__ == "__main__":
    main()