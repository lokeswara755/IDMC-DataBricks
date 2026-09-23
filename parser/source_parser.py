class SourceParser:

    def parse(self, folder):

        sources_element = folder.find("Sources")

        if sources_element is None:
            return []

        sources = []

        for source in sources_element.findall("Source"):

            source_data = {
                "name": source.get("name"),
                "type": source.get("type"),
                "connection": source.get("connection"),

                # Preserve ALL Source attributes
                "attributes": dict(source.attrib),

                "columns": [],

                # Preserve unknown Source elements
                "extensions": []
            }

            # -------------------------------------------------
            # COLUMNS
            # -------------------------------------------------

            # Your XML uses direct <Column> elements:
            #
            # <Source>
            #     <Column .../>
            #     <Column .../>
            # </Source>
            #
            # Keep support for <Columns><Column/></Columns>
            # as well.

            columns_element = source.find("Columns")

            if columns_element is not None:

                column_elements = columns_element.findall("Column")

            else:

                column_elements = source.findall("Column")

            for column in column_elements:

                column_data = {
                    "name": column.get("name"),
                    "datatype": column.get("datatype"),
                    "nullable": column.get("nullable"),
                    "length": column.get("length"),
                    "precision": column.get("precision"),
                    "scale": column.get("scale"),

                    # Preserve ALL column attributes
                    "attributes": dict(column.attrib),

                    # Preserve unknown column children
                    "extensions": []
                }

                # -------------------------------------------------
                # PRESERVE UNKNOWN COLUMN CHILDREN
                # -------------------------------------------------

                for child in column:

                    column_data["extensions"].append(
                        self._element_to_dict(child)
                    )

                source_data["columns"].append(
                    column_data
                )

            # -------------------------------------------------
            # PRESERVE UNKNOWN SOURCE ELEMENTS
            # -------------------------------------------------

            known_elements = {
                "Columns",
                "Column"
            }

            for child in source:

                if child.tag not in known_elements:

                    source_data["extensions"].append(
                        self._element_to_dict(child)
                    )

            sources.append(source_data)

        return sources

    # =========================================================
    # GENERIC XML ELEMENT CONVERTER
    # =========================================================

    def _element_to_dict(self, element):

        data = {
            "tag": element.tag,
            "attributes": dict(element.attrib),
            "children": []
        }

        if element.text and element.text.strip():

            data["text"] = element.text.strip()

        for child in element:

            data["children"].append(
                self._element_to_dict(child)
            )

        return data