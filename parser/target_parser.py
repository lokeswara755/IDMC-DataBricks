class TargetParser:

    def parse(self, folder):

        targets_element = folder.find("Targets")

        if targets_element is None:
            return []

        targets = []

        for target in targets_element.findall("Target"):

            target_data = {
                "name": target.get("name"),
                "type": target.get("type"),
                "connection": target.get("connection"),
                "catalog": target.get("catalog"),
                "schema": target.get("schema"),

                # Preserve ALL Target attributes
                "attributes": dict(target.attrib),

                "columns": [],

                # Preserve unknown Target elements
                "extensions": []
            }

            # =====================================================
            # COLUMNS
            # =====================================================

            # Your XML uses direct <Column> elements:
            #
            # <Target>
            #     <Column .../>
            #     <Column .../>
            # </Target>
            #
            # Keep support for <Columns><Column/></Columns>
            # as well.

            columns_element = target.find("Columns")

            if columns_element is not None:

                column_elements = columns_element.findall("Column")

            else:

                column_elements = target.findall("Column")

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
                # PRESERVE UNKNOWN COLUMN ELEMENTS
                # -------------------------------------------------

                for child in column:

                    column_data["extensions"].append(
                        self._element_to_dict(child)
                    )

                target_data["columns"].append(
                    column_data
                )

            # =====================================================
            # PRESERVE UNKNOWN TARGET ELEMENTS
            # =====================================================

            known_elements = {
                "Columns",
                "Column"
            }

            for child in target:

                if child.tag not in known_elements:

                    target_data["extensions"].append(
                        self._element_to_dict(child)
                    )

            targets.append(target_data)

        return targets

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