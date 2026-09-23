class ConnectorParser:

    def parse(self, folder):

        connections_element = folder.find("Connections")

        if connections_element is None:
            return []

        connections = []

        for connection in connections_element.findall("Connection"):

            connection_data = {
                "name": connection.get("name"),
                "type": connection.get("type"),
                "subtype": connection.get("subtype"),

                # Preserve ALL XML attributes
                "attributes": dict(connection.attrib),

                # Known connection properties
                "properties": {},

                # Preserve unknown/nested XML elements
                "extensions": []
            }

            # -------------------------------------------------
            # CONNECTION PROPERTIES
            # -------------------------------------------------

            for prop in connection.findall("Property"):

                property_name = prop.get("name")
                property_value = prop.get("value")

                if property_name:
                    connection_data["properties"][
                        property_name
                    ] = property_value

            # -------------------------------------------------
            # PRESERVE UNKNOWN CONNECTION ELEMENTS
            # -------------------------------------------------

            known_elements = {
                "Property"
            }

            for child in connection:

                if child.tag not in known_elements:

                    connection_data["extensions"].append(
                        self._element_to_dict(child)
                    )

            connections.append(connection_data)

        return connections

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
