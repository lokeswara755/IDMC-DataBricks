class SessionParser:

    def parse(self, folder):

        sessions_element = folder.find("Sessions")

        if sessions_element is None:
            return []

        sessions = []

        for session in sessions_element.findall("Session"):

            session_data = {
                "name": session.get("name"),
                "mapping": session.get("mapping"),
                "attributes": dict(session.attrib),
                "properties": {},
                "source_connections": [],
                "target_connections": [],
                "extensions": []
            }

            # =====================================================
            # PROPERTIES
            # =====================================================

            properties_element = session.find("Properties")

            if properties_element is not None:

                for prop in properties_element.findall("Property"):

                    property_name = prop.get("name")
                    property_value = prop.get("value")

                    if property_name:

                        session_data["properties"][
                            property_name
                        ] = property_value

            # =====================================================
            # SOURCE CONNECTIONS
            # =====================================================

            for source_connection in session.findall(
                "SourceConnection"
            ):

                source_connection_data = {
                    "source": source_connection.get("source"),
                    "connection": source_connection.get("connection"),
                    "attributes": dict(
                        source_connection.attrib
                    ),
                    "extensions": []
                }

                for child in source_connection:

                    source_connection_data["extensions"].append(
                        self._element_to_dict(child)
                    )

                session_data["source_connections"].append(
                    source_connection_data
                )

            # =====================================================
            # TARGET CONNECTIONS
            # =====================================================

            for target_connection in session.findall(
                "TargetConnection"
            ):

                target_connection_data = {
                    "target": target_connection.get("target"),
                    "connection": target_connection.get("connection"),
                    "attributes": dict(
                        target_connection.attrib
                    ),
                    "extensions": []
                }

                for child in target_connection:

                    target_connection_data["extensions"].append(
                        self._element_to_dict(child)
                    )

                session_data["target_connections"].append(
                    target_connection_data
                )

            # =====================================================
            # PRESERVE UNKNOWN SESSION ELEMENTS
            # =====================================================

            known_elements = {
                "Properties",
                "SourceConnection",
                "TargetConnection"
            }

            for child in session:

                if child.tag not in known_elements:

                    session_data["extensions"].append(
                        self._element_to_dict(child)
                    )

            sessions.append(session_data)

        return sessions

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