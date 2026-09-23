class PortParser:
    """
    Generic IDMC port parser.

    Supports:
        <Ports>
            <Port .../>
        </Ports>

    and also:

        <Transformation>
            <Port .../>
            <Port .../>
        </Transformation>

    All port attributes and unknown child elements are preserved.
    """

    def parse(self, parent_element):

        if parent_element is None:
            return []

        ports = []

        # =====================================================
        # FIND PORT ELEMENTS
        # =====================================================

        # Case 1:
        # <Ports>
        #     <Port />
        # </Ports>

        ports_element = parent_element.find("Ports")

        if ports_element is not None:

            for element in ports_element.iter("Port"):

                port_data = self._parse_port(element)

                if port_data:
                    ports.append(port_data)

            return ports

        # =====================================================
        # CASE 2:
        # DIRECT PORT CHILDREN
        #
        # <Transformation>
        #     <Port />
        #     <Port />
        # </Transformation>
        # =====================================================

        for child in parent_element:

            if child.tag == "Port":

                port_data = self._parse_port(child)

                if port_data:
                    ports.append(port_data)

        return ports

    # =========================================================
    # PARSE SINGLE PORT
    # =========================================================

    def _parse_port(self, port):

        if port is None:
            return None

        port_data = {
            "name": port.get("name"),
            "datatype": port.get("datatype"),
            "direction": port.get("direction"),
            "expression": None,
            "attributes": dict(port.attrib),
            "extensions": []
        }

        # =====================================================
        # EXPRESSION
        #
        # Supports:
        #
        # <Port expression="..."/>
        #
        # and:
        #
        # <Port>
        #     <Expression>...</Expression>
        # </Port>
        # =====================================================

        attribute_expression = port.get("expression")

        if attribute_expression:
            port_data["expression"] = attribute_expression.strip()

        expression = port.find("Expression")

        if expression is not None:

            expression_text = "".join(
                expression.itertext()
            ).strip()

            if expression_text:
                port_data["expression"] = expression_text

        # =====================================================
        # PRESERVE UNKNOWN PORT CHILD ELEMENTS
        # =====================================================

        known_elements = {
            "Expression"
        }

        for child in port:

            if child.tag not in known_elements:

                port_data["extensions"].append(
                    self._element_to_dict(child)
                )

        return port_data

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