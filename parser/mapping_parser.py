from parser.port_parser import PortParser


class MappingParser:

    def __init__(self):
        self.port_parser = PortParser()

    # ============================================================
    # PUBLIC PARSE METHOD
    # ============================================================

    def parse(self, folder):

        mappings_element = folder.find("Mappings")

        if mappings_element is None:
            return []

        mappings = []

        for mapping in mappings_element.findall("Mapping"):

            mapping_data = {
                "name": mapping.get("name"),
                "description": mapping.get("description"),
                "attributes": dict(mapping.attrib),

                "source_instances": [],
                "transformations": [],
                "target_instances": [],
                "links": [],

                "extensions": []
            }

            # ====================================================
            # SOURCE INSTANCES
            # ====================================================

            source_instances_element = mapping.find(
                "SourceInstances"
            )

            if source_instances_element is not None:

                for source_instance in source_instances_element.findall(
                    "SourceInstance"
                ):

                    source_instance_data = {
                        "name": source_instance.get("name"),
                        "source": source_instance.get("source"),
                        "attributes": dict(
                            source_instance.attrib
                        ),
                        "extensions": []
                    }

                    # Preserve every child element
                    for child in source_instance:

                        source_instance_data[
                            "extensions"
                        ].append(
                            self._element_to_dict(child)
                        )

                    mapping_data[
                        "source_instances"
                    ].append(
                        source_instance_data
                    )

            # ====================================================
            # TRANSFORMATIONS
            # ====================================================

            transformations_element = mapping.find(
                "Transformations"
            )

            if transformations_element is not None:

                for transformation in transformations_element.findall(
                    "Transformation"
                ):

                    transformation_data = (
                        self._parse_transformation(
                            transformation
                        )
                    )

                    mapping_data[
                        "transformations"
                    ].append(
                        transformation_data
                    )

            # ====================================================
            # TARGET INSTANCES
            # ====================================================

            target_instances_element = mapping.find(
                "TargetInstances"
            )

            if target_instances_element is not None:

                for target_instance in target_instances_element.findall(
                    "TargetInstance"
                ):

                    target_instance_data = {
                        "name": target_instance.get("name"),
                        "target": target_instance.get("target"),
                        "attributes": dict(
                            target_instance.attrib
                        ),
                        "ports": [],
                        "extensions": []
                    }

                    # ------------------------------------------------
                    # Parse target ports
                    # ------------------------------------------------

                    target_instance_data[
                        "ports"
                    ] = self.port_parser.parse(
                        target_instance
                    )

                    # ------------------------------------------------
                    # Preserve unknown target elements
                    # ------------------------------------------------

                    for child in target_instance:

                        if child.tag != "Ports":

                            target_instance_data[
                                "extensions"
                            ].append(
                                self._element_to_dict(child)
                            )

                    mapping_data[
                        "target_instances"
                    ].append(
                        target_instance_data
                    )

            # ====================================================
            # LINKS
            # ====================================================

            links_element = mapping.find(
                "Links"
            )

            if links_element is not None:

                for link in links_element.findall(
                    "Link"
                ):

                    link_data = {
                        "from": link.get("from"),
                        "to": link.get("to"),
                        "attributes": dict(
                            link.attrib
                        ),
                        "extensions": []
                    }

                    # Preserve nested link elements
                    for child in link:

                        link_data[
                            "extensions"
                        ].append(
                            self._element_to_dict(child)
                        )

                    mapping_data[
                        "links"
                    ].append(
                        link_data
                    )

            # ====================================================
            # UNKNOWN MAPPING ELEMENTS
            # ====================================================

            known_mapping_elements = {
                "SourceInstances",
                "Transformations",
                "TargetInstances",
                "Links"
            }

            for child in mapping:

                if child.tag not in known_mapping_elements:

                    mapping_data[
                        "extensions"
                    ].append(
                        self._element_to_dict(child)
                    )

            mappings.append(
                mapping_data
            )

        return mappings

    # ============================================================
    # TRANSFORMATION PARSER
    # ============================================================

    def _parse_transformation(
        self,
        transformation
    ):

        transformation_data = {
            "name": transformation.get("name"),
            "type": transformation.get("type"),

            # Preserve ALL transformation attributes
            "attributes": dict(
                transformation.attrib
            ),

            # Parse ports through dedicated parser
            "ports": self.port_parser.parse(
                transformation
            ),

            # Preserve every non-port XML element
            "extensions": []
        }

        # ========================================================
        # GENERIC ATTRIBUTE NORMALIZATION
        # ========================================================
        #
        # Preserve useful attribute names in normalized form.
        #
        # Example:
        #
        # lookupSource
        #     ->
        # lookup_source
        #
        # joinType
        #     ->
        # join_type
        #
        # But the original attributes remain inside "attributes".
        # ========================================================

        for key, value in transformation.attrib.items():

            normalized_key = self._normalize_key(
                key
            )

            if normalized_key not in {
                "name",
                "type"
            }:

                transformation_data[
                    normalized_key
                ] = value

        # ========================================================
        # GENERIC CHILD ELEMENT PARSING
        # ========================================================

        for child in transformation:

            # Ports are handled separately
            if child.tag == "Ports":
                continue

            child_data = self._element_to_dict(
                child
            )

            normalized_key = self._normalize_key(
                child.tag
            )

            # ----------------------------------------------------
            # If the element contains only text, expose the text
            # directly.
            # ----------------------------------------------------

            text = self._get_element_text(
                child
            )

            if text:

                transformation_data[
                    normalized_key
                ] = text

            else:

                transformation_data[
                    normalized_key
                ] = child_data

            # ----------------------------------------------------
            # Preserve complete child representation as well.
            # ----------------------------------------------------

            transformation_data[
                "extensions"
            ].append(
                child_data
            )

        return transformation_data

    # ============================================================
    # NORMALIZE XML FIELD NAME
    # ============================================================

    def _normalize_key(
        self,
        value
    ):

        if not value:
            return value

        result = []

        for index, character in enumerate(value):

            if (
                character.isupper()
                and index > 0
                and value[index - 1].islower()
            ):
                result.append("_")

            result.append(
                character.lower()
            )

        return "".join(
            result
        )

    # ============================================================
    # GET COMPLETE ELEMENT TEXT
    # ============================================================

    def _get_element_text(
        self,
        element
    ):

        text = "".join(
            element.itertext()
        ).strip()

        return text

    # ============================================================
    # GENERIC XML ELEMENT → DICTIONARY
    # ============================================================

    def _element_to_dict(
        self,
        element
    ):

        data = {
            "tag": element.tag,
            "attributes": dict(
                element.attrib
            ),
            "children": []
        }

        if element.text and element.text.strip():

            data["text"] = (
                element.text.strip()
            )

        for child in element:

            data[
                "children"
            ].append(
                self._element_to_dict(
                    child
                )
            )

        return data